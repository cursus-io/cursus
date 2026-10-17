package disk

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/util"
)

// ErrTruncateNotApplied identifies a clean pre-mutation rejection. An already
// unavailable handler and any failure after mutation begins never carry it.
var ErrTruncateNotApplied = errors.New("disk truncate did not begin mutation")

// TruncateTo removes records whose offsets are greater than or equal to nextOffset.
// It is used during leader promotion to discard a locally durable but uncommitted tail.
func (d *DiskHandler) TruncateTo(nextOffset uint64) error {
	return d.truncateTo(context.Background(), nextOffset, nil)
}

// TruncateToIfGeneration rejects stale/foreign tokens even for an equal-tail
// no-op. Flush precedes maintenance acquisition, as in TruncateTo.
func (d *DiskHandler) TruncateToIfGeneration(nextOffset uint64, token ContentGeneration) error {
	return d.TruncateToIfGenerationContext(context.Background(), nextOffset, token)
}

func (d *DiskHandler) TruncateToIfGenerationContext(ctx context.Context, nextOffset uint64, token ContentGeneration) error {
	return d.truncateTo(ctx, nextOffset, &token)
}

func (d *DiskHandler) truncateTo(ctx context.Context, nextOffset uint64, token *ContentGeneration) (err error) {
	started, unavailable := false, false
	defer func() {
		if err != nil && !started && !unavailable {
			err = errors.Join(ErrTruncateNotApplied, err)
		}
	}()
	if ctx == nil {
		return fmt.Errorf("truncate requires context")
	}
	if err := d.flushProofContext(ctx); err != nil {
		return err
	}
	unlock, err := lockProofContext(ctx, &d.maintenanceMu, &d.appendMu)
	if err != nil {
		return err
	}
	defer unlock()
	// The initial flush precedes maintenance acquisition, but an automatic
	// append can reserve/enqueue another offset while we wait. Drain again with
	// allocation excluded, before taking mu/ioMu needed by the flush worker.
	// Keep appendMu through the reset so no old reservation can arrive afterward.
	if err := d.flushProofContext(ctx); err != nil {
		return err
	}
	if err := d.writeAvailabilityError(); err != nil {
		unavailable = true
		return err
	}
	if readers := atomic.LoadInt32(&d.activeReaders); readers != 0 {
		return fmt.Errorf("cannot truncate with %d active reader(s)", readers)
	}
	unlockIO, err := lockProofContext(ctx, &d.mu, &d.ioMu)
	if err != nil {
		return err
	}
	defer unlockIO()
	d.writeFailureMu.RLock()
	availabilityErr := d.proofAvailabilityLocked()
	d.writeFailureMu.RUnlock()
	if availabilityErr != nil {
		unavailable = true
		return availabilityErr
	}
	if token != nil && *token != d.contentGenerationLocked() {
		return ErrStaleContentGeneration
	}

	currentTail := d.writtenTail
	if nextOffset > currentTail {
		return fmt.Errorf("truncate offset %d is ahead of durable tail %d", nextOffset, currentTail)
	}
	if nextOffset == currentTail && nextOffset == atomic.LoadUint64(&d.AbsoluteOffset) {
		return nil
	}
	if len(d.segments) == 0 {
		return fmt.Errorf("no segments available for truncation")
	}
	sort.Slice(d.segments, func(i, j int) bool { return d.segments[i] < d.segments[j] })
	if nextOffset < d.segments[0] {
		return fmt.Errorf("truncate offset %d precedes retained offset %d", nextOffset, d.segments[0])
	}

	targetBase := d.segments[0]
	for _, base := range d.segments {
		if base > nextOffset {
			break
		}
		targetBase = base
	}
	targetPath := d.GetSegmentPath(targetBase)
	targetInfo, err := os.Stat(targetPath)
	if err != nil {
		return fmt.Errorf("stat truncate target segment %d: %w", targetBase, err)
	}
	if d.segmentAllowsOffsetGaps(targetBase, targetInfo.Size()) {
		return fmt.Errorf("cannot truncate compacted segment %d", targetBase)
	}
	position, err := findTruncatePosition(targetPath, nextOffset)
	if err != nil {
		return err
	}
	if err := d.segmentReaders.invalidateAll(); err != nil {
		return fmt.Errorf("invalidate segment reader cache before truncation: %w", err)
	}
	// All caller input and the target boundary have been validated. From here,
	// even an unsuccessful close/removal may leave an unusable writer or prefix.
	if err := ctx.Err(); err != nil {
		return err
	}
	started = true
	d.advanceContentGenerationLocked()
	d.lineage = nil
	defer func() {
		if err != nil {
			err = d.markWriteUnavailable(err)
		}
	}()

	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			return fmt.Errorf("flush active writer before truncation: %w", err)
		}
	}
	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			return fmt.Errorf("sync active segment before truncation: %w", err)
		}
		if err := d.file.Close(); err != nil {
			return fmt.Errorf("close active segment before truncation: %w", err)
		}
		d.file = nil
		d.writer = nil
	}

	d.indexMu.Lock()
	if err := d.closeIndexFiles(); err != nil {
		d.indexMu.Unlock()
		return fmt.Errorf("close active index before truncation: %w", err)
	}
	d.indexMu.Unlock()

	kept := make([]uint64, 0, len(d.segments))
	for _, base := range d.segments {
		if base <= targetBase {
			kept = append(kept, base)
			continue
		}
		if err := os.Remove(d.GetSegmentPath(base)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove truncated segment %d: %w", base, err)
		}
		if err := os.Remove(d.GetIndexPath(base)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove truncated index %d: %w", base, err)
		}
		if err := cleanupCompactionMarkersForLog(d.GetSegmentPath(base), -1); err != nil {
			return fmt.Errorf("remove truncated compaction marker %d: %w", base, err)
		}
		d.forgetCompactedSegment(base)
	}

	// The target index is rebuilt lazily from offset zero within the retained segment.
	if err := os.Remove(d.GetIndexPath(targetBase)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove target index %d: %w", targetBase, err)
	}
	// #nosec G304 -- targetPath is derived from the broker-owned log directory.
	f, err := os.OpenFile(targetPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open target segment %d: %w", targetBase, err)
	}
	if err := f.Truncate(position); err != nil {
		_ = f.Close()
		return fmt.Errorf("truncate target segment %d: %w", targetBase, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync target segment %d: %w", targetBase, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close target segment %d: %w", targetBase, err)
	}
	truncateProcessCrashFault(d.BaseName)

	d.segments = kept
	d.CurrentSegment = targetBase
	d.CurrentOffset = uint64(position) // #nosec G115 -- position is non-negative and bounded by the segment size.
	d.lastIndexPosition = 0
	d.indexBytesWritten = 0
	d.segmentCreatedAt = time.Now()
	atomic.StoreUint64(&d.AbsoluteOffset, nextOffset)
	atomic.StoreUint64(&d.FlushedOffset, nextOffset)
	d.writtenTail = nextOffset

	if err := d.openSegment(); err != nil {
		return fmt.Errorf("reopen truncated segment: %w", err)
	}
	if err := d.openIndexFiles(); err != nil {
		return fmt.Errorf("reopen truncated index: %w", err)
	}
	return nil
}

func findTruncatePosition(path string, nextOffset uint64) (int64, error) {
	// #nosec G304 -- path is derived from the broker-owned log directory.
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open segment for truncation scan: %w", err)
	}
	defer func() { _ = f.Close() }()

	reader := bufio.NewReader(f)
	var position int64
	var length [4]byte
	for {
		start := position
		if _, err := io.ReadFull(reader, length[:]); err != nil {
			if err == io.EOF {
				return position, nil
			}
			return 0, fmt.Errorf("read record length at %d: %w", position, err)
		}
		msgLen := binary.BigEndian.Uint32(length[:])
		if msgLen == 0 || msgLen > MaxMessageSize {
			return 0, fmt.Errorf("invalid record length %d at %d", msgLen, position)
		}
		payload := make([]byte, msgLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return 0, fmt.Errorf("read record at %d: %w", position, err)
		}
		msg, err := util.DeserializeDiskMessage(payload)
		if err != nil {
			return 0, fmt.Errorf("decode record at %d: %w", position, err)
		}
		if msg.Offset >= nextOffset {
			return start, nil
		}
		position += int64(4 + msgLen)
	}
}
