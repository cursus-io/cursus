package disk

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const lineageCheckpointInterval = 256
const lineageBackgroundRecordLimit = 1024
const lineageMagic = "CLIN0001"

// ErrLineageUnsupported routes compacted/gapped histories to compacted recovery.
var ErrLineageUnsupported = errors.New("lineage requires a contiguous retained log")

var errLineageRebuildRequired = errors.New("lineage requires an explicit proof request to rebuild")

// LineagePrefix proves the entire retained interval [StartOffset, EndOffset).
// Different retained starts are incomparable, including empty prefixes.
type LineagePrefix struct {
	StartOffset uint64 `json:"start_offset"`
	EndOffset   uint64 `json:"end_offset"`
	Digest      string `json:"digest"`
}

// LineageEpochSpan describes decoded records, not proof of common ancestry.
// LeaderEpoch zero is unknown history and must never establish a shared epoch.
type LineageEpochSpan struct {
	StartOffset uint64
	EndOffset   uint64
	LeaderEpoch int64
}

type lineageCursor struct {
	EndOffset uint64
	Segment   uint64
	Position  int64
	Digest    [sha256.Size]byte
}

type lineageIndex struct {
	start       uint64
	tail        lineageCursor
	checkpoints []lineageCursor
	epochs      []LineageEpochSpan
	info        os.FileInfo
}

// The fixed tail slot is overwritten after appending sparse checkpoints and
// completed epoch spans. Every record is independently checksummed. A crash can
// tear either write: the first proof request after restart rebuilds from the
// authoritative retained log, never trusting an old version or stale cursor.
// Storage is O(records/256 + epoch transitions), with a constant-size tail slot.
type lineageJournalRecord struct {
	Kind        uint64 // 1: tail, 2: checkpoint, 3: completed epoch span
	StartOffset uint64
	EndOffset   uint64
	LeaderEpoch int64
	EpochStart  uint64
	Segment     uint64
	Position    int64
	Digest      [sha256.Size]byte
}

// lineageRecordBytes binds each journal record checksum to the format version.
func lineageRecordBytes(record lineageJournalRecord) []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, record)
	sum := sha256.Sum256(append([]byte(lineageMagic), b.Bytes()...))
	return append(b.Bytes(), sum[:]...)
}

// lineageSeed anchors even an empty proof to its explicit retained start.
func lineageSeed(start uint64) [sha256.Size]byte {
	var offset [8]byte
	binary.BigEndian.PutUint64(offset[:], start)
	return sha256.Sum256(append([]byte("cursus-retained-lineage-v1"), offset[:]...))
}

// lineageHash extends the chain with a length-delimited canonical full record.
func lineageHash(previous [sha256.Size]byte, message types.DiskMessage) ([sha256.Size]byte, error) {
	// Decode first, then encode every persisted field in one canonical version.
	// CDM2/CDM3/CDM4 and re-encoded CDM5 records therefore compare identically. If the
	// canonical encoding changes, bump the lineage format/domain version too.
	canonical, err := util.SerializeDiskMessage(message)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	h := sha256.New()
	_, _ = h.Write(previous[:])
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(canonical)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(canonical)
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

// LineageCandidates returns at most 64 cumulative proofs, newest first,
// including the exact requested tail and the empty retained-start anchor.
// Checkpoint seeks limit each historical endpoint scan to 255 records.
func (d *DiskHandler) LineageCandidates(tail uint64) ([]LineagePrefix, error) {
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	if err := d.prepareLineageLocked(context.Background()); err != nil {
		return nil, err
	}
	if tail < d.lineage.start || tail > d.lineage.tail.EndOffset {
		return nil, fmt.Errorf("lineage tail %d outside retained range [%d,%d]", tail, d.lineage.start, d.lineage.tail.EndOffset)
	}
	ends := lineageCandidateEnds(d.lineage.start, tail)
	proofs := make([]LineagePrefix, 0, len(ends))
	for _, end := range ends {
		proof, err := d.lineagePrefixLocked(context.Background(), end)
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, proof)
	}
	return proofs, nil
}

// lineageCandidateEnds selects the exact tail, tail-relative rollback distances
// 1, 3, 7, ... and the retained-start anchor for start <= tail. Reserve the last
// slot for the anchor, keeping all uint64 spans within the 64-proof wire limit.
func lineageCandidateEnds(start, tail uint64) []uint64 {
	ends := []uint64{tail}
	span := tail - start
	for distance := uint64(1); distance < span && len(ends) < 63; distance = 2*distance + 1 {
		ends = append(ends, tail-distance)
	}
	if tail != start {
		ends = append(ends, start)
	}
	return ends
}

// VerifyLineagePrefix compares an explicit retained prefix only below the
// caller's authoritative, exclusive committed HWM. Epoch matches alone are
// never used as evidence; a matching final record cannot hide earlier divergence.
func (d *DiskHandler) VerifyLineagePrefix(prefix LineagePrefix, committedHWM uint64) (bool, error) {
	if prefix.EndOffset < prefix.StartOffset || prefix.EndOffset > committedHWM {
		return false, nil
	}
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	if err := d.prepareLineageLocked(context.Background()); err != nil {
		return false, err
	}
	if prefix.StartOffset != d.lineage.start || prefix.EndOffset > d.lineage.tail.EndOffset {
		return false, nil
	}
	want, err := d.lineagePrefixLocked(context.Background(), prefix.EndOffset)
	return err == nil && want == prefix, err
}

// MatchLineageCandidates finds the longest matching prefix below committedHWM
// with a single durability boundary and stable retained-log snapshot. A nil
// match means no comparable proof, not permission to discard committed data.
func (d *DiskHandler) MatchLineageCandidates(candidates []LineagePrefix, committedHWM uint64) (*LineagePrefix, error) {
	return d.MatchLineageCandidatesContext(context.Background(), candidates, committedHWM)
}

// MatchLineageCandidatesContext bounds lock waits and initial derived-index rebuilds.
func (d *DiskHandler) MatchLineageCandidatesContext(ctx context.Context, candidates []LineagePrefix, committedHWM uint64) (*LineagePrefix, error) {
	if ctx == nil {
		return nil, errors.New("lineage requires context")
	}
	if len(candidates) > 64 {
		return nil, errors.New("too many lineage candidates")
	}
	unlock, err := lockProofContext(ctx, &d.maintenanceMu, &d.mu, &d.ioMu)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := d.prepareLineageLocked(ctx); err != nil {
		return nil, err
	}
	ordered := append([]LineagePrefix(nil), candidates...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].EndOffset > ordered[j].EndOffset })
	for _, candidate := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if candidate.StartOffset != d.lineage.start || candidate.EndOffset < candidate.StartOffset || candidate.EndOffset > committedHWM || candidate.EndOffset > d.lineage.tail.EndOffset {
			continue
		}
		proof, err := d.lineagePrefixLocked(ctx, candidate.EndOffset)
		if err != nil {
			return nil, err
		}
		if proof == candidate {
			return &proof, nil
		}
	}
	return nil, nil
}

// LineageEpochs returns a copy of the durable retained epoch spans.
func (d *DiskHandler) LineageEpochs() ([]LineageEpochSpan, error) {
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	if err := d.prepareLineageLocked(context.Background()); err != nil {
		return nil, err
	}
	return append([]LineageEpochSpan(nil), d.lineage.epochs...), nil
}

// prepareLineageLocked requires maintenanceMu, mu and ioMu. Explicit proof
// requests retry a previously failed publication and resume automatic updates
// only after the complete durable tail has been successfully indexed.
func (d *DiskHandler) prepareLineageLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	unlockAvailability, err := lockProofContext(ctx, proofReadMutex{&d.writeFailureMu})
	if err != nil {
		return err
	}
	availabilityErr := d.proofAvailabilityLocked()
	unlockAvailability()
	if availabilityErr != nil {
		return availabilityErr
	}
	if d.file == nil {
		return errors.New("lineage log is closed")
	}
	// FlushedOffset means written, not necessarily synced. Never publish a proof
	// until the data file has crossed the durability boundary under ioMu.
	if err := d.syncFile(d.file); err != nil {
		return d.markWriteUnavailable(fmt.Errorf("sync lineage log: %w", err))
	}
	uninitialized := d.lineage == nil
	if err := d.refreshLineageLocked(ctx, 0); err != nil {
		d.suspendLineagePublicationLocked(err)
		return err
	}
	if d.lineageAutoSuppressed {
		d.lineageAutoSuppressed = false
		util.Info("lineage publication recovered partition=%s tail=%d", d.BaseName, d.lineage.tail.EndOffset)
	} else if uninitialized {
		util.Info("lineage publication ready partition=%s tail=%d", d.BaseName, d.lineage.tail.EndOffset)
	}
	return nil
}

// updateLineageAfterSyncLocked runs only from periodic sync under mu and ioMu.
// It advances an already initialized index by at most 1,024 durable records.
// It never initializes or rebuilds history, and no acknowledged write waits for
// its own sidecar publication. Explicit proof requests catch up before returning.
func (d *DiskHandler) updateLineageAfterSyncLocked() {
	if d.lineage == nil || d.lineageAutoSuppressed || d.writeAvailabilityError() != nil {
		return
	}
	if err := d.refreshLineageLocked(context.Background(), lineageBackgroundRecordLimit); err != nil {
		d.suspendLineagePublicationLocked(err)
	}
}

// suspendLineagePublicationLocked requires mu and ioMu. Do not repeatedly scan
// retained history on every idle sync while a sidecar remains unavailable.
// Proof calls still retry explicitly and never serve an unpublished cache.
func (d *DiskHandler) suspendLineagePublicationLocked(err error) {
	d.lineage = nil
	if d.lineageAutoSuppressed {
		return
	}
	d.lineageAutoSuppressed = true
	if errors.Is(err, ErrLineageUnsupported) {
		util.Debug("lineage publication suspended partition=%s reason=%v", d.BaseName, err)
	} else {
		util.Warn("lineage publication suspended partition=%s reason=%v", d.BaseName, err)
	}
}

// refreshLineageLocked requires mu and ioMu after data sync. A zero record limit
// permits explicit requests to initialize/rebuild and catch up completely. A
// positive limit advances only a still-valid cache, bounding background work.
func (d *DiskHandler) refreshLineageLocked(ctx context.Context, recordLimit uint64) (err error) {
	defer func() {
		if err != nil {
			d.lineage = nil
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(d.segments) == 0 {
		return ErrLineageUnsupported
	}
	// Compacted ranges can contain logical holes at either edge, including an
	// entirely empty physical segment. Conservatively defer all marked ranges.
	unlockCompaction, err := lockProofContext(ctx, proofReadMutex{&d.compactionMu})
	if err != nil {
		return err
	}
	compacted := len(d.compactedSegments) != 0
	unlockCompaction()
	if compacted {
		return ErrLineageUnsupported
	}
	tail := atomic.LoadUint64(&d.FlushedOffset)
	index := d.lineage
	path := d.BaseName + ".lineage"
	info, statErr := os.Stat(path)
	if index != nil && (statErr != nil || index.info == nil || info.Size() != index.info.Size() || !info.ModTime().Equal(index.info.ModTime()) || !os.SameFile(info, index.info)) {
		index = nil
	}
	if index != nil && (index.start != d.segments[0] || tail < index.tail.EndOffset) {
		index = nil
	}
	// Timestamps can be preserved or coarsely rounded during an in-place rewrite.
	// Validate the fixed header before trusting even an unchanged cached tail.
	if index != nil && !lineageHeaderMatches(path, index) {
		index = nil
	}
	fresh := index == nil
	if fresh {
		if recordLimit != 0 {
			return errLineageRebuildRequired
		}
		start := d.segments[0]
		cursor := lineageCursor{EndOffset: start, Segment: start, Digest: lineageSeed(start)}
		index = &lineageIndex{start: start, tail: cursor, checkpoints: []lineageCursor{cursor}}
	}
	if !fresh && index.tail.EndOffset == tail {
		return nil
	}
	if recordLimit != 0 && tail-index.tail.EndOffset > recordLimit {
		tail = index.tail.EndOffset + recordLimit
	}
	var events []lineageJournalRecord
	cursor, err := d.scanLineageLocked(ctx, index.tail, tail, func(message types.DiskMessage, next lineageCursor) {
		if len(index.epochs) == 0 || index.epochs[len(index.epochs)-1].LeaderEpoch != message.LeaderEpoch {
			if len(index.epochs) != 0 {
				span := index.epochs[len(index.epochs)-1]
				events = append(events, lineageJournalRecord{Kind: 3, StartOffset: span.StartOffset, EndOffset: span.EndOffset, LeaderEpoch: span.LeaderEpoch})
			}
			index.epochs = append(index.epochs, LineageEpochSpan{StartOffset: message.Offset, LeaderEpoch: message.LeaderEpoch})
		}
		index.epochs[len(index.epochs)-1].EndOffset = next.EndOffset
		if (next.EndOffset-index.start)%lineageCheckpointInterval == 0 {
			index.checkpoints = append(index.checkpoints, next)
			events = append(events, lineageJournalRecord{Kind: 2, StartOffset: index.start, EndOffset: next.EndOffset, Segment: next.Segment, Position: next.Position, Digest: next.Digest})
		}
	})
	if err != nil {
		return err
	}
	index.tail = cursor
	if err := publishLineage(ctx, path, index, fresh, events); err != nil {
		return fmt.Errorf("publish derived lineage: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.lineage = index
	return nil
}

// lineageTailRecord builds the exact tail state shared by publication and
// validation. The caller holds mu/ioMu while accessing the derived index.
func lineageTailRecord(index *lineageIndex) lineageJournalRecord {
	tail := lineageJournalRecord{Kind: 1, StartOffset: index.start, EndOffset: index.tail.EndOffset, Segment: index.tail.Segment, Position: index.tail.Position, Digest: index.tail.Digest}
	if len(index.epochs) != 0 {
		span := index.epochs[len(index.epochs)-1]
		tail.LeaderEpoch, tail.EpochStart = span.LeaderEpoch, span.StartOffset
	}
	return tail
}

// lineageHeaderMatches reads only the 128-byte magic and checksummed tail slot.
// Comparing the complete expected encoding detects stale valid tails as well as
// corrupt magic/checksums even when size, identity and modification time match.
func lineageHeaderMatches(path string, index *lineageIndex) bool {
	// #nosec G304 -- path is the partition-owned derived index.
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	expected := append([]byte(lineageMagic), lineageRecordBytes(lineageTailRecord(index))...)
	actual := make([]byte, len(expected))
	if _, err := io.ReadFull(f, actual); err != nil {
		return false
	}
	return bytes.Equal(actual, expected)
}

// publishLineage appends sparse events and replaces the constant-size tail slot,
// syncing before updating the cache's file identity. The caller owns mu/ioMu.
func publishLineage(ctx context.Context, path string, index *lineageIndex, fresh bool, events []lineageJournalRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	flags := os.O_CREATE | os.O_RDWR
	if fresh {
		flags |= os.O_TRUNC
	}
	// #nosec G304 -- path is the partition-owned derived index.
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	encodedTail := lineageRecordBytes(lineageTailRecord(index))
	if fresh {
		if _, err := f.Write(append([]byte(lineageMagic), make([]byte, len(encodedTail))...)); err != nil {
			return err
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := f.Write(lineageRecordBytes(event)); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := f.WriteAt(encodedTail, int64(len(lineageMagic))); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if fresh {
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return err
		}
	}
	// Windows may finalize last-write metadata only when the write handle closes.
	// Capture it afterwards so normal appends do not look like sidecar damage.
	if err := f.Close(); err != nil {
		return err
	}
	index.info, err = os.Stat(path)
	return err
}

// lineagePrefixLocked requires mu/ioMu and an endpoint inside the prepared index.
// A checkpoint bounds the remaining canonical record scan to fewer than 256 records.
func (d *DiskHandler) lineagePrefixLocked(ctx context.Context, end uint64) (LineagePrefix, error) {
	if err := ctx.Err(); err != nil {
		return LineagePrefix{}, err
	}
	index := d.lineage
	cursor := index.tail
	if end != cursor.EndOffset {
		i := sort.Search(len(index.checkpoints), func(i int) bool { return index.checkpoints[i].EndOffset > end })
		cursor = index.checkpoints[i-1]
		var err error
		cursor, err = d.scanLineageLocked(ctx, cursor, end, nil)
		if err != nil {
			return LineagePrefix{}, err
		}
	}
	return LineagePrefix{StartOffset: index.start, EndOffset: end, Digest: hex.EncodeToString(cursor.Digest[:])}, nil
}

// scanLineageLocked starts at an indexed physical cursor, never rescanning the
// retained prefix. mu/ioMu keep segment membership and active bytes stable.
func (d *DiskHandler) scanLineageLocked(ctx context.Context, cursor lineageCursor, end uint64, visit func(types.DiskMessage, lineageCursor)) (lineageCursor, error) {
	if cursor.EndOffset == end {
		return cursor, nil
	}
	first := sort.Search(len(d.segments), func(i int) bool { return d.segments[i] >= cursor.Segment })
	for i := first; i < len(d.segments); i++ {
		if err := ctx.Err(); err != nil {
			return cursor, err
		}
		segment := d.segments[i]
		position := int64(0)
		if segment == cursor.Segment {
			position = cursor.Position
		}
		// #nosec G304 -- segment paths are generated beneath the partition root.
		f, err := os.Open(d.GetSegmentPath(segment))
		if err != nil {
			return cursor, err
		}
		if _, err := f.Seek(position, io.SeekStart); err != nil {
			_ = f.Close()
			return cursor, err
		}
		reader := bufio.NewReader(f)
		for cursor.EndOffset < end {
			if err := ctx.Err(); err != nil {
				_ = f.Close()
				return cursor, err
			}
			var length [4]byte
			if _, err := io.ReadFull(reader, length[:]); err != nil {
				if err == io.EOF {
					break
				}
				_ = f.Close()
				return cursor, err
			}
			size := binary.BigEndian.Uint32(length[:])
			if size == 0 || size > MaxMessageSize {
				_ = f.Close()
				return cursor, fmt.Errorf("invalid lineage record size %d", size)
			}
			payload := make([]byte, size)
			if _, err := io.ReadFull(reader, payload); err != nil {
				_ = f.Close()
				return cursor, err
			}
			message, err := util.DeserializeDiskMessage(payload)
			if err != nil {
				_ = f.Close()
				return cursor, err
			}
			if message.Offset != cursor.EndOffset || message.Offset == math.MaxUint64 {
				_ = f.Close()
				return cursor, ErrLineageUnsupported
			}
			digest, err := lineageHash(cursor.Digest, message)
			if err != nil {
				_ = f.Close()
				return cursor, err
			}
			position += int64(size) + 4
			cursor = lineageCursor{EndOffset: message.Offset + 1, Segment: segment, Position: position, Digest: digest}
			if visit != nil {
				visit(message, cursor)
			}
		}
		_ = f.Close()
		if cursor.EndOffset == end {
			return cursor, nil
		}
	}
	return cursor, ErrLineageUnsupported
}
