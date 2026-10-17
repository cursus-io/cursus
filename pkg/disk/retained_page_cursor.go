package disk

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/metrics"
)

const retainedPageCheckpointLimit = 128
const retainedPageChunkBytes = 1 << 20
const retainedPageChunkRecords = 256
const retainedPageChunkTransitions = 64

// RetainedPageWork reports cumulative local page-read work, including failed
// and canceled requests. BytesRead counts file-read results, including headers
// and read-ahead. Cache entries contain metadata only and are process-local.
type RetainedPageWork struct {
	BytesRead, DecodedFrames, SegmentTransitions, CacheHits, CacheMisses uint64
	PeakCheckpoints                                                      uint64
}

// RetainedPageWorkSnapshot is a read-only diagnostic; it does not authorize a
// recovery operation or expose cached cursor identities.
func (d *DiskHandler) RetainedPageWorkSnapshot() RetainedPageWork {
	d.retainedPageMu.Lock()
	defer d.retainedPageMu.Unlock()
	return d.retainedPageWork
}

func (d *DiskHandler) lockRetainedPageStorage(ctx context.Context) (func(), error) {
	return lockProofContext(ctx, &d.maintenanceMu, &d.mu, &d.ioMu, proofReadMutex{&d.compactionMu})
}

// The page gate is held, but storage locks cover only the snapshot or a bounded
// capture. An individual filesystem call cannot be interrupted by cancellation.
func (d *DiskHandler) retainedPageSnapshot(ctx context.Context, start, end uint64) (rawFrameCursor, uint64, error) {
	unlock, err := d.lockRetainedPageStorage(ctx)
	if err != nil {
		return rawFrameCursor{}, 0, err
	}
	defer unlock()
	if err := d.lineageAvailable(ctx); err != nil {
		return rawFrameCursor{}, 0, err
	}
	tail := atomic.LoadUint64(&d.FlushedOffset)
	if start > end || end > tail || len(d.segments) == 0 || start < d.segments[0] {
		return rawFrameCursor{}, 0, fmt.Errorf("retained page outside durable bounds")
	}
	if d.file == nil {
		return rawFrameCursor{}, 0, fmt.Errorf("retained page log is closed")
	}
	metrics.RecordRecoveryWork(metrics.RecoveryRetainedPage, metrics.RecoverySyncCalls, 1)
	if err := d.syncFile(d.file); err != nil {
		return rawFrameCursor{}, 0, d.markWriteUnavailable(fmt.Errorf("sync retained page: %w", err))
	}
	i := sort.Search(len(d.segments), func(i int) bool { return d.segments[i] > start }) - 1
	base := d.segments[i]
	cursor := rawFrameCursor{Generation: d.contentGenerationLocked(), Anchor: d.segments[0], End: base, Segment: base}
	if len(d.retainedCursors) != 0 && (d.retainedCursors[0].Generation != cursor.Generation || d.retainedCursors[0].Anchor != cursor.Anchor) {
		d.retainedCursors = nil
	}
	best := -1
	for i, candidate := range d.retainedCursors {
		if candidate.Generation == cursor.Generation && candidate.Anchor == cursor.Anchor && candidate.Segment == base && candidate.End <= start && candidate.End >= cursor.End {
			cursor, best = candidate, i
		}
	}
	if best >= 0 {
		d.retainedPageWork.CacheHits++
		metrics.RecordRecoveryWork(metrics.RecoveryRetainedPage, metrics.RecoveryCacheHits, 1)
		copy(d.retainedCursors[best:], d.retainedCursors[best+1:])
		d.retainedCursors[len(d.retainedCursors)-1] = cursor
	} else {
		d.retainedPageWork.CacheMisses++
		metrics.RecordRecoveryWork(metrics.RecoveryRetainedPage, metrics.RecoveryCacheMisses, 1)
	}
	return cursor, tail, nil
}

func (d *DiskHandler) captureRetainedPage(ctx context.Context, cursor rawFrameCursor, budget rawFrameBudget) (rawFrameBatch, error) {
	unlock, err := d.lockRetainedPageStorage(ctx)
	if err != nil {
		return rawFrameBatch{}, err
	}
	defer unlock()
	if err := d.lineageAvailable(ctx); err != nil {
		return rawFrameBatch{}, err
	}
	batch := d.captureRawFramesLocked(ctx, cursor, budget)
	metrics.RecordRecoveryWork(metrics.RecoveryRetainedPage, metrics.RecoveryReadBytes, batch.BytesRead)
	d.retainedPageWork.BytesRead += batch.BytesRead
	d.retainedPageWork.SegmentTransitions += batch.Transitions
	return batch, nil
}

// Check exact physical membership as well as the generation before publishing
// either a checkpoint or a page. Cache eviction only causes additional reads.
func (d *DiskHandler) certifyRetainedCursors(ctx context.Context, cursors ...rawFrameCursor) error {
	unlock, err := lockProofContext(ctx, &d.mu, proofReadMutex{&d.writeFailureMu})
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.proofAvailabilityLocked(); err != nil {
		return err
	}
	for _, cursor := range cursors {
		if cursor.Generation != d.contentGenerationLocked() || len(d.segments) == 0 || cursor.Anchor != d.segments[0] {
			return ErrStaleContentGeneration
		}
		i := sort.Search(len(d.segments), func(i int) bool { return d.segments[i] >= cursor.Segment })
		if i == len(d.segments) || d.segments[i] != cursor.Segment {
			return ErrStaleContentGeneration
		}
	}
	for _, cursor := range cursors {
		d.rememberRetainedCursor(cursor)
	}
	return nil
}

func (d *DiskHandler) rememberRetainedCursor(cursor rawFrameCursor) {
	for i, old := range d.retainedCursors {
		if old == cursor {
			copy(d.retainedCursors[i:], d.retainedCursors[i+1:])
			d.retainedCursors = d.retainedCursors[:len(d.retainedCursors)-1]
			break
		}
	}
	if len(d.retainedCursors) == retainedPageCheckpointLimit {
		copy(d.retainedCursors, d.retainedCursors[1:])
		d.retainedCursors = d.retainedCursors[:retainedPageCheckpointLimit-1]
	}
	if d.retainedCursors == nil {
		d.retainedCursors = make([]rawFrameCursor, 0, retainedPageCheckpointLimit)
	}
	d.retainedCursors = append(d.retainedCursors, cursor)
	d.retainedPageWork.PeakCheckpoints = max(d.retainedPageWork.PeakCheckpoints, uint64(len(d.retainedCursors)))
}
