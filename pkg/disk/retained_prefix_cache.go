package disk

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
)

const retainedPrefixCheckpointLimit = 128

// RetainedPrefixWork counts actual local scan work, including failed and
// canceled requests. BytesRead includes headers and lookahead; CanonicalBytes
// counts serialized bytes hashed. These are logical I/O, not device I/O counts.
type RetainedPrefixWork struct {
	BytesRead, DecodedFrames, CanonicalBytes, SegmentTransitions uint64
	CacheHits, CacheMisses, PeakCheckpoints, SyncCalls           uint64
}

func (d *DiskHandler) RetainedPrefixWorkSnapshot() RetainedPrefixWork {
	d.retainedPrefixMu.Lock()
	defer d.retainedPrefixMu.Unlock()
	return d.retainedPrefixWork
}

func (d *DiskHandler) retainedPrefixSnapshot(ctx context.Context, generation ContentGeneration, start, end uint64) (retainedPrefixCheckpoint, uint64, error) {
	unlock, err := d.lockRetainedPageStorage(ctx)
	if err != nil {
		return retainedPrefixCheckpoint{}, 0, err
	}
	defer unlock()
	if err := d.lineageAvailable(ctx); err != nil {
		return retainedPrefixCheckpoint{}, 0, err
	}
	if generation != d.contentGenerationLocked() {
		return retainedPrefixCheckpoint{}, 0, ErrStaleContentGeneration
	}
	tail := atomic.LoadUint64(&d.FlushedOffset)
	if len(d.segments) == 0 || start != d.segments[0] || start > end || end > tail || d.file == nil {
		return retainedPrefixCheckpoint{}, 0, fmt.Errorf("retained prefix outside durable retained anchor")
	}
	d.retainedPrefixWork.SyncCalls++
	if err := d.syncFile(d.file); err != nil {
		return retainedPrefixCheckpoint{}, 0, d.markWriteUnavailable(fmt.Errorf("sync retained prefix: %w", err))
	}
	checkpoint := retainedPrefixCheckpoint{
		cursor: rawFrameCursor{Generation: generation, Anchor: start, End: start, Segment: start},
		chain:  retainedPrefixSeed(start), complete: start < tail,
	}
	if len(d.retainedPrefixes) != 0 && (d.retainedPrefixes[0].cursor.Generation != generation || d.retainedPrefixes[0].cursor.Anchor != start) {
		d.retainedPrefixes = nil
	}
	best := -1
	for i, candidate := range d.retainedPrefixes {
		if candidate.cursor.End <= end && candidate.cursor.End >= checkpoint.cursor.End {
			checkpoint, best = candidate, i
		}
	}
	if best >= 0 {
		d.retainedPrefixWork.CacheHits++
		copy(d.retainedPrefixes[best:], d.retainedPrefixes[best+1:])
		d.retainedPrefixes[len(d.retainedPrefixes)-1] = checkpoint
	} else {
		d.retainedPrefixWork.CacheMisses++
	}
	return checkpoint, tail, nil
}

func (d *DiskHandler) captureRetainedPrefix(ctx context.Context, cursor rawFrameCursor, budget rawFrameBudget) (rawFrameBatch, error) {
	unlock, err := d.lockRetainedPageStorage(ctx)
	if err != nil {
		return rawFrameBatch{}, err
	}
	defer unlock()
	if err := d.lineageAvailable(ctx); err != nil {
		return rawFrameBatch{}, err
	}
	batch := d.captureRawFramesLocked(ctx, cursor, budget)
	d.retainedPrefixWork.BytesRead += batch.BytesRead
	d.retainedPrefixWork.SegmentTransitions += batch.Transitions
	return batch, nil
}

func (d *DiskHandler) commitRetainedPrefix(ctx context.Context, checkpoint retainedPrefixCheckpoint) error {
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
	cursor := checkpoint.cursor
	if cursor.Generation != d.contentGenerationLocked() || len(d.segments) == 0 || cursor.Anchor != d.segments[0] {
		return ErrStaleContentGeneration
	}
	i := sort.Search(len(d.segments), func(i int) bool { return d.segments[i] >= cursor.Segment })
	if i == len(d.segments) || d.segments[i] != cursor.Segment {
		return ErrStaleContentGeneration
	}
	d.rememberRetainedPrefix(checkpoint)
	return nil
}

// LRU eviction preserves the furthest certified boundary, so historical probes
// cannot evict the prefix needed by the next suffix request. Evicted historical
// requests may rescan; no payloads or unbounded checkpoint history are retained.
func (d *DiskHandler) rememberRetainedPrefix(checkpoint retainedPrefixCheckpoint) {
	for i, old := range d.retainedPrefixes {
		if old.cursor.End == checkpoint.cursor.End {
			copy(d.retainedPrefixes[i:], d.retainedPrefixes[i+1:])
			d.retainedPrefixes = d.retainedPrefixes[:len(d.retainedPrefixes)-1]
			break
		}
	}
	if len(d.retainedPrefixes) == retainedPrefixCheckpointLimit {
		furthest := uint64(0)
		for _, old := range d.retainedPrefixes {
			furthest = max(furthest, old.cursor.End)
		}
		evict := 0
		if d.retainedPrefixes[0].cursor.End == furthest && checkpoint.cursor.End < furthest {
			evict = 1
		}
		copy(d.retainedPrefixes[evict:], d.retainedPrefixes[evict+1:])
		d.retainedPrefixes = d.retainedPrefixes[:retainedPrefixCheckpointLimit-1]
	}
	if d.retainedPrefixes == nil {
		d.retainedPrefixes = make([]retainedPrefixCheckpoint, 0, retainedPrefixCheckpointLimit)
	}
	d.retainedPrefixes = append(d.retainedPrefixes, checkpoint)
	d.retainedPrefixWork.PeakCheckpoints = max(d.retainedPrefixWork.PeakCheckpoints, uint64(len(d.retainedPrefixes)))
}
