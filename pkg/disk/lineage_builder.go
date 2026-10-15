package disk

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync/atomic"

	"github.com/cursus-io/cursus/util"
)

const lineageByteLimit = 1 << 20
const lineageTransitionLimit = 64
const lineageEndpointCacheLimit = 128

// All mutable slices and progress belong to lineageMu. The published d.lineage
// contains only a scalar tail; mutation paths never read builder-owned memory.
type lineageBuild struct {
	complete                                       bool
	index                                          *lineageIndex
	cursor                                         rawFrameCursor
	checkpointBytes                                uint64
	endpoints                                      map[uint64]lineageCursor
	bytesRead, recordsHashed, journalBytes, chunks uint64
}

func (d *DiskHandler) lockLineageBuilder(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("lineage requires context")
	}
	return lockProofContext(ctx, &d.lineageMu)
}

func (d *DiskHandler) checkLineageGeneration(ctx context.Context) error {
	return d.CheckContentGenerationContext(ctx, d.lineageProgress.cursor.Generation)
}

// The storage order is unchanged, but held only for durability capture or one
// bounded raw read. Decode, canonical hashing and sidecar I/O run after release;
// only the authoritative log's durability boundary may sync under these locks.
func (d *DiskHandler) lockLineageStorage(ctx context.Context) (func(), error) {
	return lockProofContext(ctx, &d.maintenanceMu, &d.mu, &d.ioMu, proofReadMutex{&d.compactionMu})
}

func (d *DiskHandler) lineageAvailable(ctx context.Context) error {
	unlock, err := lockProofContext(ctx, proofReadMutex{&d.writeFailureMu})
	if err != nil {
		return err
	}
	defer unlock()
	return d.proofAvailabilityLocked()
}

func (d *DiskHandler) lineageSnapshot(ctx context.Context, background bool) (rawFrameCursor, uint64, error) {
	unlock, err := d.lockLineageStorage(ctx)
	if err != nil {
		return rawFrameCursor{}, 0, err
	}
	defer unlock()
	if err := d.lineageAvailable(ctx); err != nil {
		return rawFrameCursor{}, 0, err
	}
	if background && (d.lineage == nil || d.lineageAutoSuppressed) {
		return rawFrameCursor{}, 0, errLineageRebuildRequired
	}
	if len(d.segments) == 0 || len(d.compactedSegments) != 0 {
		return rawFrameCursor{}, 0, ErrLineageUnsupported
	}
	if d.file == nil {
		return rawFrameCursor{}, 0, errors.New("lineage log is closed")
	}
	// FlushedOffset is a written tail. Capture its fsync witness under ioMu.
	if err := d.syncFile(d.file); err != nil {
		return rawFrameCursor{}, 0, d.markWriteUnavailable(fmt.Errorf("sync lineage log: %w", err))
	}
	start := d.segments[0]
	return rawFrameCursor{Generation: d.contentGenerationLocked(), Anchor: start, End: start, Segment: start}, atomic.LoadUint64(&d.FlushedOffset), nil
}

func lineageSidecarMatches(path string, index *lineageIndex) bool {
	info, err := os.Stat(path)
	return err == nil && index.info != nil && info.Size() == index.info.Size() && info.ModTime().Equal(index.info.ModTime()) && os.SameFile(info, index.info) && lineageHeaderMatches(path, index)
}

// prepareLineage owns lineageMu. Each call captures a fixed durable target;
// appends while hashing cannot make the builder chase a moving log forever.
func (d *DiskHandler) prepareLineage(ctx context.Context, background bool) (err error) {
	defer func() {
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errLineageRebuildRequired) {
			return
		}
		d.lineageProgress = nil
		d.mu.Lock()
		d.lineage = nil
		d.lineageAutoSuppressed = true
		d.mu.Unlock()
		util.Debug("lineage publication suspended partition=%s reason=%v", d.BaseName, err)
	}()
	initial, target, err := d.lineageSnapshot(ctx, background)
	if err != nil {
		return err
	}
	p := d.lineageProgress
	valid := p != nil && p.cursor.Generation == initial.Generation && p.cursor.Anchor == initial.Anchor && p.cursor.End <= target
	if valid && p.index.info != nil {
		valid = lineageSidecarMatches(d.BaseName+".lineage", p.index)
	}
	if !valid {
		if background {
			d.mu.Lock()
			d.lineage = nil
			d.lineageAutoSuppressed = true
			d.mu.Unlock()
			d.lineageProgress = nil
			return errLineageRebuildRequired
		}
		d.mu.Lock()
		d.lineage = nil
		d.mu.Unlock()
		cursor := lineageCursor{EndOffset: initial.End, Segment: initial.Segment, Digest: lineageSeed(initial.Anchor)}
		p = &lineageBuild{index: &lineageIndex{start: initial.Anchor, tail: cursor, checkpoints: []lineageCursor{cursor}}, cursor: initial, endpoints: make(map[uint64]lineageCursor)}
		d.lineageProgress = p
	}
	if p.complete && p.index.info != nil && p.cursor.End == target {
		return d.publishLineageView(ctx)
	}
	for {
		exhausted, err := d.advanceLineageChunk(ctx, target)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if exhausted {
			return d.publishLineageView(ctx)
		}
		if background {
			return nil
		}
	}
}

func (d *DiskHandler) captureLineageBatch(ctx context.Context, cursor rawFrameCursor, budget rawFrameBudget) (rawFrameBatch, error) {
	unlock, err := d.lockLineageStorage(ctx)
	if err != nil {
		return rawFrameBatch{}, err
	}
	defer unlock()
	if err := d.lineageAvailable(ctx); err != nil {
		return rawFrameBatch{}, err
	}
	b := d.captureRawFramesLocked(ctx, cursor, budget)
	d.lineageProgress.bytesRead += b.BytesRead
	return b, nil
}

// advanceLineageChunk stages at most one bounded chunk; no full-history slice
// clone is needed. Completed checkpoints and epochs append only after fsync.
func (d *DiskHandler) advanceLineageChunk(ctx context.Context, target uint64) (bool, error) {
	p := d.lineageProgress
	b, err := d.captureLineageBatch(ctx, p.cursor, rawFrameBudget{lineageByteLimit, lineageBackgroundRecordLimit, lineageTransitionLimit})
	if err != nil {
		return false, err
	}
	cursor, tail := p.cursor, p.index.tail
	checkpointBytes := p.checkpointBytes
	var checkpoints []lineageCursor
	var epochs []LineageEpochSpan
	if n := len(p.index.epochs); n != 0 {
		epochs = append(epochs, p.index.epochs[n-1])
	}
	var events []lineageJournalRecord
	exhausted := false
	for _, frame := range b.Frames {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		next, message, included, done, err := d.validateRawFrame(cursor, frame, rawFramesContiguous, target, b.Tail)
		if err != nil {
			return false, err
		}
		cursor = next
		if included {
			digest, err := lineageHash(tail.Digest, message)
			if err != nil {
				return false, err
			}
			tail = lineageCursor{EndOffset: next.End, Segment: next.Segment, Position: next.Position, Digest: digest}
			p.recordsHashed++
			if len(epochs) == 0 || epochs[len(epochs)-1].LeaderEpoch != message.LeaderEpoch {
				if len(epochs) != 0 {
					span := epochs[len(epochs)-1]
					events = append(events, lineageJournalRecord{Kind: 3, StartOffset: span.StartOffset, EndOffset: span.EndOffset, LeaderEpoch: span.LeaderEpoch})
				}
				epochs = append(epochs, LineageEpochSpan{StartOffset: message.Offset, LeaderEpoch: message.LeaderEpoch})
			}
			epochs[len(epochs)-1].EndOffset = next.End
			checkpointBytes += uint64(len(frame.Payload)) + 4
			if (next.End-p.index.start)%lineageCheckpointInterval == 0 || checkpointBytes >= lineageByteLimit {
				checkpoints = append(checkpoints, tail)
				events = append(events, lineageJournalRecord{Kind: 2, StartOffset: p.index.start, EndOffset: tail.EndOffset, Segment: tail.Segment, Position: tail.Position, Digest: tail.Digest})
				checkpointBytes = 0
			}
		}
		if done {
			exhausted = true
			break
		}
	}
	if !exhausted && b.Err != nil {
		return false, b.Err
	}
	if len(b.Frames) == 0 {
		return false, errors.New("lineage raw scan made no progress")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := d.checkLineageGeneration(ctx); err != nil {
		return false, err
	}
	// The journal transaction is bounded and finishes once started. Cancellation
	// during fsync retains this completed chunk, but cannot return a proof.
	tail.Segment, tail.Position = cursor.Segment, cursor.Position
	staged := &lineageIndex{start: p.index.start, tail: tail, epochs: epochs}
	fresh := p.index.info == nil
	if err := d.publishLineage(context.WithoutCancel(ctx), d.BaseName+".lineage", staged, fresh, events); err != nil {
		return false, fmt.Errorf("publish derived lineage: %w", err)
	}
	if err := d.checkLineageGeneration(context.WithoutCancel(ctx)); err != nil {
		return false, err
	}
	if len(p.index.epochs) > 0 {
		p.index.epochs[len(p.index.epochs)-1] = epochs[0]
		epochs = epochs[1:]
	}
	p.index.epochs = append(p.index.epochs, epochs...)
	p.index.checkpoints = append(p.index.checkpoints, checkpoints...)
	p.index.tail, p.index.info = tail, staged.info
	p.cursor, p.checkpointBytes = cursor, checkpointBytes
	p.complete = exhausted
	p.chunks++
	p.journalBytes += uint64(len(events)+1) * uint64(len(lineageRecordBytes(lineageJournalRecord{})))
	if fresh {
		p.journalBytes += uint64(len(lineageMagic) + len(lineageRecordBytes(lineageJournalRecord{})))
	}
	return exhausted, nil
}

// Only scalar state is published under mu; checkpoints and epoch backing arrays
// remain private to the builder gate. Never publish after shutdown or mutation.
func (d *DiskHandler) publishLineageView(ctx context.Context) error {
	unlock, err := lockProofContext(ctx, &d.mu, proofReadMutex{&d.writeFailureMu})
	if err != nil {
		return err
	}
	defer unlock()
	if err := d.proofAvailabilityLocked(); err != nil {
		return err
	}
	p := d.lineageProgress
	if p.cursor.Generation != d.contentGenerationLocked() {
		return ErrStaleContentGeneration
	}
	if d.lineage == nil || d.lineage.tail != p.index.tail {
		d.lineage = &lineageIndex{start: p.index.start, tail: p.index.tail}
	}
	d.lineageAutoSuppressed = false
	return nil
}

func (d *DiskHandler) lineagePrefix(ctx context.Context, end uint64) (LineagePrefix, error) {
	p := d.lineageProgress
	cursor := p.index.tail
	if end != cursor.EndOffset {
		if cached, ok := p.endpoints[end]; ok {
			cursor = cached
		} else {
			i := sort.Search(len(p.index.checkpoints), func(i int) bool { return p.index.checkpoints[i].EndOffset > end })
			cursor = p.index.checkpoints[i-1]
			for cursor.EndOffset < end {
				raw := p.cursor
				raw.End, raw.Segment, raw.Position = cursor.EndOffset, cursor.Segment, cursor.Position
				b, err := d.captureLineageBatch(ctx, raw, rawFrameBudget{lineageByteLimit, lineageCheckpointInterval, lineageTransitionLimit})
				if err != nil {
					return LineagePrefix{}, err
				}
				exhausted := false
				for _, frame := range b.Frames {
					if err := ctx.Err(); err != nil {
						return LineagePrefix{}, err
					}
					next, message, included, done, err := d.validateRawFrame(raw, frame, rawFramesContiguous, end, b.Tail)
					if err != nil {
						return LineagePrefix{}, err
					}
					raw = next
					if included {
						digest, err := lineageHash(cursor.Digest, message)
						if err != nil {
							return LineagePrefix{}, err
						}
						cursor = lineageCursor{EndOffset: next.End, Segment: next.Segment, Position: next.Position, Digest: digest}
						p.recordsHashed++
					}
					if done {
						exhausted = true
						break
					}
				}
				if !exhausted && b.Err != nil {
					return LineagePrefix{}, b.Err
				}
				cursor.Segment, cursor.Position = raw.Segment, raw.Position
				if len(b.Frames) == 0 {
					return LineagePrefix{}, errors.New("lineage endpoint scan made no progress")
				}
			}
			if err := d.checkLineageGeneration(ctx); err != nil {
				return LineagePrefix{}, err
			}
			if len(p.endpoints) >= lineageEndpointCacheLimit {
				clear(p.endpoints)
			}
			p.endpoints[end] = cursor
		}
	}
	return LineagePrefix{StartOffset: p.index.start, EndOffset: end, Digest: hex.EncodeToString(cursor.Digest[:])}, nil
}

func (d *DiskHandler) requestLineageUpdate() {
	select {
	case d.lineageRequests <- struct{}{}:
	default:
	}
}

// One coalesced worker never initializes history and never runs on the data
// acknowledgement path. Close cancels queued lock waits and joins this worker.
func (d *DiskHandler) lineageLoop() {
	ctx := lineageWorkerContext{Context: context.Background(), done: d.done}
	for {
		select {
		case <-d.done:
			return
		case <-d.lineageRequests:
			unlock, err := d.lockLineageBuilder(ctx)
			if err != nil {
				return
			}
			_ = d.prepareLineage(ctx, true)
			unlock()
		}
	}
}

type lineageWorkerContext struct {
	context.Context
	done <-chan struct{}
}

func (c lineageWorkerContext) Done() <-chan struct{} { return c.done }
func (c lineageWorkerContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}
