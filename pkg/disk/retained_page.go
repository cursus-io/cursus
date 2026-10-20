package disk

import (
	"context"
	"errors"
	"fmt"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/types"
)

var ErrRetainedPageLimit = errors.New("first retained record exceeds page byte limit")

// RetainedPage contains every physical record in [requested start, EndOffset),
// including transaction data and control markers. EndOffset is exclusive;
// Exhausted means the entire fixed requested interval has been certified.
// DecodedBytes counts serialized record payload bytes (excluding frame headers).
// Compacted indicates segment-local marked gap authority was used by the scan.
// ContentGeneration is local only and cannot authenticate a remote broker.
type RetainedPage struct {
	Messages          []types.Message
	EndOffset         uint64
	Exhausted         bool
	Compacted         bool
	DecodedBytes      int
	ContentGeneration ContentGeneration
}

// ReadRetainedPage reads a bounded, detached raw page. Cold seeks validate from
// segment base; warm seeks reuse only locally certified physical checkpoints.
// The first omitted frame is validated and retained for the next page. Callers
// must not hold partition locks; use ContentGeneration to fence publication.
func (d *DiskHandler) ReadRetainedPage(ctx context.Context, start, end uint64, maxRecords, maxBytes int) (RetainedPage, error) {
	if ctx == nil || maxRecords <= 0 || maxBytes <= 0 {
		return RetainedPage{}, fmt.Errorf("retained page requires context and positive limits")
	}
	unlock, err := lockProofContext(ctx, &d.retainedPageMu)
	if err != nil {
		return RetainedPage{}, err
	}
	defer unlock()
	cursor, tail, err := d.retainedPageSnapshot(ctx, start, end)
	if err != nil {
		return RetainedPage{}, err
	}
	page := RetainedPage{EndOffset: start, ContentGeneration: cursor.Generation}
	lastBefore := cursor
	finish := func(exhausted bool, boundary uint64) (RetainedPage, error) {
		if err := d.certifyRetainedCursors(ctx, lastBefore, cursor); err != nil {
			return RetainedPage{}, err
		}
		page.Exhausted, page.EndOffset = exhausted, boundary
		return page, nil
	}
	if start == end {
		return finish(true, end)
	}
	for {
		// #nosec G115 -- maxRecords is positive and the page returns as soon as len(Messages) reaches it.
		records := uint64(min(retainedPageChunkRecords-1, maxRecords-len(page.Messages))) + 1
		if cursor.End < start {
			records = retainedPageChunkRecords
		}
		// A fixed prefix cannot need more frames than its logical distance.
		if end < tail && cursor.End < end {
			records = min(records, end-cursor.End)
		}
		budget := rawFrameBudget{Bytes: uint64(max(1, min(retainedPageChunkBytes, maxBytes-page.DecodedBytes))), Records: records, SegmentTransitions: retainedPageChunkTransitions}
		batch, err := d.captureRetainedPage(ctx, cursor, budget)
		if err != nil {
			return RetainedPage{}, err
		}
		for _, frame := range batch.Frames {
			if err := ctx.Err(); err != nil {
				return RetainedPage{}, err
			}
			if !frame.EOF {
				d.retainedPageWork.DecodedFrames++
				metrics.RecordRecoveryWork(metrics.RecoveryRetainedPage, metrics.RecoveryDecodedFrames, 1)
				metrics.RecordRecoveryBytes(metrics.RecoveryRetainedPage, metrics.RecoveryDecodedBytes, int64(len(frame.Payload)))
			}
			next, message, included, done, err := d.validateRawFrame(cursor, frame, rawFramesMarkedGaps, end, batch.Tail)
			if err != nil {
				return RetainedPage{}, err
			}
			page.Compacted = page.Compacted || frame.Marked
			if included && message.Offset >= start {
				if len(page.Messages) == maxRecords || len(frame.Payload) > maxBytes-page.DecodedBytes {
					if len(page.Messages) == 0 {
						return RetainedPage{}, ErrRetainedPageLimit
					}
					// The gap is certified, but the physical position stays before
					// the omitted record so the next call cannot lose its payload.
					cursor.End = message.Offset
					return finish(false, message.Offset)
				}
				lastBefore = cursor
				page.Messages = append(page.Messages, retainedMessage(message))
				page.DecodedBytes += len(frame.Payload)
			}
			cursor = next
			if done {
				return finish(true, end)
			}
		}
		if batch.Err != nil {
			return RetainedPage{}, batch.Err
		}
		if len(batch.Frames) == 0 {
			return RetainedPage{}, fmt.Errorf("retained page scan made no progress")
		}
		// Certified skipped prefixes survive cancellation of a later chunk.
		// Output checkpoints never exceed a retry's start unless that retry can
		// select an earlier checkpoint and decode its output again.
		if err := d.certifyRetainedCursors(ctx, lastBefore, cursor); err != nil {
			return RetainedPage{}, err
		}
	}
}
