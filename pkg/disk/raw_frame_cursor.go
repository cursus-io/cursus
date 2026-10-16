package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

type rawFrameMode uint8

const (
	rawFramesContiguous rawFrameMode = iota
	rawFramesMarkedGaps
)

// A cursor is a process-local witness, never a durable or remote authority.
// End is the certified logical boundary; Segment/Position precede the next
// physical frame. Append and rotation preserve it, prefix mutations do not.
type rawFrameCursor struct {
	Generation           ContentGeneration
	Anchor, End, Segment uint64
	Position             int64
}

type rawFrameBudget struct{ Bytes, Records, SegmentTransitions uint64 }

type rawFrame struct {
	Payload           []byte
	Before, After     rawFrameCursor
	Base, Limit       uint64
	Marked, EOF, Last bool
}

type rawFrameBatch struct {
	Frames                 []rawFrame
	BytesRead, Transitions uint64
	Tail                   uint64
	Err                    error
}

// captureRawFramesLocked owns mu/ioMu and compactionMu.RLock. ReadAt avoids
// hidden buffering. A first oversized record may exceed Bytes by at most one
// MaxMessageSize payload plus its four-byte header. Header lookahead is charged
// even when its payload is omitted; the cursor remains BEFORE that frame.
// Errors are deferred so an earlier fixed prefix can stop before future bytes.
func (d *DiskHandler) captureRawFramesLocked(ctx context.Context, cursor rawFrameCursor, budget rawFrameBudget) rawFrameBatch {
	b := rawFrameBatch{Tail: atomic.LoadUint64(&d.FlushedOffset)}
	if budget.Bytes == 0 || budget.Records == 0 || budget.SegmentTransitions == 0 {
		b.Err = fmt.Errorf("raw scan requires positive budgets")
		return b
	}
	if cursor.Generation != d.contentGenerationLocked() || len(d.segments) == 0 || cursor.Anchor != d.segments[0] {
		b.Err = ErrStaleContentGeneration
		return b
	}
	if cursor.End < cursor.Anchor || cursor.End > b.Tail {
		b.Err = fmt.Errorf("raw cursor outside retained logical bounds")
		return b
	}
	i := sort.Search(len(d.segments), func(i int) bool { return d.segments[i] >= cursor.Segment })
	if i == len(d.segments) || d.segments[i] != cursor.Segment || cursor.Position < 0 {
		b.Err = ErrStaleContentGeneration
		return b
	}
	var records uint64
	for i < len(d.segments) {
		if err := ctx.Err(); err != nil {
			b.Err = err
			return b
		}
		base, limit := d.segments[i], b.Tail
		if i+1 < len(d.segments) {
			limit = d.segments[i+1]
		}
		// #nosec G304 -- generated partition-owned segment path.
		f, err := os.Open(d.GetSegmentPath(base))
		if err != nil {
			b.Err = err
			return b
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			b.Err = err
			return b
		}
		markedSize, marked := d.compactedSegments[base]
		marked = marked && markedSize == info.Size()
		for {
			if err := ctx.Err(); err != nil {
				_ = f.Close()
				b.Err = err
				return b
			}
			if records >= budget.Records || b.BytesRead+4 > budget.Bytes && records != 0 {
				_ = f.Close()
				return b
			}
			frame := rawFrame{Before: cursor, After: cursor, Base: base, Limit: limit, Marked: marked, Last: i+1 == len(d.segments)}
			var header [4]byte
			n, err := f.ReadAt(header[:], cursor.Position)
			// #nosec G115 -- os.File.ReadAt returns a nonnegative count, including partial reads.
			b.BytesRead += uint64(n)
			if err == io.EOF && n == 0 {
				if cursor.Position != info.Size() {
					_ = f.Close()
					b.Err = fmt.Errorf("raw cursor beyond EOF")
					return b
				}
				if b.Transitions >= budget.SegmentTransitions {
					_ = f.Close()
					return b
				}
				b.Transitions++
				frame.EOF = true
				if !frame.Last {
					frame.After.Segment, frame.After.Position = d.segments[i+1], 0
				}
				b.Frames = append(b.Frames, frame)
				cursor = frame.After
				_ = f.Close()
				if frame.Last || b.Transitions == budget.SegmentTransitions {
					return b
				}
				i++
				break
			}
			if err != nil {
				_ = f.Close()
				b.Err = fmt.Errorf("raw frame header: %w", err)
				return b
			}
			size := binary.BigEndian.Uint32(header[:])
			if size == 0 || size > MaxMessageSize {
				_ = f.Close()
				b.Err = fmt.Errorf("invalid raw record size %d", size)
				return b
			}
			if records != 0 && b.BytesRead+uint64(size) > budget.Bytes {
				_ = f.Close()
				return b
			}
			frame.Payload = make([]byte, size)
			n, err = f.ReadAt(frame.Payload, cursor.Position+4)
			// #nosec G115 -- os.File.ReadAt returns a nonnegative count bounded by the supplied payload buffer.
			b.BytesRead += uint64(n)
			if err != nil {
				_ = f.Close()
				b.Err = fmt.Errorf("raw frame payload: %w", err)
				return b
			}
			frame.After.Position += int64(size) + 4
			b.Frames = append(b.Frames, frame)
			cursor = frame.After
			records++
		}
	}
	return b
}

// validateRawFrame runs without storage locks. The caller commits Next only
// after checking the generation again. EOF events certify marked trailing holes
// and unmarked completeness, including empty segments and partial headers.
func (d *DiskHandler) validateRawFrame(cursor rawFrameCursor, frame rawFrame, mode rawFrameMode, end, tail uint64) (next rawFrameCursor, message types.DiskMessage, included, exhausted bool, err error) {
	next = cursor
	if mode != rawFramesContiguous && mode != rawFramesMarkedGaps {
		err = fmt.Errorf("unknown raw frame validation mode")
		return
	}
	if end < cursor.End || end > tail || cursor.End < cursor.Anchor {
		err = fmt.Errorf("raw frame endpoint outside retained bounds")
		return
	}
	if frame.Before.Generation != cursor.Generation || frame.Before.Anchor != cursor.Anchor || frame.Before.Segment != cursor.Segment || frame.Before.Position != cursor.Position {
		err = fmt.Errorf("raw frame does not continue the physical cursor")
		return
	}
	if frame.Marked && mode == rawFramesContiguous {
		err = ErrLineageUnsupported
		return
	}
	if frame.EOF {
		if cursor.End > frame.Limit || !frame.Marked && cursor.End != frame.Limit {
			err = ErrLineageUnsupported
			return
		}
		next = frame.After
		next.End = frame.Limit
		if end < frame.Limit {
			next = cursor
			next.End = end
			exhausted = true
			return
		}
		if next.End >= end {
			next.End = end
			exhausted = end < tail || frame.Last
		}
		return
	}
	message, err = util.DeserializeDiskMessage(frame.Payload)
	if err != nil {
		return
	}
	if message.Topic != d.topicName || int(message.Partition) != d.partitionID {
		err = fmt.Errorf("raw record identity mismatch")
		return
	}
	if message.Offset < frame.Base || message.Offset >= frame.Limit || message.Offset < cursor.End || message.Offset == math.MaxUint64 {
		err = fmt.Errorf("record outside ordered physical segment bounds")
		return
	}
	if !frame.Marked && message.Offset != cursor.End {
		err = ErrLineageUnsupported
		return
	}
	if message.Offset >= end {
		next.End = end
		exhausted = true
		return
	}
	next = frame.After
	next.End = message.Offset + 1
	included = true
	exhausted = next.End == end && end < tail
	return
}
