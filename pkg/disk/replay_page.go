package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// ReadReplayPage is a sequential, strict reader for committed cleanup proofs.
// It bypasses sparse indexes (including their validation/fallback scans), reads
// at most one segment, and counts every decoded record against both budgets.
// The caller must exclude log replacement/rollback and invalidate the cursor
// when the partition identity, mutation generation or retained start changes.
func (dh *DiskHandler) ReadReplayPage(offset, end uint64, cursor types.ReplayCursor, maxRecords, maxBytes int) (types.ReplayPage, error) {
	return dh.ReadReplayPageContext(context.Background(), offset, end, cursor, maxRecords, maxBytes)
}

func (dh *DiskHandler) ReadReplayPageContext(ctx context.Context, offset, end uint64, cursor types.ReplayCursor, maxRecords, maxBytes int) (types.ReplayPage, error) {
	page := types.ReplayPage{Next: offset, Cursor: cursor}
	if maxRecords <= 0 || maxBytes <= 0 || offset >= end {
		return page, fmt.Errorf("invalid replay page bounds")
	}
	atomic.AddInt32(&dh.activeReaders, 1)
	defer atomic.AddInt32(&dh.activeReaders, -1)
	unlock, err := lockProofContext(ctx, &dh.mu)
	if err != nil {
		return page, err
	}
	count := len(dh.segments)
	if count == 0 || dh.segments[count-1] != dh.CurrentSegment {
		count++
	}
	segmentAt := func(index int) uint64 {
		if index == len(dh.segments) {
			return dh.CurrentSegment
		}
		return dh.segments[index]
	}
	var index int
	if cursor.Initialized {
		index = sort.Search(count, func(i int) bool { return segmentAt(i) >= cursor.Segment })
		if index == count || segmentAt(index) != cursor.Segment {
			unlock()
			return page, fmt.Errorf("replay cursor segment is no longer retained")
		}
	} else {
		index = sort.Search(count, func(i int) bool { return segmentAt(i) > offset }) - 1
		if index < 0 {
			unlock()
			return page, fmt.Errorf("replay offset precedes retained segments")
		}
		page.Cursor = types.ReplayCursor{Initialized: true, Segment: segmentAt(index)}
	}
	hasNext := index+1 < count
	var nextSegment uint64
	if hasNext {
		nextSegment = segmentAt(index + 1)
	}
	unlock()
	path := dh.GetSegmentPath(page.Cursor.Segment)
	// #nosec G304 -- path is derived from the broker-owned segment directory.
	file, err := os.Open(path)
	if err != nil {
		return page, err
	}
	defer func() { _ = file.Close() }()
	stat, err := file.Stat()
	if err != nil {
		return page, err
	}
	sizeOnDisk := stat.Size()
	if sizeOnDisk < 0 {
		return page, fmt.Errorf("invalid replay segment length")
	}
	segmentBytes := uint64(sizeOnDisk)
	if page.Cursor.Position > segmentBytes || page.Cursor.Position > math.MaxInt64-4 {
		return page, fmt.Errorf("replay cursor exceeds segment length")
	}
	var length [4]byte
	for page.Records < maxRecords && page.Bytes < maxBytes {
		if err := ctx.Err(); err != nil {
			return page, err
		}
		if page.Cursor.Position == segmentBytes {
			if hasNext {
				page.Cursor.Segment = nextSegment
				page.Cursor.Position = 0
			} else {
				page.Next = end // A compacted suffix may contain no records.
			}
			return page, nil
		}
		// #nosec G115 -- cursor position is checked against MaxInt64 above and
		// advanced only within stat.Size().
		position := int64(page.Cursor.Position)
		if _, err := file.ReadAt(length[:], position); err != nil {
			return page, fmt.Errorf("read replay record length: %w", err)
		}
		size := binary.BigEndian.Uint32(length[:])
		if size == 0 || size > MaxMessageSize {
			return page, fmt.Errorf("invalid replay record length %d", size)
		}
		if uint64(size)+4 > segmentBytes-page.Cursor.Position {
			return page, fmt.Errorf("truncated committed replay record")
		}
		if int(size) > maxBytes-page.Bytes {
			if page.Records == 0 {
				return page, &types.ReplayByteBudget{Size: int(size), Budget: maxBytes}
			}
			return page, nil
		}
		payload := make([]byte, int(size))
		if _, err := file.ReadAt(payload, position+4); err != nil {
			return page, fmt.Errorf("read replay record: %w", err)
		}
		page.Bytes += int(size)
		page.Records++
		message, err := util.DeserializeDiskMessage(payload)
		if err != nil {
			return page, fmt.Errorf("decode replay record: %w", err)
		}
		if page.Cursor.HasLast && message.Offset <= page.Cursor.LastOffset {
			return page, fmt.Errorf("non-increasing replay offset")
		}
		if message.Offset >= end {
			page.Next = end
			return page, nil
		}
		page.Cursor.Position += 4 + uint64(size)
		page.Cursor.LastOffset, page.Cursor.HasLast = message.Offset, true
		if message.Offset < offset {
			continue
		}
		page.Messages = append(page.Messages, retainedMessage(message))
		page.Next = message.Offset + 1
		if page.Next == end {
			return page, nil
		}
	}
	return page, nil
}
