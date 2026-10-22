package disk

import (
	"context"
	"encoding/binary"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestRetainedPageCursorInterleavedPreviousRecordWork(t *testing.T) {
	for _, limit := range []int{1, 7, 31} {
		d, _ := bootstrapTestHandler(t, 1)
		d.SegmentSize, d.IndexSize = 1<<30, 1<<30
		const count = 512
		records := make([]types.DiskMessage, count)
		for i := range records {
			records[i] = types.DiskMessage{Topic: "orders", Partition: 1, Offset: uint64(i), Payload: strings.Repeat("x", 512)}
		}
		require.NoError(t, d.WriteBatchSync(records))
		for offset := uint64(0); offset < count; {
			if offset != 0 {
				previous, err := d.ReadRetainedPage(context.Background(), offset-1, offset, 1, 1<<20)
				require.NoError(t, err)
				require.Len(t, previous.Messages, 1)
				require.Equal(t, offset-1, previous.Messages[0].Offset)
			}
			page, err := d.ReadRetainedPage(context.Background(), offset, count, limit, 8<<10)
			require.NoError(t, err)
			require.Equal(t, offset, page.Messages[0].Offset)
			require.Greater(t, page.EndOffset, offset)
			offset = page.EndOffset
		}
		work := d.RetainedPageWorkSnapshot()
		t.Logf("record limit %d: %+v", limit, work)
		require.LessOrEqual(t, work.DecodedFrames, uint64(count*4), "historical prefix was decoded again: %+v", work)
		info, err := os.Stat(d.GetSegmentPath(0))
		require.NoError(t, err)
		require.Greater(t, work.BytesRead, uint64(info.Size()))
		require.LessOrEqual(t, work.BytesRead, uint64(info.Size()*4), "headers and read-ahead must remain bounded")
		require.Greater(t, work.CacheHits, uint64(0))
		require.LessOrEqual(t, work.PeakCheckpoints, uint64(retainedPageCheckpointLimit))
	}
}

func TestRetainedPageCursorColdSeekValidatesSkippedHistory(t *testing.T) {
	for _, damage := range []string{"topic", "partition", "order", "gap", "header", "length"} {
		t.Run(damage, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			records := []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 0}, {Topic: "orders", Partition: 1, Offset: 1}, {Topic: "orders", Partition: 1, Offset: 2}}
			require.NoError(t, d.WriteBatchSync(records))
			switch damage {
			case "topic":
				records[0].Topic = "other"
			case "partition":
				records[0].Partition = 2
			case "order":
				records[1].Offset = 0
			case "gap":
				records = records[1:]
			}
			var data []byte
			for _, record := range records {
				encoded, err := util.SerializeDiskMessage(record)
				require.NoError(t, err)
				data = binary.BigEndian.AppendUint32(data, uint32(len(encoded)))
				data = append(data, encoded...)
			}
			if damage == "header" {
				data = []byte{0}
			}
			if damage == "length" {
				binary.BigEndian.PutUint32(data[:4], MaxMessageSize+1)
			}
			require.NoError(t, os.WriteFile(d.GetSegmentPath(0), data, 0o600))
			// A forged sparse index cannot authorize skipping the damaged prefix.
			require.NoError(t, os.WriteFile(d.GetIndexPath(0), make([]byte, 64), 0o600))
			page, err := d.ReadRetainedPage(context.Background(), 2, 3, 1, 1<<20)
			require.Error(t, err)
			require.Empty(t, page.Messages)
		})
	}
}

type retainedPageTestContext struct {
	context.Context
	check func() error
}

func (c retainedPageTestContext) Err() error { return c.check() }

func TestRetainedPageCursorCancellationAndRetry(t *testing.T) {
	for _, start := range []uint64{0, 500} {
		d, _ := bootstrapTestHandler(t, 1)
		d.SegmentSize, d.IndexSize = 1<<30, 1<<30
		records := make([]types.DiskMessage, 600)
		for i := range records {
			records[i] = types.DiskMessage{Topic: "orders", Partition: 1, Offset: uint64(i)}
		}
		require.NoError(t, d.WriteBatchSync(records))
		threshold := uint64(5)
		if start != 0 {
			threshold = 300
		}
		ctx := retainedPageTestContext{Context: context.Background(), check: func() error {
			if d.retainedPageWork.DecodedFrames >= threshold {
				return context.Canceled
			}
			return nil
		}}
		page, err := d.ReadRetainedPage(ctx, start, 600, 600, 1<<20)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, page.Messages)
		if start != 0 {
			require.NotEmpty(t, d.retainedCursors)
			require.GreaterOrEqual(t, d.retainedCursors[len(d.retainedCursors)-1].End, uint64(256))
		}
		page, err = d.ReadRetainedPage(context.Background(), start, 600, 600, 1<<20)
		require.NoError(t, err)
		require.Len(t, page.Messages, 600-int(start))
		require.Equal(t, start, page.Messages[0].Offset)
		require.True(t, page.Exhausted)
		if start != 0 {
			require.Less(t, d.retainedPageWork.DecodedFrames, uint64(700))
		}
	}
}

func TestRetainedPageCursorQueuedCancellation(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	d.retainedPageMu.Lock()
	defer d.retainedPageMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	page, err := d.ReadRetainedPage(ctx, 0, 0, 1, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, page.Messages)
}

func TestRetainedPageCursorMutationDuringDetachedValidation(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 0}, {Topic: "orders", Partition: 1, Offset: 1}}))
	mutated := false
	ctx := retainedPageTestContext{Context: context.Background(), check: func() error {
		if !mutated && d.retainedPageWork.DecodedFrames != 0 && d.mu.TryLock() {
			mutated = true
			if err := d.advanceContentGenerationLocked(); err != nil {
				t.Fatal(err)
			}
			d.mu.Unlock()
		}
		return nil
	}}
	page, err := d.ReadRetainedPage(ctx, 0, 1, 1, 1<<20)
	require.True(t, mutated, "decode/publication must release storage locks")
	require.ErrorIs(t, err, ErrStaleContentGeneration)
	require.Empty(t, page.Messages)
}

func TestRetainedPageCursorAppendRotationAndDestructiveABA(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	record := types.DiskMessage{Topic: "orders", Partition: 1, Offset: 0, Payload: "old"}
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{record}))
	first, err := d.ReadRetainedPage(context.Background(), 0, 1, 1, 1<<20)
	require.NoError(t, err)
	d.SegmentSize = 1
	record.Offset = 1
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{record}))
	second, err := d.ReadRetainedPage(context.Background(), 1, 2, 1, 1<<20)
	require.NoError(t, err)
	require.Equal(t, first.ContentGeneration, second.ContentGeneration)
	require.Equal(t, uint64(1), second.Messages[0].Offset)
	require.NoError(t, d.TruncateTo(0))
	record.Offset, record.Payload = 0, "new"
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{record}))
	page, err := d.ReadRetainedPage(context.Background(), 0, 1, 1, 1<<20)
	require.NoError(t, err)
	require.NotEqual(t, first.ContentGeneration, page.ContentGeneration)
	require.Equal(t, "new", page.Messages[0].Payload)
}

func TestRetainedPageCursorEvictionConcurrentSeeksAndReopen(t *testing.T) {
	d, cfg := bootstrapTestHandler(t, 1)
	d.SegmentSize, d.IndexSize = 1<<30, 1<<30
	records := make([]types.DiskMessage, 300)
	for i := range records {
		records[i] = types.DiskMessage{Topic: "orders", Partition: 1, Offset: uint64(i)}
	}
	require.NoError(t, d.WriteBatchSync(records))
	for i := uint64(0); i < 300; i++ {
		p, err := d.ReadRetainedPage(context.Background(), i, 300, 1, 1<<20)
		require.NoError(t, err)
		require.Equal(t, i, p.Messages[0].Offset)
	}
	require.Len(t, d.retainedCursors, retainedPageCheckpointLimit)
	require.Equal(t, retainedPageCheckpointLimit, cap(d.retainedCursors))
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				start := uint64((worker*47 + i*113) % 300)
				p, err := d.ReadRetainedPage(context.Background(), start, 300, 1, 1<<20)
				if err != nil || len(p.Messages) != 1 || p.Messages[0].Offset != start {
					t.Errorf("seek %d: %+v %v", start, p, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	require.NoError(t, d.Close())
	reopened, err := NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.Empty(t, reopened.retainedCursors)
	p, err := reopened.ReadRetainedPage(context.Background(), 250, 300, 1, 1<<20)
	require.NoError(t, err)
	require.Equal(t, uint64(250), p.Messages[0].Offset)
	require.GreaterOrEqual(t, reopened.RetainedPageWorkSnapshot().DecodedFrames, uint64(252))
}

func TestRetainedPageCursorMarkedEmptyTransitions(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	d.mu.Lock()
	d.compactionMu.Lock()
	for i := uint64(0); i < 130; i++ {
		if i != 0 {
			require.NoError(t, os.WriteFile(d.GetSegmentPath(i), nil, 0o600))
			d.segments = append(d.segments, i)
		}
		d.compactedSegments[i] = 0
	}
	atomic.StoreUint64(&d.FlushedOffset, 130)
	d.compactionMu.Unlock()
	d.mu.Unlock()
	p, err := d.ReadRetainedPage(context.Background(), 0, 130, 1, 1)
	require.NoError(t, err)
	require.Empty(t, p.Messages)
	require.True(t, p.Exhausted)
	require.True(t, p.Compacted)
	require.Equal(t, uint64(130), p.EndOffset)
	work := d.RetainedPageWorkSnapshot()
	require.Equal(t, uint64(130), work.SegmentTransitions)
	require.Zero(t, work.BytesRead)
}
