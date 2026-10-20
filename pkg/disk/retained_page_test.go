package disk

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestRetainedPageCertifiedCutoffsAndRawTransactions(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	records := []types.DiskMessage{
		{Topic: "orders", Partition: 1, Offset: 2, Payload: "aborted", TransactionalID: "tx", TransactionState: types.TransactionStateAborted},
		{Topic: "orders", Partition: 1, Offset: 5, Payload: "marker", TransactionalID: "tx", TransactionMarker: types.TransactionMarkerAbort},
	}
	require.NoError(t, d.WriteCompactedReplicaRange(0, 10, records))
	encoded, err := util.SerializeDiskMessage(records[0])
	require.NoError(t, err)
	p, err := d.ReadRetainedPage(context.Background(), 0, 10, 10, len(encoded))
	require.NoError(t, err)
	require.Len(t, p.Messages, 1)
	require.Equal(t, uint64(5), p.EndOffset)
	require.False(t, p.Exhausted)
	require.True(t, p.Compacted)
	require.Equal(t, len(encoded), p.DecodedBytes)
	require.Equal(t, types.TransactionStateAborted, p.Messages[0].TransactionState)
	require.NoError(t, d.CheckContentGeneration(p.ContentGeneration))
	p, err = d.ReadRetainedPage(context.Background(), 5, 10, 1, 1<<20)
	require.NoError(t, err)
	require.True(t, p.Exhausted)
	require.Equal(t, uint64(10), p.EndOffset)
	require.Equal(t, types.TransactionMarkerAbort, p.Messages[0].TransactionMarker)
	p, err = d.ReadRetainedPage(context.Background(), 6, 9, 1, 1<<20)
	require.NoError(t, err)
	require.Empty(t, p.Messages)
	require.True(t, p.Exhausted)
	require.Equal(t, uint64(9), p.EndOffset)
	_, err = d.ReadRetainedPage(context.Background(), 0, 10, 1, 1)
	require.ErrorIs(t, err, ErrRetainedPageLimit)
}

func TestRetainedPageBoundsCancellationAndUnmarkedGap(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.WriteCompactedReplicaRange(0, 4, nil))
	_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "raw"})
	require.NoError(t, err)
	for _, bounds := range [][4]int{{0, 6, 1, 99}, {3, 2, 1, 99}, {0, 5, 0, 99}, {0, 5, 1, 0}} {
		_, err = d.ReadRetainedPage(context.Background(), uint64(bounds[0]), uint64(bounds[1]), bounds[2], bounds[3])
		require.Error(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = d.ReadRetainedPage(ctx, 0, 5, 1, 1<<20)
	require.ErrorIs(t, err, context.Canceled)
	_, err = d.RetainedRangeDigestContext(ctx, 0, 5)
	require.ErrorIs(t, err, context.Canceled)
	p, err := d.ReadRetainedPage(context.Background(), 4, 4, 1, 1)
	require.NoError(t, err)
	require.True(t, p.Exhausted)
	require.Equal(t, uint64(4), p.EndOffset)
	require.NoError(t, os.Truncate(d.GetSegmentPath(4), 0))
	_, err = d.ReadRetainedPage(context.Background(), 0, 5, 10, 1<<20)
	require.Error(t, err)
}

func TestRetainedPageRejectsFailureBeforeStamp(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "raw"})
	require.NoError(t, err)
	injected := errors.New("terminal failure before page stamp")
	d.mu.Lock()
	d.ioMu.Lock()
	d.syncFileFn = func(f *os.File) error {
		_ = d.markWriteUnavailable(injected)
		return f.Sync()
	}
	d.ioMu.Unlock()
	d.mu.Unlock()
	_, err = d.ReadRetainedPage(context.Background(), 0, 1, 1, 1<<20)
	require.ErrorIs(t, err, injected)
}

func TestRetainedDigestRejectsClosedHandler(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.Close())
	_, err := d.RetainedRangeDigestContext(context.Background(), 0, 0)
	require.Error(t, err)
}

type enteredProofContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
}

func (c *enteredProofContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.entered) })
	return err
}

func TestRetainedProofCancellationWhileWaitingForMaintenance(t *testing.T) {
	for _, operation := range []string{"page", "digest"} {
		t.Run(operation, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &enteredProofContext{Context: base, entered: make(chan struct{})}
			d.maintenanceMu.Lock()
			result := make(chan error, 1)
			go func() {
				var err error
				if operation == "page" {
					_, err = d.ReadRetainedPage(ctx, 0, 0, 1, 1)
				} else {
					_, err = d.RetainedRangeDigestContext(ctx, 0, 0)
				}
				result <- err
			}()
			<-ctx.entered // first context check already observed nil
			cancel()
			select {
			case err := <-result:
				d.maintenanceMu.Unlock()
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				d.maintenanceMu.Unlock()
				<-result
				t.Error("context cancellation waited for maintenance lock")
			}
		})
	}
}

func TestRetainedPageStrictPhysicalValidation(t *testing.T) {
	for _, damage := range []string{"topic", "partition", "order", "hole", "segment-bound", "partial-frame", "partial-header", "bad-length"} {
		t.Run(damage, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			records := []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 0}, {Topic: "orders", Partition: 1, Offset: 1}, {Topic: "orders", Partition: 1, Offset: 2}}
			require.NoError(t, d.WriteBatchSync(records))
			switch damage {
			case "topic":
				records[1].Topic = "foreign"
			case "partition":
				records[1].Partition = 2
			case "order":
				records[1].Offset = 0
			case "hole":
				records = append(records[:1], records[2])
			case "segment-bound":
				records[2].Offset = 3
			}
			var data []byte
			for _, record := range records {
				encoded, err := util.SerializeDiskMessage(record)
				require.NoError(t, err)
				data = binary.BigEndian.AppendUint32(data, uint32(len(encoded)))
				data = append(data, encoded...)
			}
			switch damage {
			case "partial-frame":
				data = data[:len(data)-1]
			case "partial-header":
				data = append(data, 0)
			case "bad-length":
				binary.BigEndian.PutUint32(data[:4], MaxMessageSize+1)
			}
			require.NoError(t, os.WriteFile(d.GetSegmentPath(0), data, 0o600))
			_, err := d.ReadRetainedPage(context.Background(), 0, 3, 10, 1<<20)
			require.Error(t, err)
			_, err = d.RetainedRangeDigestContext(context.Background(), 0, 3)
			require.Error(t, err)
		})
	}
}

func TestRetainedPageStopsAtCertifiedCutoffAndPreservesAppendToken(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	for i := 0; i < 3; i++ {
		_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: "record"})
		require.NoError(t, err)
	}
	p, err := d.ReadRetainedPage(context.Background(), 0, 2, 1, 1<<20)
	require.NoError(t, err)
	require.False(t, p.Exhausted)
	require.Equal(t, uint64(1), p.EndOffset)
	require.Len(t, p.Messages, 1)
	_, err = d.AppendMessageSync("orders", 1, &types.Message{Payload: "new append"})
	require.NoError(t, err)
	require.NoError(t, d.CheckContentGeneration(p.ContentGeneration))
	// A malformed frame after the already certified cutoff must not force a
	// bounded page to inspect the rest of the file. Full scans still reject it.
	f, err := os.OpenFile(d.GetSegmentPath(0), os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.Write([]byte{0})
	require.NoError(t, err)
	require.NoError(t, f.Close())
	p, err = d.ReadRetainedPage(context.Background(), 0, 1, 10, 1<<20)
	require.NoError(t, err)
	require.True(t, p.Exhausted)
	require.Equal(t, uint64(1), p.EndOffset)
	require.Error(t, d.ScanRetainedRange(context.Background(), 0, 1, func(types.Message) error { return nil }))
}

func TestRetainedPageCompactedBoundsAndPendingAvailability(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.WriteCompactedReplicaRange(0, 10, nil))
	p, err := d.ReadRetainedPage(context.Background(), 2, 8, 1, 1)
	require.NoError(t, err)
	require.True(t, p.Exhausted)
	require.True(t, p.Compacted)
	require.Equal(t, uint64(8), p.EndOffset)
	require.Empty(t, p.Messages)
	d.mu.Lock()
	d.bootstrapPending.Store(true)
	d.mu.Unlock()
	_, err = d.ReadRetainedPage(context.Background(), 0, 10, 1, 1)
	require.ErrorIs(t, err, ErrBootstrapPending)
	require.NoError(t, d.ScanRetainedRange(context.Background(), 0, 10, func(types.Message) error { return nil }))
	d.mu.Lock()
	d.bootstrapPending.Store(false)
	d.mu.Unlock()
}

func TestRetainedPageStopsImmediatelyAtExactEnd(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	record := types.DiskMessage{Topic: "orders", Partition: 1, Offset: 0, Payload: "certified"}
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{record, {Topic: "orders", Partition: 1, Offset: 1}}))
	encoded, err := util.SerializeDiskMessage(record)
	require.NoError(t, err)
	// The next out-of-range frame has only a partial header. Reading it is
	// unnecessary once the included frame has certified exactly end-1.
	require.NoError(t, os.Truncate(d.GetSegmentPath(0), int64(4+len(encoded)+1)))
	p, err := d.ReadRetainedPage(context.Background(), 0, 1, 10, 1<<20)
	require.NoError(t, err)
	require.True(t, p.Exhausted)
	require.Equal(t, uint64(1), p.EndOffset)
	require.Len(t, p.Messages, 1)
	require.Error(t, d.ScanRetainedRange(context.Background(), 0, 1, func(types.Message) error { return nil }))
}
