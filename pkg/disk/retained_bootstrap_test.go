package disk

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func retainedExportTest(t *testing.T, d *DiskHandler, anchor, start, end uint64) *BootstrapExport {
	t.Helper()
	token, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	before := d.RetainedBootstrapWorkSnapshot()
	e, err := d.ExportRetainedBootstrapContext(context.Background(), token, anchor, start, end, bootstrapTestFence(1, end))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	require.Nil(t, e.transfer.timer, "private prepared exports do not start ready TTL")
	require.Zero(t, e.Manifest().Files[1].Size)
	require.Equal(t, e.Manifest().Files[0].Size, int64(e.RetainedWork().CanonicalWriteBytes))
	require.Equal(t, e.RetainedWork().CanonicalWriteBytes, e.RetainedWork().HashReadBytes)
	after := d.RetainedBootstrapWorkSnapshot()
	require.Equal(t, e.RetainedWork(), RetainedBootstrapWork{Pages: after.Pages - before.Pages, CanonicalWriteBytes: after.CanonicalWriteBytes - before.CanonicalWriteBytes, HashReadBytes: after.HashReadBytes - before.HashReadBytes})
	return e
}

func TestRetainedBootstrapRawFieldRoundTripAndPages(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	record := types.DiskMessage{AppendTimeMS: 1700000000000, Topic: "orders", Partition: 1, Offset: 0, ProducerID: "p", SeqNum: 8, Epoch: 2, LeaderEpoch: 7, Payload: "body", Key: "key", EventType: "event", SchemaVersion: 3, AggregateVersion: 4, Metadata: `{"k":"v"}`, EventID: "id", PayloadDigest: "digest", TransactionalID: "tx", TransactionState: types.TransactionStateAborted, TransactionMarker: types.TransactionMarkerAbort, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 9, ControlBatchKey: []byte{1, 2}, ControlBatchValue: []byte{3, 4}}
	// Every current disk field must be populated so a newly added field cannot
	// silently escape this round-trip regression check.
	value := reflect.ValueOf(record)
	for i := 0; i < value.NumField(); i++ {
		if value.Type().Field(i).Name != "Offset" {
			require.False(t, value.Field(i).IsZero(), value.Type().Field(i).Name)
		}
	}
	records := []types.DiskMessage{record, {Topic: "orders", Partition: 1, Offset: 1, Payload: "legacy"}}
	for i := 2; i < 2100; i++ {
		r := record
		r.Offset = uint64(i)
		records = append(records, r)
	}
	require.NoError(t, d.WriteBatchSync(records))
	before := d.RetainedPageWorkSnapshot()
	e := retainedExportTest(t, d, 0, 0, uint64(len(records)))
	require.Equal(t, records, bootstrapRawRecords(t, filepath.Join(e.transfer.dir, "records")))
	require.GreaterOrEqual(t, e.RetainedWork().Pages, uint64(3))
	after := d.RetainedPageWorkSnapshot()
	require.GreaterOrEqual(t, after.DecodedFrames-before.DecodedFrames, uint64(len(records)))
	require.Greater(t, after.BytesRead-before.BytesRead, uint64(0))
	m := e.Manifest()
	m.Files[0].Size++
	require.NotEqual(t, m, e.Manifest())
}

func TestRetainedBootstrapMarkedGapsAndSuffix(t *testing.T) {
	for _, offsets := range [][]uint64{{2, 5}, {}, {0, 3}, {3, 9}} {
		t.Run(fmt.Sprint(offsets), func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			var records []types.DiskMessage
			for _, off := range offsets {
				records = append(records, types.DiskMessage{Topic: "orders", Partition: 1, Offset: off, Payload: "raw"})
			}
			require.NoError(t, d.WriteCompactedReplicaRange(0, 10, records))
			e := retainedExportTest(t, d, 0, 0, 10)
			require.True(t, e.Manifest().Compacted)
			require.Equal(t, records, bootstrapRawRecords(t, filepath.Join(e.transfer.dir, "records")))
			e = retainedExportTest(t, d, 0, 6, 10)
			var suffix []types.DiskMessage
			for _, r := range records {
				if r.Offset >= 6 {
					suffix = append(suffix, r)
				}
			}
			require.Equal(t, suffix, bootstrapRawRecords(t, filepath.Join(e.transfer.dir, "records")))
		})
	}
}

func TestRetainedBootstrapEmptyRangeStillChecksSource(t *testing.T) {
	for _, invalid := range []string{"valid", "token", "anchor", "closed", "pending", "nil", "canceled", "identity", "bounds"} {
		t.Run(invalid, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			token, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			ctx := context.Background()
			anchor := uint64(0)
			fence := bootstrapTestFence(1, 0)
			switch invalid {
			case "token":
				token = ContentGeneration{}
			case "anchor":
				anchor = 1
			case "closed":
				require.NoError(t, d.Close())
			case "pending":
				d.bootstrapPending.Store(true)
				defer d.bootstrapPending.Store(false)
			case "nil":
				ctx = nil
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "identity":
				fence.Topic = "foreign"
			case "bounds":
				fence.CommittedHWM = 0
				anchor = 1
			}
			before := d.RetainedPageWorkSnapshot()
			e, err := d.ExportRetainedBootstrapContext(ctx, token, anchor, 0, 0, fence)
			if invalid == "valid" {
				require.NoError(t, err)
				require.NoError(t, e.Close())
				require.Equal(t, uint64(1), e.RetainedWork().Pages)
				require.Greater(t, d.RetainedPageWorkSnapshot().CacheMisses, before.CacheMisses)
			} else {
				require.Error(t, err)
				require.Nil(t, e)
			}
		})
	}
}

func TestRetainedBootstrapOriginalTokenABA(t *testing.T) {
	for _, stage := range []string{"before", "between-pages", "hash"} {
		t.Run(stage, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			records := detachedCacheRecords(0, 1100, 10)
			require.NoError(t, d.WriteBatchSync(records))
			token, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			rewrite := func() { require.NoError(t, d.TruncateTo(0)); require.NoError(t, d.WriteBatchSync(records)) }
			ops := defaultRetainedExportIO()
			fired := false
			if stage == "before" {
				rewrite()
			}
			serialize := ops.serialize
			ops.serialize = func(m types.DiskMessage) ([]byte, error) {
				if stage == "between-pages" && !fired {
					fired = true
					rewrite()
				}
				return serialize(m)
			}
			read := ops.read
			ops.read = func(f *os.File, p []byte) (int, error) {
				if stage == "hash" && !fired {
					fired = true
					rewrite()
				}
				return read(f, p)
			}
			e, err := d.exportRetainedBootstrap(context.Background(), token, 0, 0, 1100, bootstrapTestFence(1, 1100), ops)
			require.ErrorIs(t, err, ErrStaleContentGeneration)
			require.Nil(t, e)
		})
	}
}

func TestRetainedBootstrapDetachedIOAllowsAppendAndRoll(t *testing.T) {
	for _, stage := range []string{"serialize", "write", "hash", "sync"} {
		t.Run(stage, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			records := detachedCacheRecords(0, 2, 100)
			require.NoError(t, d.WriteBatchSync(records))
			token, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			ops := defaultRetainedExportIO()
			entered, release := make(chan struct{}), make(chan struct{})
			fired := false
			block := func() {
				if !fired {
					fired = true
					close(entered)
					<-release
				}
			}
			switch stage {
			case "serialize":
				orig := ops.serialize
				ops.serialize = func(m types.DiskMessage) ([]byte, error) { block(); return orig(m) }
			case "write":
				orig := ops.write
				ops.write = func(f *os.File, p []byte) (int, error) { block(); return orig(f, p) }
			case "hash":
				orig := ops.read
				ops.read = func(f *os.File, p []byte) (int, error) { block(); return orig(f, p) }
			case "sync":
				orig := ops.sync
				ops.sync = func(f *os.File) error { block(); return orig(f) }
			}
			result := make(chan *BootstrapExport, 1)
			errs := make(chan error, 1)
			go func() {
				e, err := d.exportRetainedBootstrap(context.Background(), token, 0, 0, 2, bootstrapTestFence(1, 2), ops)
				result <- e
				errs <- err
			}()
			<-entered
			appendDone := make(chan error, 1)
			go func() {
				_, err := d.AppendMessageSync("orders", 1, &types.Message{Payload: strings.Repeat("x", 2<<20)})
				appendDone <- err
			}()
			select {
			case err := <-appendDone:
				require.NoError(t, err)
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("export IO held storage gate")
			}
			close(release)
			e := <-result
			require.NoError(t, <-errs)
			require.NotNil(t, e)
			require.Equal(t, records, bootstrapRawRecords(t, filepath.Join(e.transfer.dir, "records")))
			require.Equal(t, uint64(2), e.Manifest().EndOffset)
			require.NoError(t, e.Close())
		})
	}
}

func TestRetainedBootstrapFailureCleanupAndDebt(t *testing.T) {
	for _, stage := range []string{"serialize", "short-write", "partial-write", "write", "sync", "hash", "directory", "cancel", "delete"} {
		t.Run(stage, func(t *testing.T) {
			beforeActive, beforeBytes := bootstrapBudgetSnapshot()
			d, _ := bootstrapTestHandler(t, 1)
			require.NoError(t, d.WriteBatchSync(detachedCacheRecords(0, 2, 100)))
			token, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("injected export failure")
			ops := defaultRetainedExportIO()
			switch stage {
			case "serialize":
				ops.serialize = func(types.DiskMessage) ([]byte, error) { return nil, injected }
			case "short-write":
				ops.write = func(f *os.File, data []byte) (int, error) { return f.Write(data[:1]) }
			case "partial-write":
				ops.write = func(f *os.File, data []byte) (int, error) { n, _ := f.Write(data[:2]); return n, injected }
			case "write":
				ops.write = func(*os.File, []byte) (int, error) { return 0, injected }
			case "sync":
				ops.sync = func(*os.File) error { return injected }
			case "hash":
				ops.read = func(f *os.File, data []byte) (int, error) { n, _ := f.Read(data[:7]); return n, injected }
			case "directory":
				ops.syncDirectory = func(string) error { return injected }
			case "cancel":
				orig := ops.serialize
				ops.serialize = func(m types.DiskMessage) ([]byte, error) { cancel(); return orig(m) }
			case "delete":
				ops.read = func(*os.File, []byte) (int, error) { return 0, injected }
				ops.removeAll = func(string) error { return injected }
			}
			e, err := d.exportRetainedBootstrap(ctx, token, 0, 0, 2, bootstrapTestFence(1, 2), ops)
			require.Error(t, err)
			require.Nil(t, e)
			work := d.RetainedBootstrapWorkSnapshot()
			require.Equal(t, uint64(1), work.Pages)
			switch stage {
			case "serialize", "write", "cancel":
				require.Zero(t, work.CanonicalWriteBytes)
			case "short-write":
				require.Equal(t, uint64(1), work.CanonicalWriteBytes)
			case "partial-write":
				require.Equal(t, uint64(2), work.CanonicalWriteBytes)
			default:
				require.Greater(t, work.CanonicalWriteBytes, uint64(0))
			}
			if stage == "hash" {
				require.Equal(t, uint64(7), work.HashReadBytes)
			}
			if stage == "directory" {
				require.Equal(t, work.CanonicalWriteBytes, work.HashReadBytes)
			}
			if stage == "short-write" {
				require.ErrorIs(t, err, io.ErrShortWrite)
			}
			active, bytes := bootstrapBudgetSnapshot()
			if stage == "delete" {
				require.Equal(t, beforeActive+1, active)
				require.Greater(t, bytes, beforeBytes)
				bootstrapCleanup.Lock()
				var debt *bootstrapTransfer
				for _, p := range bootstrapCleanup.pending {
					if strings.HasPrefix(p.dir, d.BaseName+".bootstrap-") {
						debt = p
					}
				}
				bootstrapCleanup.Unlock()
				require.NotNil(t, debt)
				require.True(t, debt.activeToken.owned)
				debt.mu.Lock()
				debt.removeAll = os.RemoveAll
				debt.mu.Unlock()
				retryBootstrapCleanup()
				active, bytes = bootstrapBudgetSnapshot()
			}
			require.Equal(t, beforeActive, active)
			require.Equal(t, beforeBytes, bytes)
		})
	}
}

func TestRetainedBootstrapOversizeRetryAndHeaderReservation(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	record := detachedCacheRecords(0, 1, 2<<20)[0]
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{record}))
	before := d.RetainedPageWorkSnapshot()
	e := retainedExportTest(t, d, 0, 0, 1)
	after := d.RetainedPageWorkSnapshot()
	require.Equal(t, uint64(2), e.RetainedWork().Pages)
	require.GreaterOrEqual(t, after.DecodedFrames-before.DecodedFrames, uint64(2))
	require.Greater(t, after.BytesRead-before.BytesRead, e.RetainedWork().CanonicalWriteBytes)
	encoded, err := util.SerializeDiskMessage(record)
	require.NoError(t, err)
	require.Equal(t, int64(len(encoded)+4), e.transfer.byteOwner.reserved)
	require.NoError(t, e.Close())
	require.NoError(t, d.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: int64(len(encoded)), MaxTotalBytes: int64(len(encoded) + 4)}))
	token, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	e, err = d.ExportRetainedBootstrapContext(context.Background(), token, 0, 0, 1, bootstrapTestFence(1, 1))
	var limit *BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	require.Nil(t, e)
}

func TestRetainedBootstrapActiveAdmissionBeforePage(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.WriteBatchSync(detachedCacheRecords(0, 1, 100)))
	token, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	for i := 0; i < BootstrapMaxActive; i++ {
		transfer, err := newBootstrapTransfer(d.BaseName, BootstrapMetadata{}, 0, d.BootstrapLimits())
		require.NoError(t, err)
		defer func() { require.NoError(t, transfer.close()) }()
	}
	before := d.RetainedPageWorkSnapshot()
	e, err := d.ExportRetainedBootstrapContext(context.Background(), token, 0, 0, 1, bootstrapTestFence(1, 1))
	var limit *BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "active transfers", limit.Resource)
	require.Nil(t, e)
	require.Equal(t, before, d.RetainedPageWorkSnapshot())
}

func TestRetainedBootstrapWarmAndColdSuffixWork(t *testing.T) {
	for _, count := range []int{8, 512} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			d.SegmentSize, d.IndexSize = 1<<30, 1<<30
			records := detachedCacheRecords(0, count, 1000)
			require.NoError(t, d.WriteBatchSync(records))
			prefix := retainedExportTest(t, d, 0, 0, uint64(count))
			require.NoError(t, prefix.Close())
			suffix := detachedCacheRecords(uint64(count), 2, 1000)
			require.NoError(t, d.WriteBatchSync(suffix))
			before := d.RetainedPageWorkSnapshot()
			warm := retainedExportTest(t, d, 0, uint64(count), uint64(count+2))
			after := d.RetainedPageWorkSnapshot()
			warmBytes, warmFrames := after.BytesRead-before.BytesRead, after.DecodedFrames-before.DecodedFrames
			require.LessOrEqual(t, warmFrames, uint64(3))
			require.Less(t, warmBytes, uint64(8192))
			require.LessOrEqual(t, after.SegmentTransitions-before.SegmentTransitions, uint64(1))
			require.NoError(t, warm.Close())
			// Eviction/restart removes evidence, so a suffix may validate its segment prefix again.
			d.retainedPageMu.Lock()
			d.retainedCursors = nil
			d.retainedPageMu.Unlock()
			before = d.RetainedPageWorkSnapshot()
			cold := retainedExportTest(t, d, 0, uint64(count), uint64(count+2))
			after = d.RetainedPageWorkSnapshot()
			require.Greater(t, after.BytesRead-before.BytesRead, warmBytes)
			require.GreaterOrEqual(t, after.DecodedFrames-before.DecodedFrames, uint64(count+2))
			require.Equal(t, warm.RetainedWork().CanonicalWriteBytes, cold.RetainedWork().CanonicalWriteBytes)
			require.Equal(t, warm.RetainedWork().HashReadBytes, cold.RetainedWork().HashReadBytes)
			t.Logf("prefix=%d warm bytes=%d frames=%d cold bytes=%d frames=%d transitions=%d canonical=%d hash=%d", count, warmBytes, warmFrames, after.BytesRead-before.BytesRead, after.DecodedFrames-before.DecodedFrames, after.SegmentTransitions-before.SegmentTransitions, cold.RetainedWork().CanonicalWriteBytes, cold.RetainedWork().HashReadBytes)
		})
	}
}

func TestRetainedBootstrapMixedSourceCacheIntegration(t *testing.T) {
	for _, prefixMarked := range []bool{false, true} {
		t.Run(fmt.Sprint(prefixMarked), func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			records := detachedCacheRecords(0, 2, 100)
			if prefixMarked {
				require.NoError(t, d.WriteCompactedReplicaRange(0, 2, records))
			} else {
				require.NoError(t, d.WriteBatchSync(records))
			}
			token, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			root := detachedTestRoot(t)
			prepare := func(e *BootstrapExport) *DetachedRawStore {
				raw, err := os.ReadFile(filepath.Join(e.transfer.dir, "records"))
				require.NoError(t, err)
				prepared, err := detachedTestStage(t, root, e.Manifest(), raw).Prepare(context.Background())
				require.NoError(t, err)
				require.NoError(t, e.Close())
				return prepared
			}
			prefix := retainedExportTest(t, d, 0, 0, 2)
			require.Equal(t, prefixMarked, prefix.Manifest().Compacted)
			prepared := prepare(prefix)
			original := cloneBootstrapMetadata(prepared.transfer.metadata)
			adopted, err := root.Adopt(prepared)
			require.NoError(t, err)
			old, err := adopted.Bounds()
			require.NoError(t, err)
			suffix := detachedCacheRecords(2, 2, 100)
			if prefixMarked {
				require.NoError(t, d.WriteBatchSync(suffix))
			} else {
				require.NoError(t, d.WriteCompactedReplicaRange(2, 4, suffix))
			}
			require.NoError(t, d.CheckContentGeneration(token))
			e, err := d.ExportRetainedBootstrapContext(context.Background(), token, 0, 2, 4, bootstrapTestFence(1, 4))
			require.NoError(t, err)
			require.Equal(t, !prefixMarked, e.Manifest().Compacted)
			next := prepare(e)
			result, err := adopted.AppendPrepared(context.Background(), old, next)
			require.NoError(t, err)
			require.Equal(t, original, prepared.transfer.metadata)
			require.Equal(t, uint64(next.transfer.metadata.Files[0].Size), result.Work.RawReadBytes)
			lease, err := adopted.Acquire()
			require.NoError(t, err)
			page, err := lease.ReadRaw(context.Background(), 0, 10, 1<<20)
			require.NoError(t, err)
			require.Equal(t, append(records, suffix...), page.Messages)
			require.NoError(t, lease.Close())
		})
	}
}

func TestRetainedBootstrapGrowthAdmissionBeforeWrites(t *testing.T) {
	for _, failure := range []string{"aggregate", "headroom"} {
		t.Run(failure, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			records := detachedCacheRecords(0, 1, 100)
			require.NoError(t, d.WriteBatchSync(records))
			token, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			encoded, err := util.SerializeDiskMessage(records[0])
			require.NoError(t, err)
			size := int64(len(encoded) + 4)
			if failure == "aggregate" {
				require.NoError(t, d.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: size, MaxTotalBytes: size}))
				owner, err := newBootstrapTransfer(d.BaseName, BootstrapMetadata{}, 1, d.BootstrapLimits())
				require.NoError(t, err)
				defer func() { require.NoError(t, owner.close()) }()
			}
			ops := defaultRetainedExportIO()
			written := 0
			serialize := ops.serialize
			ops.serialize = func(m types.DiskMessage) ([]byte, error) {
				if failure == "headroom" {
					d.headroom.mu.Lock()
					d.headroom.checkedAt = time.Now()
					d.headroom.free = 1
					d.headroom.reserved = 0
					d.headroom.minFreeBytes = 1
					d.headroom.mu.Unlock()
				}
				return serialize(m)
			}
			ops.write = func(f *os.File, p []byte) (int, error) { written++; return f.Write(p) }
			e, err := d.exportRetainedBootstrap(context.Background(), token, 0, 0, 1, bootstrapTestFence(1, 1), ops)
			require.Error(t, err)
			require.Nil(t, e)
			require.Zero(t, written)
			if failure == "aggregate" {
				var limit *BootstrapLimitError
				require.ErrorAs(t, err, &limit)
				require.Equal(t, "total staging bytes", limit.Resource)
			}
		})
	}
}

func TestRetainedBootstrapHeadroomDoesNotHoldBudgetMutex(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, d.WriteBatchSync(detachedCacheRecords(0, 1, 100)))
	token, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	ops := defaultRetainedExportIO()
	serialize := ops.serialize
	entered := make(chan struct{})
	ops.serialize = func(m types.DiskMessage) ([]byte, error) { d.headroom.mu.Lock(); close(entered); return serialize(m) }
	result := make(chan *BootstrapExport, 1)
	errs := make(chan error, 1)
	go func() {
		e, err := d.exportRetainedBootstrap(context.Background(), token, 0, 0, 1, bootstrapTestFence(1, 1), ops)
		result <- e
		errs <- err
	}()
	<-entered
	// The growth call can block on this headroom guard, never the global budget.
	acquired := make(chan int, 1)
	go func() { active, _ := bootstrapBudgetSnapshot(); acquired <- active }()
	select {
	case active := <-acquired:
		require.GreaterOrEqual(t, active, 1)
	case <-time.After(time.Second):
		d.headroom.mu.Unlock()
		t.Fatal("filesystem headroom held global budget")
	}
	d.headroom.mu.Unlock()
	e := <-result
	require.NoError(t, <-errs)
	require.NoError(t, e.Close())
}

func TestRetainedBootstrapCancellationDuringHashAndAfterSync(t *testing.T) {
	for _, stage := range []string{"hash", "directory"} {
		t.Run(stage, func(t *testing.T) {
			d, _ := bootstrapTestHandler(t, 1)
			require.NoError(t, d.WriteBatchSync(detachedCacheRecords(0, 1, 2*BootstrapChunkBytes)))
			token, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ops := defaultRetainedExportIO()
			reads := 0
			if stage == "hash" {
				read := ops.read
				ops.read = func(f *os.File, p []byte) (int, error) { reads++; n, err := read(f, p); cancel(); return n, err }
			} else {
				sync := ops.syncDirectory
				ops.syncDirectory = func(path string) error { err := sync(path); cancel(); return err }
			}
			e, err := d.exportRetainedBootstrap(ctx, token, 0, 0, 1, bootstrapTestFence(1, 1), ops)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, e)
			if stage == "hash" {
				require.Equal(t, 1, reads)
				require.Equal(t, uint64(BootstrapChunkBytes), d.RetainedBootstrapWorkSnapshot().HashReadBytes)
			}
			retry := retainedExportTest(t, d, 0, 0, 1)
			require.NoError(t, retry.Close())
		})
	}
}

func TestRetainedBootstrapLegacyExpansionReservesCanonicalBytes(t *testing.T) {
	d, cfg := bootstrapTestHandler(t, 1)
	require.NoError(t, d.Close())
	record := types.DiskMessage{Topic: "orders", Partition: 1, Payload: "legacy"}
	canonical, err := util.SerializeDiskMessage(record)
	require.NoError(t, err)
	leaderEpochOffset := 4 + 2 + len("orders") + 4 + 8 + 2 + 8 + 8
	old := append([]byte("CDM2"), canonical[4:leaderEpochOffset]...)
	old = append(old, canonical[leaderEpochOffset+8:len(canonical)-8]...)
	raw := binary.BigEndian.AppendUint32(nil, uint32(len(old)))
	raw = append(raw, old...)
	require.NoError(t, os.WriteFile(d.GetSegmentPath(0), raw, 0o600))
	d, err = NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	token, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	require.NoError(t, d.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: int64(len(raw)), MaxTotalBytes: int64(len(canonical) + 4)}))
	e, err := d.ExportRetainedBootstrapContext(context.Background(), token, 0, 0, 1, bootstrapTestFence(1, 1))
	require.Error(t, err)
	require.Nil(t, e)
	require.NoError(t, d.SetBootstrapLimits(DefaultBootstrapLimits()))
	e = retainedExportTest(t, d, 0, 0, 1)
	require.Greater(t, e.Manifest().Files[0].Size, int64(len(raw)))
	require.Equal(t, int64(len(canonical)+4), e.transfer.byteOwner.reserved)
	require.Equal(t, []types.DiskMessage{record}, bootstrapRawRecords(t, filepath.Join(e.transfer.dir, "records")))
}
