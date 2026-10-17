package disk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func detachedPreparedRange(t *testing.T, root *DetachedBootstrapRoot, start, end uint64, records []types.DiskMessage, compacted bool) *DetachedRawStore {
	t.Helper()
	m, raw := detachedTestMetadata(t, records, end, compacted)
	m.StartOffset = start
	store, err := detachedTestStage(t, root, m, raw).Prepare(context.Background())
	require.NoError(t, err)
	return store
}

func detachedCacheRecords(start uint64, count, payload int) []types.DiskMessage {
	records := make([]types.DiskMessage, count)
	for i := range records {
		records[i] = types.DiskMessage{Topic: "orders", Partition: 1, Offset: start + uint64(i), Payload: strings.Repeat("x", payload), LeaderEpoch: 3, TransactionalID: "txn", TransactionState: types.TransactionStateCommitted}
	}
	return records
}

func TestDetachedAdoptedSlotRetainsPinnedZeroAndFailedCleanup(t *testing.T) {
	root, err := NewDetachedBootstrapRoot(t.TempDir(), DefaultBootstrapLimits(), 0, 0, 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	prepared := detachedPreparedRange(t, root, 0, 1, nil, true)
	adopted, err := root.Adopt(prepared)
	require.NoError(t, err)
	lease, err := adopted.Acquire()
	require.NoError(t, err)
	injected := errors.New("delete denied")
	prepared.transfer.removeAll = func(string) error { return injected }
	t.Cleanup(func() { prepared.transfer.removeAll = os.RemoveAll; _ = lease.Close(); _ = adopted.Close() })
	require.ErrorIs(t, adopted.Close(), ErrDetachedBootstrapPending)
	extra := detachedPreparedRange(t, root, 1, 2, nil, true)
	_, err = root.Adopt(extra)
	require.Error(t, err)
	require.ErrorIs(t, lease.Close(), injected)
	require.False(t, bootstrapCleanupContains(prepared.transfer))
	_, err = root.Adopt(extra)
	require.Error(t, err, "failed cleanup continues occupying the zero-byte slot")
	require.Len(t, root.adopted, 1)
	prepared.transfer.removeAll = os.RemoveAll
	_, err = root.Adopt(extra)
	require.NoError(t, err, "admission retries one retired adopted deletion")
	require.NoDirExists(t, prepared.transfer.dir)
}

func TestDetachedAppendRollbackRestoresBothFilesAndReservation(t *testing.T) {
	for _, phase := range []string{"raw-write", "index-write", "raw-sync", "index-sync", "publication", "cancellation"} {
		t.Run(phase, func(t *testing.T) {
			root := detachedTestRoot(t)
			prepared := detachedPreparedRange(t, root, 0, 2, detachedCacheRecords(0, 2, 5000), false)
			adopted, err := root.Adopt(prepared)
			require.NoError(t, err)
			old, err := adopted.Bounds()
			require.NoError(t, err)
			oldReservation := prepared.transfer.byteOwner.reserved
			suffix := detachedPreparedRange(t, root, 2, 4, detachedCacheRecords(2, 2, 5000), false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("append fault")
			ops := defaultDetachedAppendIO()
			once := false
			root.appendIO.writeAt = func(f *os.File, p []byte, offset int64) (int, error) {
				if !once && ((phase == "raw-write" && filepath.Base(f.Name()) == "records") || (phase == "index-write" && filepath.Base(f.Name()) == "range-index")) {
					once = true
					n, err := f.WriteAt(p[:len(p)/2], offset)
					return n, errors.Join(err, injected)
				}
				return ops.writeAt(f, p, offset)
			}
			root.appendIO.sync = func(f *os.File) error {
				if !once && ((phase == "raw-sync" && filepath.Base(f.Name()) == "records") || (phase == "index-sync" && filepath.Base(f.Name()) == "range-index")) {
					once = true
					return injected
				}
				return ops.sync(f)
			}
			root.appendIO.beforePublish = func() error {
				if phase == "cancellation" {
					cancel()
				}
				if phase == "publication" {
					return injected
				}
				return nil
			}
			result, err := adopted.AppendPrepared(ctx, old, suffix)
			require.Error(t, err)
			require.Equal(t, uint64(suffix.transfer.metadata.Files[0].Size), result.Work.RawReadBytes)
			if phase == "raw-write" {
				require.Greater(t, result.Work.RawReadBytes, result.Work.RawBytes)
			}
			require.NoError(t, result.CleanupError)
			require.Nil(t, prepared.transfer.detached.tailDebt)
			require.Equal(t, oldReservation, prepared.transfer.byteOwner.reserved)
			actual, err := adopted.Bounds()
			require.NoError(t, err)
			require.Equal(t, old, actual)
			for name, size := range map[string]int64{"records": old.RawBytes, "range-index": old.IndexBytes} {
				info, err := os.Stat(filepath.Join(prepared.transfer.dir, name))
				require.NoError(t, err)
				require.Equal(t, size, info.Size())
			}
			require.NoDirExists(t, suffix.transfer.dir)
			lease, err := adopted.Acquire()
			require.NoError(t, err)
			page, err := lease.ReadRaw(context.Background(), 0, 10, 1<<20)
			require.NoError(t, err)
			require.Equal(t, detachedCacheRecords(0, 2, 5000), page.Messages)
			require.NoError(t, lease.Close())
		})
	}
}

func TestDetachedAdoptedCleanupRotatesPastPermanentFailure(t *testing.T) {
	root := detachedTestRoot(t)
	injected := errors.New("delete denied")
	var stores []*AdoptedRawStore
	for i := 0; i < 3; i++ {
		prepared := detachedPreparedRange(t, root, 0, 1, nil, true)
		adopted, err := root.Adopt(prepared)
		require.NoError(t, err)
		prepared.transfer.removeAll = func(string) error { return injected }
		stores = append(stores, adopted)
	}
	t.Cleanup(func() {
		for _, store := range stores {
			store.transfer.removeAll = os.RemoveAll
			_ = store.Close()
		}
	})
	for _, store := range stores {
		require.ErrorIs(t, store.Close(), injected)
	}
	stores[1].transfer.removeAll = os.RemoveAll
	stores[2].transfer.removeAll = os.RemoveAll
	root.retryAdoptedCleanup()
	require.Len(t, root.adopted, 3, "first attempt hits the permanently failed oldest entry")
	root.retryAdoptedCleanup()
	require.Len(t, root.adopted, 2)
	require.NoDirExists(t, stores[1].transfer.dir)
	root.retryAdoptedCleanup()
	require.Len(t, root.adopted, 1)
	require.NoDirExists(t, stores[2].transfer.dir)
	require.DirExists(t, stores[0].transfer.dir)
}

func TestDetachedAppendReservesAggregatePhysicalPeakBeforeWrites(t *testing.T) {
	root := detachedTestRoot(t)
	prepared := detachedPreparedRange(t, root, 0, 1, detachedCacheRecords(0, 1, 100), false)
	adopted, err := root.Adopt(prepared)
	require.NoError(t, err)
	old, err := adopted.Bounds()
	require.NoError(t, err)
	suffix := detachedPreparedRange(t, root, 1, 2, detachedCacheRecords(1, 1, 100), false)
	_, charged := bootstrapBudgetSnapshot()
	growth := suffix.transfer.detached.extent.RawBytes
	prepared.transfer.byteOwner.limits.MaxTotalBytes = charged + growth - 1
	_, err = adopted.AppendPrepared(context.Background(), old, suffix)
	var limit *BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "total staging bytes", limit.Resource)
	require.False(t, suffix.transfer.detached.retired)
	prepared.transfer.byteOwner.limits.MaxTotalBytes++
	ops := root.appendIO
	root.appendIO.writeAt = func(f *os.File, p []byte, off int64) (int, error) {
		_, bytes := bootstrapBudgetSnapshot()
		require.Equal(t, charged+growth, bytes, "peak includes source, destination prefix, and new destination bytes before writes")
		return ops.writeAt(f, p, off)
	}
	_, err = adopted.AppendPrepared(context.Background(), old, suffix)
	require.NoError(t, err)
}

func TestDetachedAppendPartialSourceReadFailureRollsBackAndCountsWork(t *testing.T) {
	// Truncation deliberately simulates a partial source-read failure after
	// preparation. This exercises I/O failure handling, not a tampering guarantee.
	for _, failed := range []string{"records", "range-index"} {
		t.Run(failed, func(t *testing.T) {
			root := detachedTestRoot(t)
			prepared := detachedPreparedRange(t, root, 0, 2, detachedCacheRecords(0, 2, 5000), false)
			adopted, err := root.Adopt(prepared)
			require.NoError(t, err)
			old, err := adopted.Bounds()
			require.NoError(t, err)
			reserved := prepared.transfer.byteOwner.reserved
			beforeActive, beforeBytes := bootstrapBudgetSnapshot()
			suffix := detachedPreparedRange(t, root, 2, 4, detachedCacheRecords(2, 2, BootstrapChunkBytes), false)
			source := suffix.transfer.detached.extent
			remaining := source.RawBytes - 7
			if failed == "range-index" {
				remaining = 8
			}
			require.NoError(t, os.Truncate(filepath.Join(suffix.transfer.dir, failed), remaining))
			result, err := adopted.AppendPrepared(context.Background(), old, suffix)
			require.Error(t, err)
			require.NoError(t, result.CleanupError)
			if failed == "records" {
				require.Equal(t, uint64(remaining), result.Work.RawReadBytes)
				require.Equal(t, uint64(remaining/BootstrapChunkBytes*BootstrapChunkBytes), result.Work.RawBytes)
				require.Greater(t, result.Work.RawReadBytes, result.Work.RawBytes)
				require.Zero(t, result.Work.IndexReadBytes)
			} else {
				require.Equal(t, uint64(source.RawBytes), result.Work.RawReadBytes)
				require.Equal(t, uint64(source.RawBytes), result.Work.RawBytes)
				require.Equal(t, uint64(8), result.Work.IndexReadBytes)
				require.Equal(t, uint64(16), result.Work.IndexBytes, "boundary anchor was written before the failing source-index read")
			}
			bounds, err := adopted.Bounds()
			require.NoError(t, err)
			require.Equal(t, old, bounds)
			for name, size := range map[string]int64{"records": old.RawBytes, "range-index": old.IndexBytes} {
				info, err := os.Stat(filepath.Join(prepared.transfer.dir, name))
				require.NoError(t, err)
				require.Equal(t, size, info.Size())
			}
			require.Nil(t, prepared.transfer.detached.tailDebt)
			require.Equal(t, reserved, prepared.transfer.byteOwner.reserved)
			require.True(t, suffix.transfer.detached.retired)
			require.NoDirExists(t, suffix.transfer.dir)
			active, bytes := bootstrapBudgetSnapshot()
			require.Equal(t, beforeActive, active)
			require.Equal(t, beforeBytes, bytes)
			lease, err := adopted.Acquire()
			require.NoError(t, err)
			page, err := lease.ReadRaw(context.Background(), 0, 10, 1<<20)
			require.NoError(t, err)
			require.Equal(t, detachedCacheRecords(0, 2, 5000), page.Messages)
			require.NoError(t, lease.Close())
		})
	}
}

func TestDetachedAppendOpenFailuresLeaveSuffixOwnershipAndAccountingIntact(t *testing.T) {
	for _, owner := range []string{"destination", "source"} {
		for _, name := range []string{"records", "range-index"} {
			t.Run(owner+"/"+name, func(t *testing.T) {
				root := detachedTestRoot(t)
				prepared := detachedPreparedRange(t, root, 0, 1, detachedCacheRecords(0, 1, 100), false)
				adopted, err := root.Adopt(prepared)
				require.NoError(t, err)
				old, err := adopted.Bounds()
				require.NoError(t, err)
				suffix := detachedPreparedRange(t, root, 1, 2, detachedCacheRecords(1, 1, 100), false)
				beforeActive, beforeBytes := bootstrapBudgetSnapshot()
				dir := prepared.transfer.dir
				if owner == "source" {
					dir = suffix.transfer.dir
				}
				path := filepath.Join(dir, name)
				// Temporarily hiding a file injects an open failure without changing
				// the production ownership or I/O implementation.
				require.NoError(t, os.Rename(path, path+".saved"))
				t.Cleanup(func() {
					if _, err := os.Stat(path + ".saved"); err == nil {
						require.NoError(t, os.Rename(path+".saved", path))
					}
				})
				result, err := adopted.AppendPrepared(context.Background(), old, suffix)
				require.Error(t, err)
				require.NoError(t, os.Rename(path+".saved", path))
				require.Zero(t, result.Work)
				require.False(t, suffix.transfer.detached.retired)
				require.True(t, suffix.transfer.activeToken.owned)
				require.Nil(t, prepared.transfer.detached.tailDebt)
				active, bytes := bootstrapBudgetSnapshot()
				require.Equal(t, beforeActive, active)
				require.Equal(t, beforeBytes, bytes)
				bounds, err := adopted.Bounds()
				require.NoError(t, err)
				require.Equal(t, old, bounds)
				lease, err := suffix.Acquire()
				require.NoError(t, err)
				require.NoError(t, lease.Close())
				_, err = adopted.AppendPrepared(context.Background(), old, suffix)
				require.NoError(t, err, "restored files permit retry with the same prepared owner")
			})
		}
	}
}

func TestDetachedAppendTailDebtRetainsChargeUntilBothFilesRepaired(t *testing.T) {
	for _, failed := range []string{"records", "range-index", "sync"} {
		t.Run(failed, func(t *testing.T) {
			root := detachedTestRoot(t)
			prepared := detachedPreparedRange(t, root, 0, 2, detachedCacheRecords(0, 2, 5000), false)
			adopted, err := root.Adopt(prepared)
			require.NoError(t, err)
			old, err := adopted.Bounds()
			require.NoError(t, err)
			reserved := prepared.transfer.byteOwner.reserved
			suffix := detachedPreparedRange(t, root, 2, 4, detachedCacheRecords(2, 2, 5000), false)
			injected := errors.New("rollback denied")
			attempts := map[string]int{}
			root.appendIO.beforePublish = func() error { return injected }
			root.appendIO.truncate = func(f *os.File, size int64) error {
				name := filepath.Base(f.Name())
				attempts[name]++
				if name == failed {
					return injected
				}
				return f.Truncate(size)
			}
			root.appendIO.sync = func(f *os.File) error {
				if failed == "sync" {
					return injected
				}
				return f.Sync()
			}
			_, err = adopted.AppendPrepared(context.Background(), old, suffix)
			require.ErrorIs(t, err, injected)
			require.NotNil(t, prepared.transfer.detached.tailDebt)
			require.Greater(t, prepared.transfer.byteOwner.reserved, reserved)
			require.Equal(t, map[string]int{"records": 1, "range-index": 1}, attempts)
			charged := prepared.transfer.byteOwner.reserved
			lease, err := adopted.Acquire()
			require.NoError(t, err)
			require.Equal(t, old, lease.Bounds())
			page, err := lease.ReadRaw(context.Background(), 0, 10, 1<<20)
			require.NoError(t, err)
			require.Equal(t, detachedCacheRecords(0, 2, 5000), page.Messages)
			require.NoError(t, lease.Close())
			retry := detachedPreparedRange(t, root, 2, 4, detachedCacheRecords(2, 2, 5000), false)
			_, err = adopted.AppendPrepared(context.Background(), old, retry)
			require.ErrorIs(t, err, injected)
			require.Equal(t, charged, prepared.transfer.byteOwner.reserved)
			require.False(t, retry.transfer.detached.retired, "failed repair cannot consume next suffix")
			root.appendIO = defaultDetachedAppendIO()
			result, err := adopted.AppendPrepared(context.Background(), old, retry)
			require.NoError(t, err)
			require.Nil(t, prepared.transfer.detached.tailDebt)
			require.Equal(t, reserved+result.Bounds.RawBytes-old.RawBytes+result.Bounds.IndexBytes-old.IndexBytes, prepared.transfer.byteOwner.reserved)
		})
	}
}

func TestDetachedAppendCommittedSuffixCleanupDebtIsNotAppendFailure(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	root := detachedTestRoot(t)
	prepared := detachedPreparedRange(t, root, 0, 1, nil, true)
	adopted, err := root.Adopt(prepared)
	require.NoError(t, err)
	old, err := adopted.Bounds()
	require.NoError(t, err)
	suffix := detachedPreparedRange(t, root, 1, 2, detachedCacheRecords(1, 1, 100), true)
	injected := errors.New("suffix delete denied")
	suffix.transfer.removeAll = func(string) error { return injected }
	t.Cleanup(func() { suffix.transfer.removeAll = os.RemoveAll; _ = suffix.Close() })
	result, err := adopted.AppendPrepared(context.Background(), old, suffix)
	require.NoError(t, err)
	require.ErrorIs(t, result.CleanupError, injected)
	require.Equal(t, uint64(2), result.Bounds.EndOffset)
	require.True(t, bootstrapCleanupContains(suffix.transfer))
	require.False(t, bootstrapCleanupContains(prepared.transfer))
	active, bytes := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+1, active)
	require.Equal(t, beforeBytes+prepared.transfer.byteOwner.reserved+suffix.transfer.byteOwner.reserved, bytes)
	_, err = adopted.AppendPrepared(context.Background(), old, suffix)
	require.Error(t, err)
	suffix.transfer.removeAll = os.RemoveAll
	require.NoError(t, suffix.Close())
	require.NoError(t, suffix.Close())
	active, bytes = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes+prepared.transfer.byteOwner.reserved, bytes)
}

func TestDetachedAppendPreflightPreservesSuffixOwnership(t *testing.T) {
	for _, invalid := range []string{"start", "end", "raw", "index", "topic", "partition", "gap", "overlap", "empty", "root", "leased", "adopted", "self", "canceled", "limit"} {
		t.Run(invalid, func(t *testing.T) {
			root := detachedTestRoot(t)
			prepared := detachedPreparedRange(t, root, 0, 1, nil, true)
			adopted, err := root.Adopt(prepared)
			require.NoError(t, err)
			expected, err := adopted.Bounds()
			require.NoError(t, err)
			sourceRoot := root
			if invalid == "root" {
				sourceRoot = detachedTestRoot(t)
			}
			suffix := detachedPreparedRange(t, sourceRoot, 1, 2, nil, true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch invalid {
			case "start":
				expected.StartOffset++
			case "end":
				expected.EndOffset++
			case "raw":
				expected.RawBytes++
			case "index":
				expected.IndexBytes++
			case "topic":
				suffix.transfer.metadata.Fence.Topic = "other"
			case "partition":
				suffix.transfer.metadata.Fence.Partition++
			case "gap":
				suffix.transfer.detached.extent.StartOffset++
			case "overlap":
				suffix.transfer.detached.extent.StartOffset--
			case "empty":
				suffix.transfer.detached.extent.EndOffset = 1
			case "leased":
				lease, err := suffix.Acquire()
				require.NoError(t, err)
				defer func() { require.NoError(t, lease.Close()) }()
			case "adopted":
				_, err = root.Adopt(suffix)
				require.NoError(t, err)
			case "self":
				require.NoError(t, suffix.Close())
				suffix = prepared
			case "canceled":
				cancel()
			case "limit":
				require.NoError(t, suffix.Close())
				suffix = detachedPreparedRange(t, root, 1, 2, detachedCacheRecords(1, 1, 100), true)
				prepared.transfer.byteOwner.limits.MaxTransferBytes = 1
			}
			_, err = adopted.AppendPrepared(ctx, expected, suffix)
			require.Error(t, err)
			require.False(t, suffix.transfer.detached.retired)
			require.DirExists(t, suffix.transfer.dir)
		})
	}
}

func TestDetachedAppendEmptyCompactedExtensionAndConstantSuffixWork(t *testing.T) {
	for _, count := range []int{1, 200} {
		root := detachedTestRoot(t)
		prepared := detachedPreparedRange(t, root, 0, uint64(count), detachedCacheRecords(0, count, 5000), true)
		adopted, err := root.Adopt(prepared)
		require.NoError(t, err)
		old, err := adopted.Bounds()
		require.NoError(t, err)
		reservation := prepared.transfer.byteOwner.reserved
		empty := detachedPreparedRange(t, root, old.EndOffset, old.EndOffset+10, nil, true)
		result, err := adopted.AppendPrepared(context.Background(), old, empty)
		require.NoError(t, err)
		require.Zero(t, result.Work)
		require.Equal(t, old.RawBytes, result.Bounds.RawBytes)
		require.Equal(t, old.IndexBytes, result.Bounds.IndexBytes)
		require.Equal(t, reservation, prepared.transfer.byteOwner.reserved)
		old = result.Bounds
		suffix := detachedPreparedRange(t, root, old.EndOffset, old.EndOffset+2, detachedCacheRecords(old.EndOffset, 2, 5000), true)
		raw := suffix.transfer.detached.extent.RawBytes
		index := suffix.transfer.detached.extent.IndexBytes
		result, err = adopted.AppendPrepared(context.Background(), old, suffix)
		require.NoError(t, err)
		require.Equal(t, DetachedAppendWork{RawBytes: uint64(raw), IndexBytes: uint64(index + 16), RawReadBytes: uint64(raw), IndexReadBytes: uint64(index)}, result.Work)
		require.Equal(t, reservation+raw+index+16, prepared.transfer.byteOwner.reserved)
		lease, err := adopted.Acquire()
		require.NoError(t, err)
		page, err := lease.ReadRaw(context.Background(), old.EndOffset, 10, 1<<20)
		require.NoError(t, err)
		require.Equal(t, detachedCacheRecords(old.EndOffset, 2, 5000), page.Messages)
		require.NoError(t, lease.Close())
	}
}

func TestDetachedAppendManySmallSuffixesKeepSparseSeekBounded(t *testing.T) {
	root := detachedTestRoot(t)
	prepared := detachedPreparedRange(t, root, 0, 0, nil, false)
	adopted, err := root.Adopt(prepared)
	require.NoError(t, err)
	for offset := uint64(0); offset < 80; offset++ {
		bounds, err := adopted.Bounds()
		require.NoError(t, err)
		suffix := detachedPreparedRange(t, root, offset, offset+1, detachedCacheRecords(offset, 1, 400), false)
		_, err = adopted.AppendPrepared(context.Background(), bounds, suffix)
		require.NoError(t, err)
	}
	lease, err := adopted.Acquire()
	require.NoError(t, err)
	page, err := lease.ReadRaw(context.Background(), 75, 1, 1<<20)
	require.NoError(t, err)
	require.Equal(t, uint64(75), page.Messages[0].Offset)
	require.Less(t, lease.Work().ReadBytes, uint64(8192))
	require.NoError(t, lease.Close())
}

func TestDetachedAppendRaceRootCloseAndStaleTTL(t *testing.T) {
	root := detachedTestRoot(t)
	prepared := detachedPreparedRange(t, root, 0, 1, detachedCacheRecords(0, 1, 100), false)
	adopted, err := root.Adopt(prepared)
	require.NoError(t, err)
	oldLease, err := adopted.Acquire()
	require.NoError(t, err)
	suffix := detachedPreparedRange(t, root, 1, 2, detachedCacheRecords(1, 1, 100), false)
	entered, release := make(chan struct{}), make(chan struct{})
	root.appendIO.beforePublish = func() error { close(entered); <-release; return nil }
	result := make(chan error, 1)
	go func() {
		_, err := adopted.AppendPrepared(context.Background(), oldLease.Bounds(), suffix)
		result <- err
	}()
	<-entered
	require.ErrorIs(t, root.Close(), ErrDetachedBootstrapPending)
	ttlDone := make(chan error, 1)
	go func() { ttlDone <- prepared.transfer.close() }()
	close(release)
	require.ErrorIs(t, <-result, ErrDetachedBootstrapClosed)
	require.NoError(t, <-ttlDone)
	page, err := oldLease.ReadRaw(context.Background(), 0, 10, 1<<20)
	require.NoError(t, err)
	require.Equal(t, detachedCacheRecords(0, 1, 100), page.Messages)
	require.NoError(t, oldLease.Close())
	require.NoError(t, root.Close())
}

func TestDetachedAdoptAcquireAndCloseRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		root := detachedTestRoot(t)
		prepared := detachedPreparedRange(t, root, 0, 1, nil, true)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			if adopted, err := root.Adopt(prepared); err == nil {
				_ = adopted.Close()
			}
		}()
		go func() {
			defer wg.Done()
			if lease, err := prepared.Acquire(); err == nil {
				_ = lease.Close()
			}
		}()
		go func() { defer wg.Done(); _ = root.Close() }()
		wg.Wait()
		require.NoError(t, root.Close())
	}
}

func TestDetachedAdoptionSeparatesSlotsAndByteLifetime(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	root, err := NewDetachedBootstrapRoot(t.TempDir(), DefaultBootstrapLimits(), 0, 0, 12)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	for i := 0; i < 12; i++ {
		prepared := detachedPreparedRange(t, root, 0, 1, []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 0}}, false)
		owner := prepared.transfer.byteOwner.reserved
		adopted, err := root.Adopt(prepared)
		require.NoError(t, err)
		require.Same(t, prepared.transfer, adopted.transfer)
		require.Equal(t, owner, adopted.transfer.byteOwner.reserved)
		require.NoError(t, prepared.Close())
		_, err = prepared.Acquire()
		require.ErrorIs(t, err, ErrDetachedBootstrapClosed)
		active, size := bootstrapBudgetSnapshot()
		require.Equal(t, beforeActive, active)
		require.Greater(t, size, beforeBytes)
	}
	extra := detachedPreparedRange(t, root, 0, 1, nil, true)
	_, err = root.Adopt(extra)
	require.Error(t, err)
	lease, err := extra.Acquire()
	require.NoError(t, err, "failed adoption leaves prepared ownership intact")
	require.NoError(t, lease.Close())
	require.NoError(t, extra.Close())
}

func TestDetachedAppendPreservesOldLeaseAndPhysicalIdentity(t *testing.T) {
	root := detachedTestRoot(t)
	first := types.DiskMessage{Topic: "orders", Partition: 1, Offset: 0, Payload: "first", TransactionalID: "tx", TransactionState: types.TransactionStateAborted}
	prepared := detachedPreparedRange(t, root, 0, 1, []types.DiskMessage{first}, false)
	adopted, err := root.Adopt(prepared)
	require.NoError(t, err)
	old, err := adopted.Acquire()
	require.NoError(t, err)
	defer func() { require.NoError(t, old.Close()) }()
	bounds := old.Bounds()
	path := filepath.Join(prepared.transfer.dir, "records")
	before, err := os.Stat(path)
	require.NoError(t, err)
	second := types.DiskMessage{Topic: "orders", Partition: 1, Offset: 1, Payload: "second", LeaderEpoch: 8, ControlBatchType: types.ControlBatchTransaction, TransactionMarker: types.TransactionMarkerAbort}
	suffix := detachedPreparedRange(t, root, 1, 2, []types.DiskMessage{second}, false)
	result, err := adopted.AppendPrepared(context.Background(), bounds, suffix)
	require.NoError(t, err)
	require.NoError(t, result.CleanupError)
	require.Equal(t, uint64(suffix.transfer.metadata.Files[0].Size), result.Work.RawBytes)
	require.Equal(t, uint64(2), result.Bounds.EndOffset)
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
	require.Equal(t, bounds, old.Bounds())
	page, err := old.ReadRaw(context.Background(), 0, 10, 1<<20)
	require.NoError(t, err)
	require.Equal(t, []types.DiskMessage{first}, page.Messages)
	newLease, err := adopted.Acquire()
	require.NoError(t, err)
	defer func() { require.NoError(t, newLease.Close()) }()
	page, err = newLease.ReadRaw(context.Background(), 0, 10, 1<<20)
	require.NoError(t, err)
	require.Equal(t, []types.DiskMessage{first, second}, page.Messages)
	require.Equal(t, result.Bounds, newLease.Bounds())
	require.NoDirExists(t, suffix.transfer.dir)
}

func TestDetachedAppendMixedRangeGapAuthority(t *testing.T) {
	for _, prefixMarked := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			if empty && prefixMarked {
				continue
			}
			t.Run(fmt.Sprintf("prefix-marked=%t/empty=%t", prefixMarked, empty), func(t *testing.T) {
				root := detachedTestRoot(t)
				first := detachedCacheRecords(0, 2, 5000)
				prepared := detachedPreparedRange(t, root, 0, 2, first, prefixMarked)
				original := cloneBootstrapMetadata(prepared.transfer.metadata)
				adopted, err := root.Adopt(prepared)
				require.NoError(t, err)
				old, err := adopted.Acquire()
				require.NoError(t, err)
				defer func() { require.NoError(t, old.Close()) }()
				path := filepath.Join(prepared.transfer.dir, "records")
				before, err := os.Stat(path)
				require.NoError(t, err)
				second := detachedCacheRecords(2, 2, 5000)
				if empty {
					second = nil
				}
				suffix := detachedPreparedRange(t, root, 2, 4, second, !prefixMarked)
				raw, index := suffix.transfer.detached.extent.RawBytes, suffix.transfer.detached.extent.IndexBytes
				result, err := adopted.AppendPrepared(context.Background(), old.Bounds(), suffix)
				require.NoError(t, err)
				require.NoError(t, result.CleanupError)
				require.Equal(t, uint64(raw), result.Work.RawReadBytes)
				require.Equal(t, uint64(raw), result.Work.RawBytes)
				require.Equal(t, uint64(index), result.Work.IndexReadBytes)
				require.Equal(t, original, prepared.transfer.metadata)
				after, err := os.Stat(path)
				require.NoError(t, err)
				require.True(t, os.SameFile(before, after))
				page, err := old.ReadRaw(context.Background(), 0, 10, 1<<20)
				require.NoError(t, err)
				require.Equal(t, first, page.Messages)
				require.Equal(t, uint64(2), old.Bounds().EndOffset)
				fresh, err := adopted.Acquire()
				require.NoError(t, err)
				defer func() { require.NoError(t, fresh.Close()) }()
				page, err = fresh.ReadRaw(context.Background(), 0, 10, 1<<20)
				require.NoError(t, err)
				require.Equal(t, append(first, second...), page.Messages)
				require.Equal(t, uint64(4), fresh.Bounds().EndOffset)
			})
		}
	}
	root := detachedTestRoot(t)
	m, raw := detachedTestMetadata(t, detachedCacheRecords(1, 1, 100), 3, false)
	store, err := detachedTestStage(t, root, m, raw).Prepare(context.Background())
	require.Error(t, err, "unmarked actual gaps must still fail preparation")
	require.Nil(t, store)
}
