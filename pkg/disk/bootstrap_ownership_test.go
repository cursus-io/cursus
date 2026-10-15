package disk

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func bootstrapBudgetSnapshot() (int, int64) {
	bootstrapBudget.Lock()
	defer bootstrapBudget.Unlock()
	return bootstrapBudget.active, bootstrapBudget.bytes
}

func TestBootstrapCleanupFailureRetainsOwnershipAndRetries(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	transfer, err := newBootstrapTransfer(filepath.Join(t.TempDir(), "partition_1"), BootstrapMetadata{}, 23, DefaultBootstrapLimits())
	require.NoError(t, err)
	path := filepath.Join(transfer.dir, "records")
	require.NoError(t, os.WriteFile(path, []byte("retained bytes"), 0600))
	injected := errors.New("injected staging deletion failure")
	calls := 0
	transfer.removeAll = func(string) error { calls++; return injected }
	t.Cleanup(func() { transfer.removeAll = os.RemoveAll; _ = transfer.close() })
	require.ErrorIs(t, transfer.close(), injected)
	require.FileExists(t, path)
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+1, active, "failed cleanup must retain its active token")
	require.Equal(t, beforeBytes+23, reserved, "failed cleanup must retain its full byte reservation")
	require.ErrorIs(t, transfer.close(), injected)
	require.Equal(t, 2, calls, "closing a retired handle must retry cleanup")
	_, err = (&BootstrapExport{transfer: transfer}).ReadChunk("records", 0, 1)
	require.Error(t, err)
	receiver := &BootstrapReceiver{transfer: transfer}
	require.Error(t, receiver.WriteChunk("records", 0, nil))
	require.Error(t, receiver.Install())
	transfer.removeAll = os.RemoveAll
	require.NoError(t, receiver.Abort())
	require.NoDirExists(t, transfer.dir)
	require.NoError(t, transfer.close())
	active, reserved = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes, reserved)
}

func bootstrapCleanupContains(transfer *bootstrapTransfer) bool {
	bootstrapCleanup.Lock()
	defer bootstrapCleanup.Unlock()
	for _, pending := range bootstrapCleanup.pending {
		if pending == transfer {
			return true
		}
	}
	return false
}

func bootstrapTestCleanupTransfer(t *testing.T, size int64) *bootstrapTransfer {
	t.Helper()
	transfer, err := newBootstrapTransfer(filepath.Join(t.TempDir(), "partition_1"), BootstrapMetadata{}, size, DefaultBootstrapLimits())
	require.NoError(t, err)
	t.Cleanup(func() {
		transfer.mu.Lock()
		transfer.removeAll = os.RemoveAll
		transfer.mu.Unlock()
		require.NoError(t, transfer.close())
	})
	return transfer
}

func TestBootstrapCleanupPartialDeletionRetainsFullReservation(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	transfer := bootstrapTestCleanupTransfer(t, 100)
	first, second := filepath.Join(transfer.dir, "records"), filepath.Join(transfer.dir, "catalog")
	require.NoError(t, os.WriteFile(first, []byte("first"), 0600))
	require.NoError(t, os.WriteFile(second, []byte("second"), 0600))
	transfer.removeAll = func(string) error {
		if err := os.Remove(first); err != nil {
			return err
		}
		return errors.New("partial removal")
	}
	require.Error(t, transfer.close())
	require.NoFileExists(t, first)
	require.FileExists(t, second)
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+1, active)
	require.Equal(t, beforeBytes+100, reserved)
	require.True(t, bootstrapCleanupContains(transfer))
	transfer.removeAll = os.RemoveAll
	require.NoError(t, transfer.close())
	require.False(t, bootstrapCleanupContains(transfer))
	active, reserved = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes, reserved)
}

func TestBootstrapCleanupTTLFailureRemainsReachableForAdmission(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	transfer := bootstrapTestCleanupTransfer(t, 41)
	require.NoError(t, os.WriteFile(filepath.Join(transfer.dir, "records"), []byte("ttl"), 0600))
	transfer.removeAll = func(string) error { return errors.New("TTL cleanup failed") }
	transfer.startTTL()
	transfer.timer.Reset(0)
	require.Eventually(t, func() bool { return bootstrapCleanupContains(transfer) }, time.Second, time.Millisecond)
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+1, active)
	require.Equal(t, beforeBytes+41, reserved)
	transfer.mu.Lock()
	require.True(t, transfer.closed)
	transfer.removeAll = os.RemoveAll
	transfer.mu.Unlock()
	admitted := bootstrapTestCleanupTransfer(t, 7)
	require.NoDirExists(t, transfer.dir)
	require.False(t, bootstrapCleanupContains(transfer))
	active, reserved = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+1, active)
	require.Equal(t, beforeBytes+7, reserved)
	require.NoError(t, admitted.close())
}

func TestBootstrapCleanupZeroByteSaturationAndRoundRobin(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	var transfers []*bootstrapTransfer
	var attempts atomic.Int64
	for i := beforeActive; i < BootstrapMaxActive; i++ {
		transfer := bootstrapTestCleanupTransfer(t, 0)
		transfer.removeAll = func(string) error { attempts.Add(1); return errors.New("cannot remove empty stage") }
		require.Error(t, transfer.close())
		transfers = append(transfers, transfer)
	}
	base := filepath.Join(t.TempDir(), "partition_1")
	// An admission retries exactly one queued owner even when every retry fails.
	for range len(transfers) {
		prior := attempts.Load()
		_, err := newBootstrapTransfer(base, BootstrapMetadata{}, 0, DefaultBootstrapLimits())
		var limit *BootstrapLimitError
		require.ErrorAs(t, err, &limit)
		require.Equal(t, "active transfers", limit.Resource)
		require.Equal(t, prior+1, attempts.Load())
	}
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, BootstrapMaxActive, active)
	require.Equal(t, beforeBytes, reserved)
	bootstrapCleanup.Lock()
	require.Len(t, bootstrapCleanup.pending, len(transfers))
	next := bootstrapCleanup.pending[0]
	bootstrapCleanup.Unlock()
	next.removeAll = os.RemoveAll
	replacement, err := newBootstrapTransfer(base, BootstrapMetadata{}, 0, DefaultBootstrapLimits())
	require.NoError(t, err)
	require.NoDirExists(t, next.dir)
	require.NoError(t, replacement.close())
	// Every remaining owner must get a turn; a failed first owner cannot starve it.
	for _, transfer := range transfers {
		transfer.removeAll = os.RemoveAll
	}
	for range len(transfers) - 1 {
		retryBootstrapCleanup()
	}
	for _, transfer := range transfers {
		require.False(t, bootstrapCleanupContains(transfer))
	}
	active, reserved = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes, reserved)
}

func TestBootstrapCleanupAdmissionSkipsBusyOwnerAndReleasesGlobalLocks(t *testing.T) {
	transfer := bootstrapTestCleanupTransfer(t, 13)
	transfer.removeAll = func(string) error { return errors.New("cleanup failed") }
	require.Error(t, transfer.close())
	// Model a caller holding a partition lock while the chosen owner holds t.mu.
	transfer.mu.Lock()
	done := make(chan error, 1)
	base := filepath.Join(t.TempDir(), "partition_1")
	go func() {
		admitted, err := newBootstrapTransfer(base, BootstrapMetadata{}, 0, DefaultBootstrapLimits())
		if err == nil {
			err = admitted.close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		transfer.mu.Unlock()
		t.Fatal("admission waited on a busy cleanup owner's lock")
	}
	transfer.removeAll = func(path string) error {
		// Acquiring both global locks from inside deletion proves neither spans I/O.
		bootstrapBudget.Lock()
		active := bootstrapBudget.active
		bootstrapBudget.Unlock()
		bootstrapCleanup.Lock()
		pending := len(bootstrapCleanup.pending)
		bootstrapCleanup.Unlock()
		require.Positive(t, active)
		require.Positive(t, pending)
		return os.RemoveAll(path)
	}
	transfer.mu.Unlock()
	go func() { retryBootstrapCleanup(); done <- nil }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup held a global lock during deletion")
	}
	require.NoDirExists(t, transfer.dir)
}

func TestBootstrapCleanupConcurrentTTLAbortAndAdmissionReleaseOnce(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	transfer := bootstrapTestCleanupTransfer(t, 19)
	transfer.removeAll = func(string) error { return errors.New("cleanup failed") }
	require.Error(t, transfer.close())
	var successfulDeletes atomic.Int64
	transfer.removeAll = func(path string) error { successfulDeletes.Add(1); return os.RemoveAll(path) }
	// Reset delivers the actual TTL callback alongside explicit abort/admission.
	transfer.startTTL()
	transfer.timer.Reset(0)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 12 {
		workers.Add(1)
		go func() { defer workers.Done(); <-start; _ = (&BootstrapReceiver{transfer: transfer}).Abort() }()
	}
	workers.Add(1)
	base := filepath.Join(t.TempDir(), "partition_1")
	admitted := make(chan error, 1)
	go func() {
		defer workers.Done()
		<-start
		next, err := newBootstrapTransfer(base, BootstrapMetadata{}, 0, DefaultBootstrapLimits())
		if err == nil {
			err = next.close()
		}
		admitted <- err
	}()
	close(start)
	workers.Wait()
	require.NoError(t, <-admitted)
	require.NoError(t, transfer.close())
	require.Equal(t, int64(1), successfulDeletes.Load())
	require.False(t, bootstrapCleanupContains(transfer))
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes, reserved)
}

func TestBootstrapConstructionFailureReleasesOnlyReservations(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	parent := filepath.Join(t.TempDir(), "occupied")
	require.NoError(t, os.WriteFile(parent, []byte("not a directory owned by the transfer"), 0600))
	_, err := newBootstrapTransfer(filepath.Join(parent, "partition_1"), BootstrapMetadata{}, 71, DefaultBootstrapLimits())
	require.Error(t, err)
	data, err := os.ReadFile(parent)
	require.NoError(t, err)
	require.Equal(t, "not a directory owned by the transfer", string(data))
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes, reserved)
}

func TestBootstrapPhysicalByteOwnerGrowthAndIndependentRelease(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	d := &DiskHandler{}
	limits := BootstrapLimits{MaxTransferBytes: 100, MaxTotalBytes: beforeBytes + 100}
	transfer, err := newBootstrapTransfer(filepath.Join(t.TempDir(), "partition_1"), BootstrapMetadata{}, 10, limits)
	require.NoError(t, err)
	defer func() { require.NoError(t, transfer.close()) }()
	require.NoError(t, d.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: 1, MaxTotalBytes: 1}))
	require.NoError(t, reserveBootstrapGrowth(d, transfer, 100))
	require.Equal(t, int64(100), transfer.byteOwner.reserved)
	require.Error(t, reserveBootstrapGrowth(d, transfer, 101))
	require.Error(t, reserveBootstrapGrowth(d, transfer, math.MaxInt64))
	require.Error(t, reserveBootstrapGrowth(d, transfer, -1))
	require.Equal(t, int64(100), transfer.byteOwner.reserved)
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+1, active)
	require.Equal(t, beforeBytes+100, reserved)
	// Releasing an active slot alone cannot remove the physical-byte charge.
	transfer.activeToken.release()
	transfer.activeToken.release()
	active, reserved = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes+100, reserved)
	require.NoError(t, transfer.close())
	transfer.byteOwner.release()
	require.Error(t, reserveBootstrapGrowth(d, transfer, 100))
	active, reserved = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes, reserved)
}

func TestBootstrapDurableHandoffReleasesTransientOwnersOnce(t *testing.T) {
	for _, failAfterIntent := range []bool{false, true} {
		name := "successful-install"
		if failAfterIntent {
			name = "replacement-failure"
		}
		t.Run(name, func(t *testing.T) {
			source, _ := bootstrapTestHandler(t, 1)
			export := bootstrapTestExport(t, source, 0, 0)
			target, cfg := bootstrapTestHandler(t, 1)
			beforeActive, beforeBytes := bootstrapBudgetSnapshot()
			receiver := bootstrapTestReceive(t, target, export)
			transfer := receiver.transfer
			stage := transfer.dir
			expectedCatalog, err := os.ReadFile(filepath.Join(stage, "catalog"))
			require.NoError(t, err)
			var deletionAttempts atomic.Int64
			transfer.removeAll = func(string) error { deletionAttempts.Add(1); return errors.New("must not delete durable intent") }
			if failAfterIntent {
				bootstrapAfterReplace = func(string) error { return errors.New("replacement failed after intent") }
				t.Cleanup(func() { bootstrapAfterReplace = nil })
				require.Error(t, receiver.Install())
				bootstrapAfterReplace = nil
			} else {
				require.NoError(t, receiver.Install())
				require.NoError(t, receiver.Install())
			}
			active, reserved := bootstrapBudgetSnapshot()
			require.Equal(t, beforeActive, active)
			require.Equal(t, beforeBytes, reserved)
			require.False(t, transfer.activeToken.owned)
			require.False(t, transfer.byteOwner.owned)
			require.False(t, bootstrapCleanupContains(transfer))
			require.ErrorIs(t, receiver.Abort(), ErrBootstrapPending)
			retryBootstrapCleanup()
			require.Zero(t, deletionAttempts.Load())
			require.DirExists(t, stage)
			var tx bootstrapTransaction
			require.NoError(t, readBootstrapEnvelope(bootstrapPendingPath(target.BaseName), &tx))
			require.Equal(t, stage, target.BaseName+".bootstrap-"+tx.StageID)
			require.Equal(t, export.Manifest().Files[1], tx.Metadata.Files[1])
			actualCatalog, err := target.PendingBootstrapCatalog()
			require.NoError(t, err)
			require.Equal(t, expectedCatalog, actualCatalog)
			require.NoError(t, target.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: 1, MaxTotalBytes: 1}))
			retry, err := target.StageBootstrap(export.Manifest())
			require.NoError(t, err)
			require.Equal(t, stage, retry.transfer.dir)
			require.ErrorIs(t, retry.Abort(), ErrBootstrapPending)
			require.NoError(t, target.Close())
			target, err = NewDiskHandler(cfg, "orders", 1)
			require.NoError(t, err)
			defer func() { require.NoError(t, target.Close()) }()
			require.NoError(t, target.SetBootstrapLimits(BootstrapLimits{MaxTransferBytes: 1, MaxTotalBytes: 1}))
			actualCatalog, err = target.PendingBootstrapCatalog()
			require.NoError(t, err)
			require.Equal(t, expectedCatalog, actualCatalog)
			retry, err = target.StageBootstrap(export.Manifest())
			require.NoError(t, err)
			require.NoError(t, retry.Install())
			require.NoError(t, target.CompleteBootstrap(export.Manifest().TransferID))
			retry, err = target.StageBootstrap(export.Manifest())
			require.NoError(t, err)
			require.NoError(t, retry.Install())
			active, reserved = bootstrapBudgetSnapshot()
			require.Equal(t, beforeActive, active)
			require.Equal(t, beforeBytes, reserved)
		})
	}
}

func TestBootstrapPhysicalByteGrowthPreservesAggregateLimitAndHeadroom(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	limits := BootstrapLimits{MaxTransferBytes: 100, MaxTotalBytes: beforeBytes + 100}
	base := filepath.Join(t.TempDir(), "partition_1")
	first, err := newBootstrapTransfer(base, BootstrapMetadata{}, 10, limits)
	require.NoError(t, err)
	defer func() { require.NoError(t, first.close()) }()
	second, err := newBootstrapTransfer(base, BootstrapMetadata{}, 50, limits)
	require.NoError(t, err)
	defer func() { require.NoError(t, second.close()) }()
	d := &DiskHandler{}
	var limit *BootstrapLimitError
	require.ErrorAs(t, reserveBootstrapGrowth(d, first, 51), &limit)
	require.Equal(t, "total staging bytes", limit.Resource)
	require.Equal(t, int64(10), first.byteOwner.reserved)
	d.headroom = &diskHeadroomGuard{path: t.TempDir(), checkedAt: time.Now(), free: 20, total: 100, minFreeBytes: 10}
	require.ErrorContains(t, reserveBootstrapGrowth(d, first, 30), "insufficient filesystem headroom")
	require.Equal(t, int64(10), first.byteOwner.reserved)
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+2, active)
	require.Equal(t, beforeBytes+60, reserved)
	require.NoError(t, reserveBootstrapGrowth(d, first, 20))
	require.Equal(t, int64(20), first.byteOwner.reserved)
	active, reserved = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+2, active)
	require.Equal(t, beforeBytes+70, reserved)
}

func TestBootstrapAdmissionRetriesCleanupBeforeHeadroom(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	export := bootstrapTestExport(t, source, 0, 0)
	for _, operation := range []string{"export", "receive"} {
		t.Run(operation, func(t *testing.T) {
			target, _ := bootstrapTestHandler(t, 1)
			transfer := bootstrapTestCleanupTransfer(t, 0)
			other := bootstrapTestCleanupTransfer(t, 0)
			transfer.removeAll = func(string) error { return errors.New("initial cleanup failure") }
			other.removeAll = func(string) error { return errors.New("second cleanup failure") }
			require.Error(t, transfer.close())
			require.Error(t, other.close())
			otherAttempts := 0
			other.removeAll = func(string) error { otherAttempts++; return errors.New("second cleanup still fails") }
			// With this guard, admission fails unless cleanup runs first. The cleanup
			// hook restores capacity deterministically without allocating a large file.
			target.headroom.minFreeBytes = math.MaxUint64
			attempts := 0
			transfer.removeAll = func(path string) error {
				attempts++
				target.headroom.mu.Lock()
				target.headroom.minFreeBytes = 0
				target.headroom.mu.Unlock()
				return os.RemoveAll(path)
			}
			if operation == "export" {
				admitted, err := target.ExportBootstrap(0, 0, bootstrapTestFence(1, 0), nil)
				require.NoError(t, err)
				require.NoError(t, admitted.Close())
			} else {
				admitted, err := target.StageBootstrap(export.Manifest())
				require.NoError(t, err)
				require.NoError(t, admitted.Abort())
			}
			require.Equal(t, 1, attempts)
			require.Zero(t, otherAttempts, "one admission must not retry a second cleanup owner")
			require.NoDirExists(t, transfer.dir)
			require.False(t, bootstrapCleanupContains(transfer))
		})
	}
}
