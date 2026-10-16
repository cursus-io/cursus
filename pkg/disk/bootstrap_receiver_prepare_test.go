package disk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func bootstrapPrepareFixture(t *testing.T) (*DiskHandler, *BootstrapReceiver) {
	t.Helper()
	source, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, source.WriteCompactedReplicaRange(0, 5, []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 2, Payload: "retained"}}))
	export := bootstrapTestExport(t, source, 0, 5)
	target, _ := bootstrapTestHandler(t, 1)
	_, err := target.AppendMessageSync("orders", 1, &types.Message{Payload: "old live record"})
	require.NoError(t, err)
	return target, bootstrapTestReceive(t, target, export)
}

func TestBootstrapReceiverPrepareLeavesLiveStateAndSealsChunks(t *testing.T) {
	d, r := bootstrapPrepareFixture(t)
	hwm := d.BaseName + ".hwm"
	require.NoError(t, os.WriteFile(hwm, []byte("existing checkpoint"), 0o600))
	before, err := os.ReadFile(d.GetSegmentPath(0))
	require.NoError(t, err)
	active, reserved := bootstrapBudgetSnapshot()
	// Preparation must not acquire the live partition's maintenance gate.
	d.maintenanceMu.Lock()
	done := make(chan error, 1)
	go func() { done <- r.PrepareContext(context.Background()) }()
	select {
	case err = <-done:
		d.maintenanceMu.Unlock()
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		d.maintenanceMu.Unlock()
		<-done
		t.Fatal("preparation waited for live maintenance gate")
	}
	after, err := os.ReadFile(d.GetSegmentPath(0))
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, uint64(1), d.GetLatestOffset())
	checkpoint, err := os.ReadFile(hwm)
	require.NoError(t, err)
	require.Equal(t, "existing checkpoint", string(checkpoint))
	_, err = os.Stat(bootstrapPendingPath(d.BaseName))
	require.True(t, os.IsNotExist(err))
	gotActive, gotReserved := bootstrapBudgetSnapshot()
	require.Equal(t, active, gotActive)
	require.Equal(t, reserved, gotReserved)
	catalog, err := os.ReadFile(filepath.Join(r.transfer.dir, "catalog"))
	require.NoError(t, err)
	require.Error(t, r.WriteChunk("catalog", 0, catalog))
	index := filepath.Join(r.transfer.dir, "range-index")
	stamp := time.Unix(123456789, 0)
	require.NoError(t, os.Chtimes(index, stamp, stamp))
	require.NoError(t, r.PrepareContext(context.Background()))
	info, err := os.Stat(index)
	require.NoError(t, err)
	require.Equal(t, stamp, info.ModTime(), "duplicate preparation rebuilt the index")
	require.NoError(t, r.Install())
	require.NoError(t, r.WriteChunk("catalog", 0, catalog), "installed duplicate behavior must remain compatible")
	require.NoError(t, d.CompleteBootstrap(r.Manifest().TransferID))
}

// Cancel at a filesystem-observable preparation boundary without timing sleeps.
type bootstrapPrepareCancelContext struct {
	context.Context
	cancel context.CancelFunc
	path   string
}

func (c bootstrapPrepareCancelContext) Err() error {
	if _, err := os.Stat(c.path); err == nil {
		c.cancel()
	}
	return c.Context.Err()
}

func TestBootstrapReceiverPrepareCancellationAndRetry(t *testing.T) {
	for _, boundary := range []string{"before", "range-index", "tail-index", "abort-partial"} {
		t.Run(boundary, func(t *testing.T) {
			_, r := bootstrapPrepareFixture(t)
			active, reserved := bootstrapBudgetSnapshot()
			require.ErrorContains(t, r.PrepareContext(nil), "context is nil") //nolint:staticcheck // Exercise the explicit nil-context rejection contract.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			prepareCtx := ctx
			if boundary == "before" {
				cancel()
			} else {
				file := boundary
				if boundary == "abort-partial" {
					file = "range-index"
				}
				prepareCtx = bootstrapPrepareCancelContext{Context: ctx, cancel: cancel, path: filepath.Join(r.transfer.dir, file)}
			}
			require.ErrorIs(t, r.PrepareContext(prepareCtx), context.Canceled)
			gotActive, gotReserved := bootstrapBudgetSnapshot()
			require.Equal(t, active, gotActive)
			require.Equal(t, reserved, gotReserved)
			catalog, err := os.ReadFile(filepath.Join(r.transfer.dir, "catalog"))
			require.NoError(t, err)
			require.NoError(t, r.WriteChunk("catalog", 0, catalog))
			if boundary != "abort-partial" {
				require.NoError(t, r.PrepareContext(context.Background()))
			}
			require.NoError(t, r.Abort())
			_, err = os.Stat(r.transfer.dir)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestBootstrapReceiverPreparedInstallRestartRecovery(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, source.WriteCompactedReplicaRange(0, 5, nil))
	export := bootstrapTestExport(t, source, 0, 5)
	target, cfg := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, target, export)
	require.NoError(t, r.PrepareContext(context.Background()))
	bootstrapAfterReplace = func(string) error { return errors.New("interrupted replacement") }
	installErr := r.Install()
	bootstrapAfterReplace = nil
	require.ErrorContains(t, installErr, "interrupted replacement")
	require.Error(t, r.PrepareContext(context.Background()))
	require.ErrorIs(t, r.Abort(), ErrBootstrapPending)
	require.NoError(t, target.Close())
	reopened, err := NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	retry, err := reopened.StageBootstrap(export.Manifest())
	require.NoError(t, err)
	require.NoError(t, retry.PrepareContext(context.Background()))
	require.NoError(t, retry.Install())
	require.Equal(t, uint64(5), reopened.GetLatestOffset())
	require.NoError(t, reopened.CompleteBootstrap(export.Manifest().TransferID))
	completed, err := reopened.StageBootstrap(export.Manifest())
	require.NoError(t, err)
	require.NoError(t, completed.PrepareContext(context.Background()))
	require.NoError(t, completed.Install())
}

func TestBootstrapReceiverPreparedCorruptionRejectedBeforeIntent(t *testing.T) {
	for _, id := range []string{"records", "catalog", "range-index", "marker", "tail-log", "tail-index"} {
		t.Run(id, func(t *testing.T) {
			d, r := bootstrapPrepareFixture(t)
			require.NoError(t, r.PrepareContext(context.Background()))
			path := filepath.Join(r.transfer.dir, id)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			if len(data) == 0 {
				data = []byte("corrupt after preparation")
			} else {
				data[0] ^= 1 // Preserve size to exercise checksum revalidation.
			}
			require.NoError(t, os.WriteFile(path, data, 0o600))
			require.Error(t, r.Install())
			_, err = os.Stat(bootstrapPendingPath(d.BaseName))
			require.True(t, os.IsNotExist(err))
			require.Equal(t, uint64(1), d.GetLatestOffset())
			require.False(t, d.bootstrapPending.Load())
			require.NoError(t, r.Abort())
		})
	}
}

func TestBootstrapReceiverPreparedTTLAndAbortOwnership(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort failure", true: "original TTL"}[expire], func(t *testing.T) {
			_, r := bootstrapPrepareFixture(t)
			active, reserved := bootstrapBudgetSnapshot()
			owned := r.transfer.byteOwner.reserved
			if expire {
				// Shorten the original TTL; preparation must not refresh it to
				// BootstrapTTL or prevent its callback from reclaiming ownership.
				r.transfer.timer.Reset(200 * time.Millisecond)
				require.NoError(t, r.PrepareContext(context.Background()))
				require.NoError(t, r.PrepareContext(context.Background()))
			} else {
				require.NoError(t, r.PrepareContext(context.Background()))
				remove := r.transfer.removeAll
				r.transfer.removeAll = func(string) error { return errors.New("cleanup unavailable") }
				require.ErrorContains(t, r.Abort(), "cleanup unavailable")
				gotActive, gotReserved := bootstrapBudgetSnapshot()
				require.Equal(t, active, gotActive)
				require.Equal(t, reserved, gotReserved)
				r.transfer.removeAll = remove
				require.NoError(t, r.Abort())
			}
			require.Eventually(t, func() bool {
				gotActive, gotReserved := bootstrapBudgetSnapshot()
				return gotActive == active-1 && gotReserved == reserved-owned
			}, time.Second, time.Millisecond)
			require.Error(t, r.PrepareContext(context.Background()))
			require.Error(t, r.Install())
		})
	}
}
