package eventsource

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestOwnedBootstrapCatalogRequiresSourceManager(t *testing.T) {
	for _, h := range []*Handler{nil, {}} {
		exported, err := h.ExportBootstrapWithCatalogOwnedContext(context.Background(), "orders", 0, nil, 1, disk.BootstrapFence{})
		require.ErrorContains(t, err, "bootstrap catalog source unavailable")
		require.Nil(t, exported)
	}
}

func TestBootstrapCatalogRejectsWholeAheadInventory(t *testing.T) {
	h, p := newBootstrapHandler(t)
	appendSnapshotFixtureHistory(t, h, SnapshotResult{Topic: "orders", Key: "a", Version: 1})
	require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "good"}))
	require.NoError(t, seedStaleSnapshotFixture(h, SnapshotResult{Topic: "orders", Key: "z", Version: 99, Payload: "ahead"}))
	require.NoError(t, p.WithBootstrapSource(func(source *topic.BootstrapSource) error {
		data, err := h.CaptureBootstrapCatalog(context.Background(), "orders", 0, source, 1)
		require.ErrorContains(t, err, "snapshot_version_exceeds_stream")
		require.Nil(t, data)
		return nil
	}))
}

// Inspect only private export files from the synchronous context check. This
// changes the decision after canonical bytes were written, while export is
// still active, without relying on scheduler timing or adding a production hook.
type catalogExportDecisionContext struct {
	context.Context
	directory string
	changed   bool
	change    func()
}

func (c *catalogExportDecisionContext) Err() error {
	if !c.changed {
		paths, _ := filepath.Glob(filepath.Join(c.directory, "partition_0.bootstrap-*", "records"))
		for _, path := range paths {
			if info, err := os.Stat(path); err == nil && info.Size() > 0 {
				c.changed = true
				c.change()
				break
			}
		}
	}
	return c.Context.Err()
}

func TestBootstrapCatalogOwnedExportRechecksDecisionDuringCopy(t *testing.T) {
	h, p := newBootstrapHandler(t)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1, TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen}, {Offset: 1, TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit}}))
	require.NoError(t, p.ApplyReplicaHWM(2))
	manager := transaction.NewManager()
	p.SetTransactionDecisionResolver(manager)
	require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "good"}))
	ctx := &catalogExportDecisionContext{Context: context.Background(), directory: filepath.Dir(h.tm.GetLogDir("orders", 0)), change: func() { _, _, err := manager.InitProducer("changed-during-copy"); require.NoError(t, err) }}
	err := p.WithBootstrapSourceContext(ctx, func(source *topic.BootstrapSource) error {
		fence := disk.BootstrapFence{Topic: "orders", Partition: 0, SourceID: "source", LeaderID: "source", ReceiverID: "target", LeaderEpoch: 1, LifecycleEpoch: 1, RecoveryProtocol: 2, CommittedHWM: 2}
		exported, err := h.ExportBootstrapWithCatalogOwnedContext(ctx, "orders", 0, source, 1, fence)
		if exported != nil {
			defer func() { require.NoError(t, exported.Close()) }()
		}
		require.Nil(t, exported)
		return err
	})
	require.True(t, ctx.changed, "decision must change after actual canonical writes")
	require.ErrorContains(t, err, "decisions changed")
	paths, err := filepath.Glob(filepath.Join(ctx.directory, "partition_0.bootstrap-*"))
	require.NoError(t, err)
	require.Empty(t, paths)
}

func TestBootstrapCatalogOriginalGuardClosesFailedExport(t *testing.T) {
	h, p := newBootstrapHandler(t)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1, TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen}, {Offset: 1, TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit}}))
	require.NoError(t, p.ApplyReplicaHWM(2))
	manager := transaction.NewManager()
	p.SetTransactionDecisionResolver(manager)
	require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "good"}))
	require.NoError(t, p.WithBootstrapSource(func(source *topic.BootstrapSource) error {
		captured, err := h.captureBootstrapCatalog(context.Background(), "orders", 0, source, 1)
		require.NoError(t, err)
		_, _, err = manager.InitProducer("changed-after-catalog")
		require.NoError(t, err)
		fence := disk.BootstrapFence{Topic: "orders", Partition: 0, SourceID: "source", LeaderID: "source", ReceiverID: "target", LeaderEpoch: 1, LifecycleEpoch: 1, RecoveryProtocol: 3, CommittedHWM: 2}
		exported, err := captured.export(context.Background(), source, fence)
		require.ErrorContains(t, err, "decisions changed")
		require.Nil(t, exported)
		files, err := filepath.Glob(filepath.Join(filepath.Dir(h.tm.GetLogDir("orders", 0)), "partition_0.bootstrap-*"))
		require.NoError(t, err)
		require.Empty(t, files)
		require.Nil(t, h.indexes["orders:0"])
		return nil
	}))
}

func TestSnapshotCertificationSeedBoundaryAndRevision(t *testing.T) {
	idx, err := NewStreamIndex(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, idx.Close()) })
	require.NoError(t, idx.seedVersionAt("a", 4, 10))
	before, err := idx.VersionBefore("a", 9)
	require.NoError(t, err)
	require.Zero(t, before)
	at, err := idx.VersionBefore("a", 10)
	require.NoError(t, err)
	require.Equal(t, uint64(4), at)
	revision, err := idx.ResetRevision()
	require.NoError(t, err)
	require.NoError(t, idx.seedVersionAt("a", 4, 11))
	changed, err := idx.ResetRevision()
	require.NoError(t, err)
	require.Greater(t, changed, revision)
}
