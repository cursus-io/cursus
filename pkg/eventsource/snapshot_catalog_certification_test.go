package eventsource

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

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
		files, err := filepath.Glob(filepath.Join(h.tm.GetLogDir("orders", 0), "partition_0.bootstrap-*"))
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
