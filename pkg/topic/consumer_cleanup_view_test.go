package topic

import (
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerCleanupViewBoundsDiskWorkAndCommitVisibility(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() { manager.Stop(); dm.CloseAllHandlers() })
	require.NoError(t, manager.CreateTopic(config.ConsumerOffsetsTopicName, 1, false, false))
	p, err := manager.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, p.ReplicaAppend([]types.Message{
		{Offset: 0, Payload: strings.Repeat("x", 6000), TransactionalID: "open", TransactionState: types.TransactionStateOpen},
		{Offset: 1, Payload: "committed-delete"},
		{Offset: 2, Payload: "uncommitted-delete"},
	}))
	require.NoError(t, p.ApplyReplicaHWM(2))
	var before coordinator.ConsumerCleanupPartition
	require.NoError(t, manager.WithConsumerCleanupView(func(view coordinator.ConsumerCleanupView) error {
		before = view.Partitions()[0]
		require.Equal(t, uint64(2), before.End)
		page, err := view.Read(0, 0, types.ReplayCursor{}, 128, 7000)
		require.NoError(t, err)
		require.LessOrEqual(t, page.Bytes, 7000)
		require.GreaterOrEqual(t, page.Records, 2, "an open transaction must not hide subsequent lifecycle records")
		require.Equal(t, uint64(2), page.Next)
		require.Len(t, page.Messages, 2)
		page, err = view.Read(0, 0, types.ReplayCursor{}, 1, 7000)
		require.NoError(t, err)
		require.Len(t, page.Messages, 1)
		_, err = view.Read(0, 0, types.ReplayCursor{}, 128, 128)
		require.ErrorContains(t, err, "byte budget")
		_, err = view.Read(0, 2, types.ReplayCursor{}, 128, 7000)
		require.ErrorContains(t, err, "committed range")
		return nil
	}))
	require.NoError(t, p.ReconcileSnapshotHWM(2))
	require.ErrorContains(t, manager.WithConsumerCleanupView(func(coordinator.ConsumerCleanupView) error {
		t.Error("snapshot-pending view must not be exposed")
		return nil
	}), "snapshot recovery pending")
	require.NoError(t, p.FinalizeSnapshotRecovery(2))
	require.NoError(t, manager.WithConsumerCleanupView(func(view coordinator.ConsumerCleanupView) error {
		require.NotEqual(t, before.Generation, view.Partitions()[0].Generation)
		return nil
	}))
}

func TestConsumerCleanupViewAdvancesAcrossCompactedHoles(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() { manager.Stop(); dm.CloseAllHandlers() })
	require.NoError(t, manager.CreateTopic(config.ConsumerOffsetsTopicName, 1, false, false))
	p := manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions[0]
	require.NoError(t, p.ReplicaAppendCompactedRange([]types.Message{{Offset: 3, Payload: "retained"}}, 5))
	require.NoError(t, p.ApplyReplicaHWM(5))
	require.NoError(t, manager.WithConsumerCleanupView(func(view coordinator.ConsumerCleanupView) error {
		page, err := view.Read(0, 0, types.ReplayCursor{}, 1, 4096)
		require.NoError(t, err)
		require.Len(t, page.Messages, 1)
		require.Equal(t, uint64(4), page.Next)
		page, err = view.Read(0, page.Next, page.Cursor, 128, 4096)
		if err == nil && page.Next < 5 {
			page, err = view.Read(0, page.Next, page.Cursor, 128, 4096)
		}
		require.NoError(t, err)
		require.Empty(t, page.Messages)
		require.Equal(t, uint64(5), page.Next, "empty compacted suffix is distinct from byte exhaustion")
		return nil
	}))
}
