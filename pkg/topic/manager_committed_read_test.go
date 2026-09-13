package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestTopicManagerReadCommittedPartitionCapsAtHWM(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	manager := NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	partition, err := manager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReplicaAppend([]types.Message{
		{Offset: 0, Payload: "committed-0"},
		{Offset: 1, Payload: "committed-1"},
		{Offset: 2, Payload: "uncommitted"},
	}))
	require.NoError(t, partition.ApplyReplicaHWM(2))

	messages, err := manager.ReadCommittedTopicPartition("orders", 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, []uint64{0, 1}, []uint64{messages[0].Offset, messages[1].Offset})
	require.Equal(t, uint64(3), partition.NextOffset())
	require.Equal(t, uint64(2), partition.GetHWM())

	_, err = manager.ReadCommittedTopicPartition("missing", 0, 0, 1)
	require.ErrorContains(t, err, "does not exist")
	_, err = manager.ReadCommittedTopicPartition("orders", 1, 0, 1)
	require.Error(t, err)
}
