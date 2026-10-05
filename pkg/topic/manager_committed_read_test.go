package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerMetadataReadPassesOpenTransactionButRespectsHWM(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() { manager.Stop(); dm.CloseAllHandlers() })
	require.NoError(t, manager.CreateTopic(config.ConsumerOffsetsTopicName, 1, false, false))
	p, err := manager.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, p.ReplicaAppend([]types.Message{
		{Offset: 0, Payload: "registration"},
		{Offset: 1, Payload: "pending", TransactionalID: "tx", TransactionState: types.TransactionStateOpen},
		{Offset: 2, Payload: "reservation"},
		{Offset: 3, Payload: "uncommitted-release"},
	}))
	require.NoError(t, p.ApplyReplicaHWM(3))
	ordinary, err := manager.ReadCommittedTopicPartition(config.ConsumerOffsetsTopicName, 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, ordinary, 1, "application reads retain their last-stable-offset barrier")
	metadata, err := manager.ReadCommittedConsumerMetadata(config.ConsumerOffsetsTopicName, 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, metadata, 2)
	require.Equal(t, "registration", metadata[0].Payload)
	require.Equal(t, "reservation", metadata[1].Payload)
	page, err := manager.ReadCommittedConsumerMetadata(config.ConsumerOffsetsTopicName, 0, 1, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, uint64(2), page[0].Offset)
	page, err = manager.ReadCommittedConsumerMetadata(config.ConsumerOffsetsTopicName, 0, 3, 1)
	require.NoError(t, err)
	require.Empty(t, page, "replication-uncommitted metadata must remain invisible")
	_, err = manager.ReadCommittedConsumerMetadata("orders", 0, 0, 1)
	require.ErrorContains(t, err, "cannot read topic")
}

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
