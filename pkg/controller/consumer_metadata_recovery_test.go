package controller

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func consumerMetadataMessage(t *testing.T, record coordinator.ConsumerMetadataRecord, offset uint64) types.Message {
	t.Helper()
	payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	return types.Message{Offset: offset, Payload: string(payload), Key: key}
}

func TestDistributedRecoveryExcludesUncommittedMetadataTail(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	diskManager := disk.NewDiskManager(cfg)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, topicManager)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	t.Cleanup(func() {
		for _, name := range topicManager.ListTopics() {
			for _, partition := range topicManager.GetTopic(name).Partitions {
				partition.Close()
			}
		}
		diskManager.CloseAllHandlers()
	})

	registration := coordinator.ConsumerMetadataRecord{
		Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration,
		Group: "workers", Topic: "orders", PartitionCount: 1, Epoch: 1, Timestamp: time.Unix(1, 0),
	}
	offset := coordinator.ConsumerMetadataRecord{
		Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot,
		Group: "workers", Topic: "orders", Epoch: 1, Revision: 1,
		Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: 10}}, Timestamp: time.Unix(2, 0),
	}
	partition, err := topicManager.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReplicaAppend([]types.Message{
		consumerMetadataMessage(t, registration, 0),
		consumerMetadataMessage(t, offset, 1),
	}))
	require.NoError(t, partition.ApplyReplicaHWM(2))
	offset.Revision = 2
	offset.Offsets[0].Offset = 20
	require.NoError(t, partition.ReplicaAppend([]types.Message{consumerMetadataMessage(t, offset, 2)}))
	require.Equal(t, uint64(2), partition.GetHWM())
	require.Equal(t, uint64(3), partition.NextOffset())

	require.NoError(t, c.ReloadDistributedConsumerMetadata())
	recovered, found := c.GetOffset("workers", "orders", 0)
	require.True(t, found)
	require.Equal(t, uint64(10), recovered)
}

type replicatedConsumerMetadata struct {
	messages []types.Message
}

func (*replicatedConsumerMetadata) Publish(string, *types.Message) error       { return nil }
func (*replicatedConsumerMetadata) CreateTopic(string, int, bool, bool) error  { return nil }
func (*replicatedConsumerMetadata) ExistingPartitionCount(string) (int, error) { return 4, nil }
func (h *replicatedConsumerMetadata) ReadTopicPartition(_ string, partition int, offset uint64, max int) ([]types.Message, error) {
	if partition != 0 {
		return nil, nil
	}
	result := make([]types.Message, 0, max)
	for _, message := range h.messages {
		if message.Offset >= offset {
			result = append(result, message)
		}
		if len(result) == max {
			break
		}
	}
	return result, nil
}

func TestDistributedDeleteReloadsGroupsOwnedByAnotherCoordinator(t *testing.T) {
	handler, state, _ := newDistributedLifecycleHandler(t)
	requestContext := NewClientContext("", 0)
	require.Contains(t, handler.HandleCommand("CREATE topic=orders partitions=1 replication_factor=1", requestContext), "OK topic=orders")

	metadata := &replicatedConsumerMetadata{}
	leader, err := coordinator.NewCoordinatorWithRecovery(context.Background(), handler.Config, metadata)
	require.NoError(t, err)
	t.Cleanup(leader.Stop)
	handler.Coordinator = leader
	state.SetCoordinator(leader)

	owner, err := coordinator.NewCoordinatorWithRecovery(context.Background(), handler.Config, metadata)
	require.NoError(t, err)
	t.Cleanup(owner.Stop)
	owner.SetOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
		metadata.messages = append(metadata.messages, consumerMetadataMessage(t, record, uint64(len(metadata.messages))))
		return nil
	})
	require.NoError(t, owner.RegisterGroup("orders", "workers", 1))
	_, err = owner.AddConsumer("workers", "member-a")
	require.NoError(t, err)
	require.Len(t, metadata.messages, 2)

	response := handler.HandleCommand("DELETE topic=orders", requestContext)
	require.Contains(t, response, "topic_delete_blocked")
	_, found := state.GetTopicDefinition("orders")
	require.True(t, found)
}
