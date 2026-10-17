package fsm

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerRouteFencesDelayedWritesBeforeCommit(t *testing.T) {
	for _, change := range []string{"owner", "epoch", "count", "lifecycle", "physical"} {
		t.Run(change, func(t *testing.T) {
			f := NewBrokerFSM(nil, nil)
			f.brokers["a"] = &BrokerInfo{ID: "a", Addr: "a:7000", Status: "active"}
			for i := 0; i < 4; i++ {
				f.partitionMetadata[fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, i)] = &PartitionMetadata{Leader: "a", LeaderEpoch: 1, LifecycleEpoch: 1, PartitionCount: 4}
			}
			routes, addresses, err := f.ConsumerMetadataRoutes([]string{"workers"})
			require.NoError(t, err)
			route := routes["workers"]
			require.Equal(t, "a:7000", addresses["workers"])
			require.NoError(t, f.ValidateConsumerMetadataRoute(config.ConsumerOffsetsTopicName, route.Partition, &route))
			meta := f.partitionMetadata[fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, route.Partition)]
			switch change {
			case "owner":
				meta.Leader = "b"
			case "epoch":
				meta.LeaderEpoch++
			case "count":
				for _, value := range f.partitionMetadata {
					value.PartitionCount++
				}
			case "lifecycle":
				for _, value := range f.partitionMetadata {
					value.LifecycleEpoch++
				}
			case "physical":
				route.Partition = (route.Partition + 1) % 4
			}
			// Even a command restamped with current authority cannot launder the origin.
			command := partitionCommitCommand{Topic: config.ConsumerOffsetsTopicName, Partition: routes["workers"].Partition, Leader: meta.Leader, LeaderEpoch: meta.LeaderEpoch, LifecycleEpoch: meta.LifecycleEpoch, HWM: 9, ConsumerRoute: &route}
			data, err := json.Marshal(command)
			require.NoError(t, err)
			result := f.applyPartitionCommitCommand(string(data))
			require.ErrorIs(t, result.(error), ErrPartitionCommitFenced)
			require.Zero(t, meta.CommittedHWM)
			ack := f.applyMessageBatch(&types.MessageCommand{Topic: command.Topic, Partition: command.Partition, ConsumerRoute: &route, Messages: []types.Message{{Payload: "delayed"}}})
			require.Equal(t, "ERROR", ack.(types.AckResponse).Status)
		})
	}
}

func TestConsumerRouteLegacyAndConsistentCapture(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	require.NoError(t, f.ValidateConsumerMetadataRoute(config.ConsumerOffsetsTopicName, 0, nil))
	_, _, err := f.ConsumerMetadataRoutes([]string{"workers"})
	require.Error(t, err)
	f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"] = &PartitionMetadata{Leader: "a", LeaderEpoch: 1, PartitionCount: 1, LifecycleEpoch: 1}
	f.brokers["a"] = &BrokerInfo{ID: "a", Status: "inactive"}
	_, _, err = f.ConsumerMetadataRoutes([]string{"workers"})
	require.Error(t, err)
	f.brokers["a"].Status = "active"
	routes, _, err := f.ConsumerMetadataRoutes([]string{"workers"})
	require.NoError(t, err)
	route := routes["workers"]
	data, err := json.Marshal(partitionCommitCommand{Topic: config.ConsumerOffsetsTopicName, Partition: 0, Leader: "a", LeaderEpoch: 1, LifecycleEpoch: 1, HWM: 3, ConsumerRoute: &route})
	require.NoError(t, err)
	require.Nil(t, f.applyPartitionCommitCommand(string(data)))
	require.Equal(t, uint64(3), f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"].CommittedHWM)
}
