package fsm

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestReplicaMaterializationReadinessTracksLEOAndHWMConvergence(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	localTopic := topicManager.GetTopic("orders")
	require.NotNil(t, localTopic)
	partition, err := localTopic.GetPartition(0)
	require.NoError(t, err)

	state := NewBrokerFSM(topicManager, nil)
	definition := localTopic.Definition()
	state.mu.Lock()
	state.topicState["orders"] = &definition
	state.partitionMetadata["orders-0"] = &PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, LifecycleEpoch: definition.LifecycleEpoch,
		CommittedHWM: 2, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-2"},
	}
	state.mu.Unlock()

	err = state.ReplicaMaterializationReadinessError("broker-1")
	require.ErrorContains(t, err, "leo=0 hwm=0 committed_hwm=2")
	require.Len(t, state.ReplicaMaterializationIssues(), 1)

	require.NoError(t, partition.ReplicaAppendWithMode([]types.Message{
		{Offset: 0, Payload: "zero"},
		{Offset: 1, Payload: "one"},
	}, true))
	require.Error(t, state.ReplicaMaterializationReadinessError("broker-1"), "LEO alone must not make the replica ready")
	require.NoError(t, partition.ApplyReplicaHWM(2))
	partition.FlushDisk()

	require.NoError(t, state.ReplicaMaterializationReadinessError("broker-1"))
	require.Empty(t, state.ReplicaMaterializationIssues())
}

func TestReplicaMaterializationReadinessAllowsUncommittedLocalTail(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	localTopic := topicManager.GetTopic("orders")
	partition, err := localTopic.GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.ReplicaAppendWithMode([]types.Message{
		{Offset: 0, Payload: "zero"},
		{Offset: 1, Payload: "one"},
		{Offset: 2, Payload: "uncommitted"},
	}, true))
	require.NoError(t, partition.ApplyReplicaHWM(2))

	state := NewBrokerFSM(topicManager, nil)
	definition := localTopic.Definition()
	state.mu.Lock()
	state.topicState["orders"] = &definition
	state.partitionMetadata["orders-0"] = &PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, LifecycleEpoch: definition.LifecycleEpoch,
		CommittedHWM: 2, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1", "broker-2"},
	}
	state.mu.Unlock()

	require.NoError(t, state.ReplicaMaterializationReadinessError("broker-1"))
}

func TestReplicaMaterializationReadinessIgnoresUnassignedPartition(t *testing.T) {
	state := NewBrokerFSM(nil, nil)
	state.mu.Lock()
	state.topicState["orders"] = snapshotTopicDefinition("orders", 1)
	state.partitionMetadata["orders-0"] = &PartitionMetadata{
		Leader: "broker-2", LeaderEpoch: 1, LifecycleEpoch: topic.InitialLifecycleEpoch,
		CommittedHWM: 3, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"broker-2"}, ISR: []string{"broker-2"},
	}
	state.mu.Unlock()

	require.NoError(t, state.ReplicaMaterializationReadinessError("broker-1"))
}
