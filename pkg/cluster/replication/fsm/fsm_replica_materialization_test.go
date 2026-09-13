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

func TestReplicaMaterializationIssueSnapshotIsDetachedAndSorted(t *testing.T) {
	var nilState *BrokerFSM
	require.Nil(t, nilState.ReplicaMaterializationIssues())
	require.NoError(t, nilState.ReconcileReplicaMaterializations("broker-1"))

	state := NewBrokerFSM(nil, nil)
	state.mu.Lock()
	state.replicaMaterialization["zeta-1"] = ReplicaMaterializationIssue{Topic: "zeta", Partition: 1}
	state.replicaMaterialization["alpha-2"] = ReplicaMaterializationIssue{Topic: "alpha", Partition: 2}
	state.replicaMaterialization["alpha-0"] = ReplicaMaterializationIssue{Topic: "alpha", Partition: 0}
	state.mu.Unlock()

	issues := state.ReplicaMaterializationIssues()
	require.Equal(t, "alpha", issues[0].Topic)
	require.Equal(t, 0, issues[0].Partition)
	require.Equal(t, "alpha", issues[1].Topic)
	require.Equal(t, 2, issues[1].Partition)
	require.Equal(t, "zeta", issues[2].Topic)
	require.Equal(t, 1, issues[2].Partition)
	issues[0].Error = "mutated"
	require.Empty(t, state.ReplicaMaterializationIssues()[0].Error)
	require.NoError(t, state.ReconcileReplicaMaterializations(""))
}

func TestReplicaMaterializationReconcileReportsFailClosedCauses(t *testing.T) {
	const brokerID = "broker-1"

	t.Run("missing authoritative definition", func(t *testing.T) {
		state := NewBrokerFSM(nil, nil)
		installReplicaMetadata(state, "orders-0", brokerID, topic.InitialLifecycleEpoch, 1)
		require.ErrorContains(t, state.ReconcileReplicaMaterializations(brokerID), "authoritative topic definition is unavailable")
		require.Contains(t, state.ReplicaMaterializationIssues()[0].Error, "definition is unavailable")
	})

	t.Run("lifecycle mismatch", func(t *testing.T) {
		state := NewBrokerFSM(nil, nil)
		definition := topic.DefaultDefinition("orders", config.DefaultConfig())
		state.mu.Lock()
		state.topicState["orders"] = &definition
		state.mu.Unlock()
		installReplicaMetadata(state, "orders-0", brokerID, definition.LifecycleEpoch+1, 1)
		require.ErrorContains(t, state.ReconcileReplicaMaterializations(brokerID), "topic lifecycle mismatch")
	})

	t.Run("topic manager unavailable", func(t *testing.T) {
		state := NewBrokerFSM(nil, nil)
		definition := topic.DefaultDefinition("orders", config.DefaultConfig())
		state.mu.Lock()
		state.topicState["orders"] = &definition
		state.mu.Unlock()
		installReplicaMetadata(state, "orders-0", brokerID, definition.LifecycleEpoch, 1)
		require.ErrorContains(t, state.ReconcileReplicaMaterializations(brokerID), "topic manager is unavailable")
	})

	t.Run("local topic unavailable", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.LogDir = t.TempDir()
		diskManager := disk.NewDiskManager(cfg)
		t.Cleanup(diskManager.CloseAllHandlers)
		state := NewBrokerFSM(topic.NewTopicManager(cfg, diskManager, nil), nil)
		definition := topic.DefaultDefinition("orders", cfg)
		state.mu.Lock()
		state.topicState["orders"] = &definition
		state.mu.Unlock()
		installReplicaMetadata(state, "orders-0", brokerID, definition.LifecycleEpoch, 1)
		require.ErrorContains(t, state.ReconcileReplicaMaterializations(brokerID), "local topic is not materialized")
	})

	t.Run("local topic lifecycle stale", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.LogDir = t.TempDir()
		diskManager := disk.NewDiskManager(cfg)
		t.Cleanup(diskManager.CloseAllHandlers)
		topicManager := topic.NewTopicManager(cfg, diskManager, nil)
		require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
		state := NewBrokerFSM(topicManager, nil)
		definition := topicManager.GetTopic("orders").Definition()
		definition.LifecycleEpoch++
		state.mu.Lock()
		state.topicState["orders"] = &definition
		state.mu.Unlock()
		installReplicaMetadata(state, "orders-0", brokerID, definition.LifecycleEpoch, 1)
		require.ErrorContains(t, state.ReconcileReplicaMaterializations(brokerID), "local topic lifecycle is stale")
	})

	t.Run("partition unavailable", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.LogDir = t.TempDir()
		diskManager := disk.NewDiskManager(cfg)
		t.Cleanup(diskManager.CloseAllHandlers)
		topicManager := topic.NewTopicManager(cfg, diskManager, nil)
		require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
		state := NewBrokerFSM(topicManager, nil)
		definition := topicManager.GetTopic("orders").Definition()
		state.mu.Lock()
		state.topicState["orders"] = &definition
		state.mu.Unlock()
		installReplicaMetadata(state, "orders-1", brokerID, definition.LifecycleEpoch, 1)
		require.Error(t, state.ReconcileReplicaMaterializations(brokerID))
	})

	t.Run("local HWM ahead", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.LogDir = t.TempDir()
		diskManager := disk.NewDiskManager(cfg)
		t.Cleanup(diskManager.CloseAllHandlers)
		topicManager := topic.NewTopicManager(cfg, diskManager, nil)
		require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
		partition, err := topicManager.GetTopic("orders").GetPartition(0)
		require.NoError(t, err)
		require.NoError(t, partition.ReplicaAppendWithMode([]types.Message{{Offset: 0, Payload: "committed"}}, true))
		require.NoError(t, partition.ApplyReplicaHWM(1))

		state := NewBrokerFSM(topicManager, nil)
		definition := topicManager.GetTopic("orders").Definition()
		state.mu.Lock()
		state.topicState["orders"] = &definition
		state.mu.Unlock()
		installReplicaMetadata(state, "orders-0", brokerID, definition.LifecycleEpoch, 0)
		require.ErrorContains(t, state.ReconcileReplicaMaterializations(brokerID), "local HWM 1 is ahead of committed HWM 0")
	})
}

func TestSplitPartitionMetadataKeyRejectsMalformedKeys(t *testing.T) {
	topicName, partition, ok := splitPartitionMetadataKey("orders-12")
	require.True(t, ok)
	require.Equal(t, "orders", topicName)
	require.Equal(t, 12, partition)
	for _, key := range []string{"orders", "-1", "orders-", "orders-invalid"} {
		_, _, ok := splitPartitionMetadataKey(key)
		require.False(t, ok, key)
	}

	state := NewBrokerFSM(nil, nil)
	installReplicaMetadata(state, "malformed", "broker-1", topic.InitialLifecycleEpoch, 1)
	require.NoError(t, state.ReconcileReplicaMaterializations("broker-1"))
	require.Empty(t, state.ReplicaMaterializationIssues())
}

func installReplicaMetadata(state *BrokerFSM, key, brokerID string, lifecycleEpoch, committedHWM uint64) {
	state.mu.Lock()
	state.partitionMetadata[key] = &PartitionMetadata{
		Leader: "leader", LeaderEpoch: 3, LifecycleEpoch: lifecycleEpoch,
		CommittedHWM: committedHWM, CommittedHWMKnown: true, PartitionCount: 2,
		Replicas: []string{brokerID}, ISR: []string{brokerID},
	}
	state.mu.Unlock()
}
