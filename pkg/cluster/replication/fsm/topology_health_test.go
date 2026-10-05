package fsm

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestEvaluateTopologyRejectsDefinitionAssignmentMismatch(t *testing.T) {
	state := newTestFSM()
	for _, brokerID := range []string{"broker-1", "broker-2", "broker-3"} {
		registerTopologyHealthBroker(t, state, brokerID, "active")
	}
	require.Nil(t, state.Apply(&raft.Log{Data: topicCommandData(t, testTopicCommand("orders", 1, 3)), Index: 4}))

	state.mu.Lock()
	state.partitionMetadata["orders-0"].Replicas = []string{"broker-1"}
	state.partitionMetadata["orders-0"].ISR = []string{"broker-1"}
	state.partitionMetadata["orders-0"].Leader = "broker-1"
	state.mu.Unlock()

	health := state.EvaluateTopology(2)
	require.False(t, health.Healthy)
	require.Equal(t, 1, health.AssignmentDeficient)
	require.Equal(t, 1, health.UnderReplicated)
	require.Equal(t, 1, health.MinISRUnsatisfied)
	require.Error(t, health.ReadinessError())
}

func TestEvaluateTopologyReportsInactiveReplicaAndLeader(t *testing.T) {
	state := newTestFSM()
	registerTopologyHealthBroker(t, state, "broker-1", "active")
	registerTopologyHealthBroker(t, state, "broker-2", "active")
	command := testTopicCommand("orders", 1, 2)
	require.Nil(t, state.Apply(&raft.Log{Data: topicCommandData(t, command), Index: 3}))

	state.mu.Lock()
	state.brokers["broker-1"].Status = "inactive"
	state.partitionMetadata["orders-0"].Leader = "broker-1"
	state.partitionMetadata["orders-0"].Replicas = []string{"broker-1", "broker-2"}
	state.partitionMetadata["orders-0"].ISR = []string{"broker-1", "broker-2"}
	state.mu.Unlock()

	health := state.EvaluateTopology(1)
	require.False(t, health.Healthy)
	require.Equal(t, 1, health.Offline)
	require.Equal(t, 1, health.InactiveReplicaPartitions)
	require.Equal(t, 1, health.InactiveReplicas)
	require.Equal(t, 1, health.UnderReplicated)
	require.False(t, health.Partitions[0].LeaderAvailable)
	require.Contains(t, health.Partitions[0].Reasons, "leader_inactive")
	require.Error(t, health.ReadinessError())
}

func TestEvaluateTopologyReadinessAllowsDegradedReplicaAboveMinISR(t *testing.T) {
	state := newTestFSM()
	for _, brokerID := range []string{"broker-1", "broker-2", "broker-3"} {
		registerTopologyHealthBroker(t, state, brokerID, "active")
	}
	require.Nil(t, state.Apply(&raft.Log{Data: topicCommandData(t, testTopicCommand("orders", 1, 3)), Index: 4}))

	state.mu.Lock()
	state.brokers["broker-3"].Status = "inactive"
	state.mu.Unlock()

	health := state.EvaluateTopology(2)
	require.False(t, health.Healthy, "full-replica health must still report degradation")
	require.Equal(t, 1, health.UnderReplicated)
	require.Equal(t, 1, health.InactiveReplicaPartitions)
	require.Zero(t, health.Offline)
	require.Zero(t, health.AssignmentDeficient)
	require.Zero(t, health.MinISRUnsatisfied)
	require.NoError(t, health.ReadinessError(), "a writable min-ISR quorum must stay in service")

	belowMinISR := state.EvaluateTopology(3)
	require.Equal(t, 1, belowMinISR.MinISRUnsatisfied)
	require.Error(t, belowMinISR.ReadinessError(), "readiness must fail when the durability quorum is unavailable")
}

func TestEvaluateTopologyReportsMissingPartitionMetadata(t *testing.T) {
	state := newTestFSM()
	registerTopologyHealthBroker(t, state, "broker-1", "active")
	require.Nil(t, state.Apply(&raft.Log{Data: topicCommandData(t, testTopicCommand("orders", 2, 1)), Index: 2}))

	state.mu.Lock()
	delete(state.partitionMetadata, "orders-1")
	state.mu.Unlock()

	health := state.EvaluateTopology(1)
	require.False(t, health.Healthy)
	require.Equal(t, 2, health.PartitionCount)
	require.Equal(t, 1, health.AssignmentDeficient)
	require.Contains(t, health.Partitions[1].Reasons, "missing_partition_metadata")
}

func TestEvaluateTopologyAcceptsConvergedAssignment(t *testing.T) {
	state := newTestFSM()
	for _, brokerID := range []string{"broker-1", "broker-2", "broker-3"} {
		registerTopologyHealthBroker(t, state, brokerID, "active")
	}
	require.Nil(t, state.Apply(&raft.Log{Data: topicCommandData(t, testTopicCommand("orders", 1, 3)), Index: 4}))

	health := state.EvaluateTopology(2)
	require.True(t, health.Healthy)
	require.NoError(t, health.ReadinessError())
	require.Equal(t, 3, health.Partitions[0].ExpectedReplicas)
	require.Equal(t, 3, health.Partitions[0].ActiveReplicas)
	require.Equal(t, 3, health.Partitions[0].InSyncReplicas)
}

func registerTopologyHealthBroker(t *testing.T, state *BrokerFSM, brokerID, status string) {
	t.Helper()
	payload, err := json.Marshal(BrokerInfo{
		ID: brokerID, Addr: brokerID + ":9001", Status: status,
		LifecycleProtocol: BrokerProtocolVersionCurrent,
	})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...)}))
}

func topicCommandData(t *testing.T, command TopicCommand) []byte {
	t.Helper()
	payload, err := json.Marshal(command)
	require.NoError(t, err)
	return append([]byte("TOPIC:"), payload...)
}
