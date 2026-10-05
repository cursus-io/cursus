package fsm

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestReplicaReassignmentExpandsLegacyAssignmentWithoutAdmittingISR(t *testing.T) {
	state, command := legacyUnderfilledReplicaState(t)
	payload, err := json.Marshal(command)
	require.NoError(t, err)

	require.Nil(t, state.Apply(&raft.Log{Index: 20, Data: append([]byte("REPLICA_REASSIGN:"), payload...)}))
	metadata := state.GetPartitionMetadata("orders-0")
	require.Equal(t, command.TargetReplicas, metadata.Replicas)
	require.Equal(t, []string{command.Leader}, metadata.ISR, "new replicas must catch up before ISR admission")
	require.Equal(t, command.Leader, metadata.Leader)
	require.Equal(t, command.LeaderEpoch, metadata.LeaderEpoch)

	require.Nil(t, state.Apply(&raft.Log{Index: 21, Data: append([]byte("REPLICA_REASSIGN:"), payload...)}), "replay must be idempotent")
	require.Equal(t, command.TargetReplicas, state.GetPartitionMetadata("orders-0").Replicas)
}

func TestReplicaReassignmentRejectsUnsafeOrStaleExpansion(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*ReplicaReassignmentCommand)
		want   string
	}{
		{name: "stale leader", mutate: func(command *ReplicaReassignmentCommand) { command.LeaderEpoch++ }, want: "stale leader fence"},
		{name: "stale lifecycle", mutate: func(command *ReplicaReassignmentCommand) { command.LifecycleEpoch++ }, want: "stale lifecycle epoch"},
		{name: "stale replicas", mutate: func(command *ReplicaReassignmentCommand) { command.ExpectedReplicas = []string{"n2"} }, want: "stale replica assignment"},
		{name: "duplicate target", mutate: func(command *ReplicaReassignmentCommand) { command.TargetReplicas[2] = command.TargetReplicas[1] }, want: "duplicate broker"},
		{name: "unknown target", mutate: func(command *ReplicaReassignmentCommand) { command.TargetReplicas[2] = "unknown" }, want: "not a durable active broker"},
		{name: "missing leader", mutate: func(command *ReplicaReassignmentCommand) {
			command.TargetReplicas = command.TargetReplicas[:0]
			for _, brokerID := range []string{"n1", "n2", "n3", "n4"} {
				if brokerID != command.Leader && len(command.TargetReplicas) < 3 {
					command.TargetReplicas = append(command.TargetReplicas, brokerID)
				}
			}
		}, want: "do not contain current leader"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			state, command := legacyUnderfilledReplicaState(t)
			testCase.mutate(&command)
			payload, err := json.Marshal(command)
			require.NoError(t, err)
			result := state.Apply(&raft.Log{Index: 20, Data: append([]byte("REPLICA_REASSIGN:"), payload...)})
			resultErr, ok := result.(error)
			require.True(t, ok, "expected reassignment error, got %T", result)
			require.ErrorContains(t, resultErr, testCase.want)
			metadata := state.GetPartitionMetadata("orders-0")
			require.Equal(t, []string{metadata.Leader}, metadata.Replicas)
			require.Equal(t, []string{metadata.Leader}, metadata.ISR)
		})
	}
}

func TestReplicaReassignmentCannotRemoveExistingReplica(t *testing.T) {
	state, command := legacyUnderfilledReplicaState(t)
	metadata := state.GetPartitionMetadata("orders-0")
	second := command.TargetReplicas[1]
	metadata.Replicas = []string{command.Leader, second}
	metadata.ISR = []string{command.Leader}
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Index: 19, Data: []byte("PARTITION:orders-0:" + string(encoded))}))

	command.ExpectedReplicas = []string{command.Leader, second}
	command.TargetReplicas = []string{command.Leader}
	for _, brokerID := range []string{"n1", "n2", "n3", "n4"} {
		if brokerID != command.Leader && brokerID != second && len(command.TargetReplicas) < 3 {
			command.TargetReplicas = append(command.TargetReplicas, brokerID)
		}
	}
	payload, err := json.Marshal(command)
	require.NoError(t, err)
	result := state.Apply(&raft.Log{Index: 20, Data: append([]byte("REPLICA_REASSIGN:"), payload...)})
	resultErr, ok := result.(error)
	require.True(t, ok)
	require.ErrorContains(t, resultErr, "cannot remove current replica")
	require.Equal(t, []string{command.Leader, second}, state.GetPartitionMetadata("orders-0").Replicas)
}

func legacyUnderfilledReplicaState(t *testing.T) (*BrokerFSM, ReplicaReassignmentCommand) {
	t.Helper()
	state := newTestFSM()
	for index, brokerID := range []string{"n1", "n2", "n3", "n4"} {
		payload, err := json.Marshal(BrokerInfo{
			ID: brokerID, Addr: fmt.Sprintf("127.0.0.1:%d", 9001+index), Status: "active",
			LifecycleProtocol: BrokerProtocolVersionCurrent,
		})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Index: uint64(index + 1), Data: append([]byte("REGISTER:"), payload...)}))
	}
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = 1
	definition.ReplicationFactor = 3
	topicPayload, err := json.Marshal(TopicCommand{Definition: &definition})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Index: 10, Data: append([]byte("TOPIC:"), topicPayload...)}))

	metadata := state.GetPartitionMetadata("orders-0")
	metadata.Replicas = []string{metadata.Leader}
	metadata.ISR = []string{metadata.Leader}
	partitionPayload, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Index: 11, Data: []byte("PARTITION:orders-0:" + string(partitionPayload))}))

	otherBrokers := []string{"n1", "n2", "n3", "n4"}
	sort.Strings(otherBrokers)
	target := []string{metadata.Leader}
	for _, brokerID := range otherBrokers {
		if brokerID != metadata.Leader && len(target) < definition.ReplicationFactor {
			target = append(target, brokerID)
		}
	}
	return state, ReplicaReassignmentCommand{
		Topic: "orders", Partition: 0, LifecycleEpoch: metadata.LifecycleEpoch,
		Leader: metadata.Leader, LeaderEpoch: metadata.LeaderEpoch,
		ExpectedReplicas: []string{metadata.Leader}, TargetReplicas: target,
	}
}
