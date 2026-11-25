package fsm

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func adminTestFSM(t *testing.T, broker2Status string) *BrokerFSM {
	t.Helper()
	state := newTestFSM()
	for _, broker := range []BrokerInfo{
		{ID: "broker-1", Addr: "broker-1:9001", Status: "active"},
		{ID: "broker-2", Addr: "broker-2:9001", Status: "active"},
		{ID: "broker-3", Addr: "broker-3:9001", Status: "active"},
	} {
		data, err := json.Marshal(broker)
		if err != nil {
			t.Fatal(err)
		}
		if result := state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}); result != nil {
			if applyErr, ok := result.(error); ok {
				t.Fatal(applyErr)
			}
		}
	}
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = 1
	definition.ReplicationFactor = 3
	command, err := json.Marshal(TopicCommand{Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	if result := state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), command...)}); result != nil {
		if applyErr, ok := result.(error); ok {
			t.Fatal(applyErr)
		}
	}
	metadata := PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 42,
		Replicas: []string{"broker-1", "broker-2", "broker-3"},
		ISR:      []string{"broker-1", "broker-2"}, LifecycleEpoch: topic.InitialLifecycleEpoch,
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	result := state.Apply(&raft.Log{Data: []byte("PARTITION:orders-0:" + string(data))})
	if applyErr, ok := result.(error); ok && applyErr != nil {
		t.Fatal(applyErr)
	}
	state.mu.Lock()
	state.brokers["broker-2"].Status = broker2Status
	state.mu.Unlock()
	return state
}

func validLeaderElectionCommand() LeaderElectionCommand {
	return LeaderElectionCommand{
		Topic: "orders", Partition: 0, Broker: "broker-2", ExpectedLeaderEpoch: 7,
		TopologyFenceVersion:       LeaderElectionTopologyFenceVersionCurrent,
		ExpectedLifecycleEpoch:     topic.InitialLifecycleEpoch,
		ExpectedDefinitionRevision: topic.InitialDefinitionRevision,
		ExpectedReplicas:           []string{"broker-1", "broker-2", "broker-3"},
		ExpectedTopicInstanceID:    new(string),
		RequireStableAssignment:    true,
	}
}

func applyLeaderElection(t *testing.T, state *BrokerFSM, command LeaderElectionCommand) interface{} {
	t.Helper()
	data, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return state.Apply(&raft.Log{Data: append([]byte("LEADER_ELECTION:"), data...)})
}

func TestLeaderElectionAdvancesEpochAndPreservesReplicationState(t *testing.T) {
	state := adminTestFSM(t, "active")
	result := applyLeaderElection(t, state, validLeaderElectionCommand())
	election, ok := result.(LeaderElectionResult)
	if !ok {
		t.Fatalf("unexpected result %T: %v", result, result)
	}
	if !election.Changed || election.PreviousLeader != "broker-1" || election.Leader != "broker-2" || election.LeaderEpoch != 8 {
		t.Fatalf("unexpected election result: %+v", election)
	}

	metadata := state.GetPartitionMetadata("orders-0")
	if metadata.Leader != "broker-2" || metadata.LeaderEpoch != 8 || metadata.CommittedHWM != 42 {
		t.Fatalf("unexpected metadata after election: %+v", metadata)
	}
	if strings.Join(metadata.Replicas, ",") != "broker-1,broker-2,broker-3" ||
		strings.Join(metadata.ISR, ",") != "broker-1,broker-2" {
		t.Fatalf("election changed replica state: %+v", metadata)
	}
}

func TestLeaderElectionRetryIsIdempotent(t *testing.T) {
	state := adminTestFSM(t, "active")
	command := validLeaderElectionCommand()
	if result := applyLeaderElection(t, state, command); result == nil {
		t.Fatal("initial election returned nil")
	}
	result := applyLeaderElection(t, state, command)
	election, ok := result.(LeaderElectionResult)
	if !ok || election.Changed || election.LeaderEpoch != 8 {
		t.Fatalf("retry was not idempotent: %T %+v", result, result)
	}
	if metadata := state.GetPartitionMetadata("orders-0"); metadata.LeaderEpoch != 8 {
		t.Fatalf("retry advanced leader epoch: %+v", metadata)
	}
}

func TestLeaderElectionReplaysLegacyCommandWithoutTopologyFields(t *testing.T) {
	state := adminTestFSM(t, "active")
	legacy := []byte(`{"topic":"orders","partition":0,"broker":"broker-2","expected_leader_epoch":7}`)
	result := state.Apply(&raft.Log{Data: append([]byte("LEADER_ELECTION:"), legacy...)})
	election, ok := result.(LeaderElectionResult)
	if !ok || !election.Changed || election.Leader != "broker-2" || election.LeaderEpoch != 8 {
		t.Fatalf("legacy leader election did not replay: %T %+v", result, result)
	}
}

func TestLeaderElectionReplaysV1WithOriginalStableAssignmentRule(t *testing.T) {
	const entry = `LEADER_ELECTION:{"topic":"orders","partition":0,"broker":"broker-2","expected_leader_epoch":7,"topology_fence_version":1,"expected_lifecycle_epoch":1,"expected_definition_revision":1,"expected_replicas":["broker-1","broker-2","broker-3"]}`
	state := adminTestFSM(t, "active")
	state.topicState["orders"].ReplicationFactor = 2
	err, ok := state.Apply(&raft.Log{Data: []byte(entry)}).(error)
	require.True(t, ok)
	require.ErrorContains(t, err, "stale replication factor")
	require.Equal(t, "broker-1", state.GetPartitionMetadata("orders-0").Leader)

	state.topicState["orders"].ReplicationFactor = 3
	election, ok := state.Apply(&raft.Log{Data: []byte(entry)}).(LeaderElectionResult)
	require.True(t, ok)
	require.True(t, election.Changed)
	require.Equal(t, "broker-2", election.Leader)
	require.Equal(t, 8, election.LeaderEpoch)
}

func TestLeaderElectionRejectsCommandForDeletedTopicInstance(t *testing.T) {
	state, _ := legacyUnderfilledReplicaState(t)
	definition := topic.DefaultDefinition("recreated-orders", nil)
	definition.Partitions = 1
	definition.InstanceID = rand.Text()
	create := func() {
		payload, err := json.Marshal(TopicCommand{Definition: &definition})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), payload...)}))
	}
	create()
	initial, original := state.GetLeaderElectionState(definition.Name, 0)
	require.NotNil(t, original)
	require.Len(t, initial.Replicas, 3)
	command := LeaderElectionCommand{
		Topic: definition.Name, Broker: initial.Replicas[1],
		ExpectedLeaderEpoch: initial.LeaderEpoch, TopologyFenceVersion: LeaderElectionTopologyFenceVersionCurrent,
		ExpectedLifecycleEpoch: initial.LifecycleEpoch, ExpectedDefinitionRevision: original.Revision,
		ExpectedReplicas: initial.Replicas, ExpectedTopicInstanceID: &original.InstanceID,
	}

	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"recreated-orders"}`)}))
	require.Nil(t, state.GetPartitionMetadata("recreated-orders-0"))
	definition.InstanceID = rand.Text()
	create()
	recreated, current := state.GetLeaderElectionState(definition.Name, 0)
	require.NotNil(t, current)
	require.NotEqual(t, original.InstanceID, current.InstanceID)
	require.Equal(t, original.Revision, current.Revision)
	require.Equal(t, initial, recreated)

	err, ok := applyLeaderElection(t, state, command).(error)
	require.True(t, ok, "a stale election must fail even when all other fences match")
	require.ErrorContains(t, err, "stale topic instance")
	require.Equal(t, recreated, state.GetPartitionMetadata("recreated-orders-0"))

	command.ExpectedTopicInstanceID = &current.InstanceID
	election, ok := applyLeaderElection(t, state, command).(LeaderElectionResult)
	require.True(t, ok)
	require.True(t, election.Changed)
	require.Equal(t, command.Broker, election.Leader)
}

func TestLeaderElectionRejectsInvalidTopologyFenceEncoding(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*LeaderElectionCommand)
		message string
	}{
		{
			name: "unversioned command with fence fields",
			mutate: func(command *LeaderElectionCommand) {
				command.TopologyFenceVersion = 0
			},
			message: "unversioned leader election contains topology fence fields",
		},
		{
			name: "missing topic instance field",
			mutate: func(command *LeaderElectionCommand) {
				command.ExpectedTopicInstanceID = nil
			},
			message: "expected topic instance ID is required",
		},
		{
			name: "unsupported fence version",
			mutate: func(command *LeaderElectionCommand) {
				command.TopologyFenceVersion++
			},
			message: "unsupported leader election topology fence version",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := adminTestFSM(t, "active")
			command := validLeaderElectionCommand()
			test.mutate(&command)
			result := applyLeaderElection(t, state, command)
			err, ok := result.(error)
			if !ok || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected %q, got %T %v", test.message, result, result)
			}
		})
	}
}

func TestLeaderElectionRejectsUnsafeTargetsAndStaleEpoch(t *testing.T) {
	tests := []struct {
		name          string
		broker2Status string
		command       LeaderElectionCommand
		want          string
	}{
		{
			name: "inactive broker", broker2Status: "inactive",
			command: validLeaderElectionCommand(),
			want:    "not active",
		},
		{
			name: "outside ISR", broker2Status: "active",
			command: func() LeaderElectionCommand {
				command := validLeaderElectionCommand()
				command.Broker = "broker-3"
				return command
			}(),
			want: "not in the ISR",
		},
		{
			name: "stale epoch", broker2Status: "active",
			command: func() LeaderElectionCommand {
				command := validLeaderElectionCommand()
				command.ExpectedLeaderEpoch = 6
				return command
			}(),
			want: "stale leader epoch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := adminTestFSM(t, test.broker2Status)
			result := applyLeaderElection(t, state, test.command)
			err, ok := result.(error)
			if !ok || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q, got %T %v", test.want, result, result)
			}
			metadata := state.GetPartitionMetadata("orders-0")
			if metadata.Leader != "broker-1" || metadata.LeaderEpoch != 7 || metadata.CommittedHWM != 42 {
				t.Fatalf("rejected election mutated metadata: %+v", metadata)
			}
		})
	}
}

func TestLeaderElectionRejectsStaleTopologyBeforeIdempotentSuccess(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*BrokerFSM)
		want   string
	}{
		{
			name: "topic lifecycle changed",
			mutate: func(state *BrokerFSM) {
				state.topicState["orders"].LifecycleEpoch++
			},
			want: "stale lifecycle epoch",
		},
		{
			name: "partition lifecycle changed",
			mutate: func(state *BrokerFSM) {
				state.partitionMetadata["orders-0"].LifecycleEpoch++
			},
			want: "stale lifecycle epoch",
		},
		{
			name: "replica order changed",
			mutate: func(state *BrokerFSM) {
				state.partitionMetadata["orders-0"].Replicas = []string{"broker-2", "broker-1", "broker-3"}
			},
			want: "stale replica assignment",
		},
		{
			name: "topic definition changed",
			mutate: func(state *BrokerFSM) {
				state.topicState["orders"].Revision++
			},
			want: "stale definition revision",
		},
		{
			name: "replication factor changed",
			mutate: func(state *BrokerFSM) {
				state.topicState["orders"].ReplicationFactor--
			},
			want: "stale replication factor",
		},
		{
			name: "replica recovery started",
			mutate: func(state *BrokerFSM) {
				state.partitionMetadata["orders-0"].RecoveryReplicas = []string{"broker-3"}
			},
			want: "replica recovery pending",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := adminTestFSM(t, "active")
			command := validLeaderElectionCommand()
			if result := applyLeaderElection(t, state, command); result == nil {
				t.Fatal("initial election returned nil")
			}

			state.mu.Lock()
			test.mutate(state)
			state.mu.Unlock()

			result := applyLeaderElection(t, state, command)
			err, ok := result.(error)
			if !ok || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q, got %T %v", test.want, result, result)
			}
		})
	}
}

func TestLeaderElectionRejectsMissingPartition(t *testing.T) {
	state := adminTestFSM(t, "active")
	result := applyLeaderElection(t, state, LeaderElectionCommand{
		Topic: "missing", Partition: 1, Broker: "broker-2", ExpectedLeaderEpoch: 1,
		TopologyFenceVersion:       LeaderElectionTopologyFenceVersionCurrent,
		ExpectedLifecycleEpoch:     topic.InitialLifecycleEpoch,
		ExpectedDefinitionRevision: topic.InitialDefinitionRevision,
		ExpectedReplicas:           []string{"broker-1", "broker-2"},
		ExpectedTopicInstanceID:    new(string),
	})
	err, ok := result.(error)
	if !ok || !strings.Contains(err.Error(), fmt.Sprintf("%s-%d", "missing", 1)) {
		t.Fatalf("unexpected missing partition result: %T %v", result, result)
	}
}
