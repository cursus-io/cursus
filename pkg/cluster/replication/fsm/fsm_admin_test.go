package fsm

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

func adminTestFSM(t *testing.T, broker2Status string) *BrokerFSM {
	t.Helper()
	state := newTestFSM()
	for _, broker := range []BrokerInfo{
		{ID: "broker-1", Addr: "broker-1:9001", Status: "active"},
		{ID: "broker-2", Addr: "broker-2:9001", Status: broker2Status},
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
	metadata := PartitionMetadata{
		Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 42,
		Replicas: []string{"broker-1", "broker-2", "broker-3"},
		ISR:      []string{"broker-1", "broker-2"},
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	result := state.Apply(&raft.Log{Data: []byte("PARTITION:orders-0:" + string(data))})
	if applyErr, ok := result.(error); ok && applyErr != nil {
		t.Fatal(applyErr)
	}
	return state
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
	result := applyLeaderElection(t, state, LeaderElectionCommand{
		Topic: "orders", Partition: 0, Broker: "broker-2", ExpectedLeaderEpoch: 7,
	})
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
	command := LeaderElectionCommand{Topic: "orders", Partition: 0, Broker: "broker-2", ExpectedLeaderEpoch: 7}
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

func TestLeaderElectionRejectsUnsafeTargetsAndStaleEpoch(t *testing.T) {
	tests := []struct {
		name          string
		broker2Status string
		command       LeaderElectionCommand
		want          string
	}{
		{
			name: "inactive broker", broker2Status: "inactive",
			command: LeaderElectionCommand{Topic: "orders", Partition: 0, Broker: "broker-2", ExpectedLeaderEpoch: 7},
			want:    "not active",
		},
		{
			name: "outside ISR", broker2Status: "active",
			command: LeaderElectionCommand{Topic: "orders", Partition: 0, Broker: "broker-3", ExpectedLeaderEpoch: 7},
			want:    "not in the ISR",
		},
		{
			name: "stale epoch", broker2Status: "active",
			command: LeaderElectionCommand{Topic: "orders", Partition: 0, Broker: "broker-2", ExpectedLeaderEpoch: 6},
			want:    "stale leader epoch",
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

func TestLeaderElectionRejectsMissingPartition(t *testing.T) {
	state := adminTestFSM(t, "active")
	result := applyLeaderElection(t, state, LeaderElectionCommand{
		Topic: "missing", Partition: 1, Broker: "broker-2", ExpectedLeaderEpoch: 1,
	})
	err, ok := result.(error)
	if !ok || !strings.Contains(err.Error(), fmt.Sprintf("%s-%d", "missing", 1)) {
		t.Fatalf("unexpected missing partition result: %T %v", result, result)
	}
}
