package controller

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/hashicorp/raft"
)

func TestClusterObservationCommandRoutingContract(t *testing.T) {
	ch := NewCommandHandler(nil, &config.Config{}, nil, nil, nil)
	t.Cleanup(func() { _ = ch.Close() })

	tests := []struct {
		command string
		want    string
	}{
		{command: "LIST_CLUSTER", want: "ERROR: distribution_not_enabled"},
		{command: "  list_cluster  ", want: "ERROR: distribution_not_enabled"},
		{command: "CLUSTER_STATUS", want: "ERROR: distribution_required command=CLUSTER_STATUS"},
		{command: "cluster_status", want: "ERROR: distribution_required command=CLUSTER_STATUS"},
	}
	for _, test := range tests {
		if got := ch.HandleCommand(test.command, NewClientContext("", 0)); got != test.want {
			t.Errorf("HandleCommand(%q) = %q, want %q", test.command, got, test.want)
		}
	}

	for _, command := range []string{"LIST_CLUSTER extra=x", "CLUSTER_STATUS extra=x"} {
		if got := ch.HandleCommand(command, NewClientContext("", 0)); !strings.Contains(got, "unknown_command") {
			t.Errorf("exact command %q accepted extra arguments: %q", command, got)
		}
	}
}

func TestListClusterResponseContract(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	broker := fsm.BrokerInfo{ID: "broker-1", Addr: "broker-1:9001", ClientAddr: "broker-1:9000", Status: "active"}
	data, err := json.Marshal(broker)
	if err != nil {
		t.Fatal(err)
	}
	state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)})

	rm := &MockRaftManagerForForward{state: state}
	ch := NewCommandHandler(nil, &config.Config{EnabledDistribution: true}, nil, nil, &clusterController.ClusterController{RaftManager: rm})
	t.Cleanup(func() { _ = ch.Close() })

	const prefix = "OK brokers="
	response := ch.HandleCommand("LIST_CLUSTER", NewClientContext("", 0))
	if !strings.HasPrefix(response, prefix) {
		t.Fatalf("unexpected LIST_CLUSTER response: %s", response)
	}
	var brokers []fsm.BrokerInfo
	if err := json.Unmarshal([]byte(strings.TrimPrefix(response, prefix)), &brokers); err != nil {
		t.Fatalf("invalid broker JSON: %v", err)
	}
	if len(brokers) != 1 || brokers[0] != broker {
		t.Fatalf("broker response changed: %+v", brokers)
	}
}

func TestListClusterReportsMissingFSM(t *testing.T) {
	rm := &MockRaftManagerForForward{}
	ch := NewCommandHandler(nil, &config.Config{EnabledDistribution: true}, nil, nil, &clusterController.ClusterController{RaftManager: rm})
	t.Cleanup(func() { _ = ch.Close() })

	if got := ch.HandleCommand("LIST_CLUSTER", NewClientContext("", 0)); got != "ERROR: fsm_not_available" {
		t.Fatalf("unexpected missing FSM response: %q", got)
	}
}

func TestClusterStatusExposesLocalRaftSnapshotProgress(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	rm := &MockRaftManagerForForward{
		state: state,
		raftStatus: replication.RaftStatus{
			State: "Follower", AppliedIndex: 41, CommitIndex: 42, LastLogIndex: 43,
			LastSnapshotIndex: 40, LastSnapshotTerm: 3,
		},
	}
	ch := NewCommandHandler(nil, &config.Config{EnabledDistribution: true}, nil, nil, &clusterController.ClusterController{RaftManager: rm})
	t.Cleanup(func() { _ = ch.Close() })

	response := ch.HandleCommand("CLUSTER_STATUS", NewClientContext("", 0))
	const prefix = "OK cluster="
	if !strings.HasPrefix(response, prefix) {
		t.Fatalf("unexpected CLUSTER_STATUS response: %s", response)
	}
	var status clusterStatus
	if err := json.Unmarshal([]byte(strings.TrimPrefix(response, prefix)), &status); err != nil {
		t.Fatalf("invalid cluster status JSON: %v", err)
	}
	if status.RaftState != "Follower" || status.RaftAppliedIndex != 41 ||
		status.RaftCommitIndex != 42 || status.RaftLastLogIndex != 43 ||
		status.RaftLastSnapshotIndex != 40 || status.RaftLastSnapshotTerm != 3 {
		t.Fatalf("unexpected local Raft status: %+v", status)
	}
}

func TestClusterStatusPreservesDiagnosticsWhenLocalRaftStatsAreUnavailable(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	rm := &MockRaftManagerForForward{
		state:         state,
		raftStatusErr: fmt.Errorf("parse raft stat %q", "last_snapshot_index"),
	}
	ch := NewCommandHandler(nil, &config.Config{EnabledDistribution: true}, nil, nil, &clusterController.ClusterController{RaftManager: rm})
	t.Cleanup(func() { _ = ch.Close() })

	response := ch.HandleCommand("CLUSTER_STATUS", NewClientContext("", 0))
	const prefix = "OK cluster="
	if !strings.HasPrefix(response, prefix) {
		t.Fatalf("unexpected CLUSTER_STATUS response: %s", response)
	}
	var status clusterStatus
	if err := json.Unmarshal([]byte(strings.TrimPrefix(response, prefix)), &status); err != nil {
		t.Fatalf("invalid cluster status JSON: %v", err)
	}
	if status.RaftState != "unavailable" {
		t.Fatalf("expected unavailable Raft telemetry, got %+v", status)
	}
	if status.BrokerCount != 0 || status.PartitionCount != 0 {
		t.Fatalf("expected broker and partition diagnostics to remain available, got %+v", status)
	}
}
