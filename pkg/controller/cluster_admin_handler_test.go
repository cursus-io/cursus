package controller

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/hashicorp/raft"
)

func TestBuildClusterStatusReportsAvailabilityAndReplication(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	for _, broker := range []fsm.BrokerInfo{
		{ID: "broker-1", Addr: "broker-1:9001", Status: "active"},
		{ID: "broker-2", Addr: "broker-2:9001", Status: "active"},
		{ID: "broker-3", Addr: "broker-3:9001", Status: "inactive"},
	} {
		data, err := json.Marshal(broker)
		if err != nil {
			t.Fatal(err)
		}
		state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)})
	}
	partitions := map[string]fsm.PartitionMetadata{
		"orders-eu-0": {
			Leader: "broker-1", LeaderEpoch: 2, CommittedHWM: 11,
			Replicas: []string{"broker-1", "broker-2", "broker-3"}, ISR: []string{"broker-1", "broker-2"},
		},
		"orders-eu-1": {
			Leader: "broker-3", LeaderEpoch: 4, CommittedHWM: 9,
			Replicas: []string{"broker-3"}, ISR: []string{"broker-3"},
		},
	}
	for key, metadata := range partitions {
		data, err := json.Marshal(metadata)
		if err != nil {
			t.Fatal(err)
		}
		state.Apply(&raft.Log{Data: []byte("PARTITION:" + key + ":" + string(data))})
	}

	status := buildClusterStatus(state, "broker-1:9001")
	if status.BrokerCount != 3 || status.ActiveBrokers != 2 || status.InactiveBrokers != 1 {
		t.Fatalf("unexpected broker status: %+v", status)
	}
	if status.PartitionCount != 2 || status.UnderReplicated != 1 || status.Leaderless != 1 {
		t.Fatalf("unexpected partition status: %+v", status)
	}
	if status.Partitions[0].Topic != "orders-eu" || status.Partitions[0].Partition != 0 {
		t.Fatalf("hyphenated topic was not parsed correctly: %+v", status.Partitions[0])
	}
}

func TestClusterAdminCommandsAreRegisteredOutsideDistributedMode(t *testing.T) {
	ch, _ := newTestHandler(t)
	for _, command := range []string{
		"CLUSTER_STATUS",
		"ELECT_LEADER topic=orders partition=0 broker=broker-2",
	} {
		resp := ch.HandleCommand(command, NewClientContext("", 0))
		if strings.Contains(resp, "unknown_command") || !strings.Contains(resp, "distribution_required") {
			t.Fatalf("%s was not routed to its handler: %s", command, resp)
		}
	}
}
