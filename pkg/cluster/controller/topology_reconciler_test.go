package controller

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestTopologyReconcilerExpandsOneLegacyAssignmentDurably(t *testing.T) {
	state, configuration, leader := underfilledTopologyState(t, fsm.BrokerProtocolVersionCurrent)
	manager := &bootstrapRaftManager{
		MockRaftManager: &MockRaftManager{isLeader: true, mockFSM: state},
		configuration:   configuration,
	}
	controller := &ClusterController{RaftManager: manager}

	require.NoError(t, controller.RunTopologyReconciliationOnce())
	metadata := state.GetPartitionMetadata("orders-0")
	require.Len(t, metadata.Replicas, 3)
	require.Equal(t, leader, metadata.Replicas[0], "expansion must preserve the current assignment order")
	require.Equal(t, []string{leader}, metadata.ISR, "catch-up proof must control ISR admission")
	require.Equal(t, 1, manager.applyCount)

	require.NoError(t, controller.RunTopologyReconciliationOnce())
	require.Equal(t, 1, manager.applyCount, "a repaired assignment must be a no-op")
}

func TestTopologyReconcilerWaitsForEveryVoterCapability(t *testing.T) {
	state, configuration, _ := underfilledTopologyState(t, fsm.DistributedCompactionProtocolVersion)
	manager := &bootstrapRaftManager{
		MockRaftManager: &MockRaftManager{isLeader: true, mockFSM: state},
		configuration:   configuration,
	}
	controller := &ClusterController{RaftManager: manager}

	err := controller.RunTopologyReconciliationOnce()
	require.ErrorContains(t, err, "replica reassignment requires 3")
	require.Equal(t, 0, manager.applyCount)
	require.Len(t, state.GetPartitionMetadata("orders-0").Replicas, 1)
}

func underfilledTopologyState(t *testing.T, protocol int) (*fsm.BrokerFSM, raft.Configuration, string) {
	t.Helper()
	state := fsm.NewBrokerFSM(nil, nil)
	servers := make([]raft.Server, 0, 3)
	for index, brokerID := range []string{"n1", "n2", "n3"} {
		broker := fsm.BrokerInfo{
			ID: brokerID, Addr: fmt.Sprintf("127.0.0.1:%d", 9001+index),
			Status: "active", LifecycleProtocol: protocol,
		}
		payload, err := json.Marshal(broker)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Index: uint64(index + 1), Data: append([]byte("REGISTER:"), payload...)}))
		servers = append(servers, raft.Server{ID: raft.ServerID(brokerID), Suffrage: raft.Voter})
	}
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = 1
	definition.ReplicationFactor = 3
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Index: 10, Data: append([]byte("TOPIC:"), payload...)}))

	metadata := state.GetPartitionMetadata("orders-0")
	leader := metadata.Leader
	metadata.Replicas = []string{leader}
	metadata.ISR = []string{leader}
	payload, err = json.Marshal(metadata)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Index: 11, Data: []byte("PARTITION:orders-0:" + string(payload))}))
	return state, raft.Configuration{Servers: servers}, leader
}
