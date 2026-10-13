package replication

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestISRManager_Lifecycle(t *testing.T) {
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	isrManager := NewISRManager(context.Background(), brokerFSM, "node1", 100*time.Millisecond, nil)
	t.Cleanup(isrManager.Stop)

	isrManager.Start()

	isrManager.UpdateHeartbeat("node2")
	isrManager.mu.RLock()
	_, ok := isrManager.lastSeen["node2"]
	isrManager.mu.RUnlock()
	assert.True(t, ok)
}

func TestISRManager_SetLeader(t *testing.T) {
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	applier := &MockCommandApplier{IsLeaderResult: false}
	isrManager := NewISRManager(context.Background(), brokerFSM, "node1", 100*time.Millisecond, applier)

	isrManager.SetLeader(true)
	isrManager.mu.RLock()
	assert.False(t, isrManager.leaderSince.IsZero())
	isrManager.mu.RUnlock()

	isrManager.SetLeader(false)
	isrManager.mu.RLock()
	assert.True(t, isrManager.leaderSince.IsZero())
	isrManager.mu.RUnlock()
}

func TestISRManagerBrokerLivenessUsesHeartbeatAndLeaderGrace(t *testing.T) {
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	isrManager := NewISRManager(context.Background(), brokerFSM, "node1", time.Second, nil)
	isrManager.SetLeader(true)
	assert.True(t, isrManager.IsBrokerAlive("node2"), "new leader should grant heartbeat grace")

	isrManager.mu.Lock()
	isrManager.leaderSince = time.Now().Add(-2 * time.Second)
	isrManager.lastSeen["node2"] = time.Now().Add(-2 * time.Second)
	isrManager.mu.Unlock()
	assert.False(t, isrManager.IsBrokerAlive("node2"))

	isrManager.UpdateHeartbeat("node2")
	assert.True(t, isrManager.IsBrokerAlive("node2"))
	assert.True(t, isrManager.IsBrokerAlive("node1"))
}

func TestISRManagerComputeISRPreservesExistingMembersDuringLeaderGrace(t *testing.T) {
	brokerFSM := fsm.NewBrokerFSM(nil, nil)
	for _, brokerID := range []string{"node1", "node2", "node3"} {
		payload, err := json.Marshal(fsm.BrokerInfo{ID: brokerID, Status: "active"})
		require.NoError(t, err)
		require.Nil(t, brokerFSM.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...)}))
	}
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = 1
	definition.ReplicationFactor = 3
	topicPayload, err := json.Marshal(fsm.TopicCommand{Definition: &definition, LeaderID: "node1"})
	require.NoError(t, err)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: append([]byte("TOPIC:"), topicPayload...)}))

	applier := &MockCommandApplier{IsLeaderResult: true, fsm: brokerFSM}
	manager := NewISRManager(context.Background(), brokerFSM, "node1", time.Second, applier)
	manager.mu.Lock()
	manager.leaderSince = time.Now()
	manager.mu.Unlock()

	existingISR := brokerFSM.GetPartitionMetadata("orders-0").ISR
	require.ElementsMatch(t, []string{"node1", "node2", "node3"}, existingISR)
	require.Equal(t, existingISR, manager.ComputeISR("orders", 0))
	require.Equal(t, existingISR, brokerFSM.GetPartitionMetadata("orders-0").ISR)

	manager.mu.Lock()
	manager.leaderSince = time.Now().Add(-2 * time.Second)
	manager.mu.Unlock()
	require.Equal(t, []string{"node1"}, manager.ComputeISR("orders", 0),
		"members without a heartbeat must still leave ISR after the bounded leader grace")
}
