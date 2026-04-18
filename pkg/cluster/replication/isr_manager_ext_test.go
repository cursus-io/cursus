package replication

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/stretchr/testify/assert"
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
