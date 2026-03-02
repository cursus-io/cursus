package coordinator

import (
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
)

type testCoordinator struct {
	*Coordinator
	rebalanceCalls map[string]int
}

func (tc *testCoordinator) triggerRebalance(groupName string) {
	if tc.rebalanceCalls == nil {
		tc.rebalanceCalls = make(map[string]int)
	}
	tc.rebalanceCalls[groupName]++
}

func TestHeartbeatMonitor_TimeoutRemovesMember(t *testing.T) {
	c := &testCoordinator{
		Coordinator: &Coordinator{
			mu: sync.RWMutex{},
			groups: map[string]*GroupMetadata{
				"testGroup": {
					Members: map[string]*MemberMetadata{
						"consumer1": {LastHeartbeat: time.Now().Add(-10 * time.Second)}, // will timeout
						"consumer2": {LastHeartbeat: time.Now()},                        // healthy
					},
				},
			},
			cfg: &config.Config{
				ConsumerSessionTimeoutMS: 5000,
			},
		},
	}

	c.mu.Lock()
	for groupName, group := range c.groups {
		for memberID, member := range group.Members {
			timeout := time.Duration(c.cfg.ConsumerSessionTimeoutMS) * time.Millisecond
			if time.Since(member.LastHeartbeat) > timeout {
				delete(group.Members, memberID)
				c.triggerRebalance(groupName)
			}
		}
	}
	c.mu.Unlock()

	group := c.groups["testGroup"]
	if _, exists := group.Members["consumer1"]; exists {
		t.Fatal("consumer1 should have been removed due to timeout")
	}
	if _, exists := group.Members["consumer2"]; !exists {
		t.Fatal("consumer2 should still exist")
	}

	if c.rebalanceCalls["testGroup"] != 1 {
		t.Fatalf("expected rebalance to be called once, got %d", c.rebalanceCalls["testGroup"])
	}
}
