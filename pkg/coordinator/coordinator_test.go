package coordinator_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
)

func TestCoordinator_Register_Add_Remove(t *testing.T) {
	cfg := &config.Config{ConsumerSessionTimeoutMS: 30000, ConsumerHeartbeatCheckMS: 5000}
	c := coordinator.NewCoordinator(cfg, &coordinator.DummyPublisher{})

	err := c.RegisterGroup("orders", "groupA", 3)
	if err != nil {
		t.Fatalf("RegisterGroup failed: %v", err)
	}

	_, err = c.AddConsumer("groupA", "consumer-1")
	if err != nil {
		t.Fatalf("AddConsumer failed: %v", err)
	}

	_, err = c.AddConsumer("groupA", "consumer-2")
	if err != nil {
		t.Fatalf("AddConsumer failed: %v", err)
	}

	c.Rebalance("groupA")

	assign1 := c.GetAssignments("groupA")["consumer-1"]
	assign2 := c.GetAssignments("groupA")["consumer-2"]

	if len(assign1) == 0 {
		t.Fatalf("consumer-1 expected to have at least 1 partition")
	}
	if len(assign2) == 0 {
		t.Fatalf("consumer-2 expected to have at least 1 partition")
	}

	if overlap(assign1, assign2) {
		t.Fatalf("expected non-overlapping assignments, got overlap: %v & %v", assign1, assign2)
	}

	all := c.GetAssignments("groupA")
	if len(all) != 2 {
		t.Fatalf("expected 2 members, got %d", len(all))
	}
	if _, ok := all["consumer-1"]; !ok {
		t.Fatalf("consumer-1 missing in assignment map")
	}
	if _, ok := all["consumer-2"]; !ok {
		t.Fatalf("consumer-2 missing in assignment map")
	}

	old := assign1[0]
	err = c.RecordHeartbeat("groupA", "consumer-1")
	if err != nil {
		t.Fatalf("RecordHeartbeat failed: %v", err)
	}
	updated := c.GetAssignments("groupA")
	if updated["consumer-1"][0] != old {
		t.Fatalf("heartbeat should not change assignments")
	}

	err = c.RemoveConsumer("groupA", "consumer-1")
	if err != nil {
		t.Fatalf("RemoveConsumer failed: %v", err)
	}

	afterRemove := c.GetAssignments("groupA")
	if len(afterRemove) != 1 {
		t.Fatalf("after remove, expected 1 consumer, got %d", len(afterRemove))
	}

	if len(afterRemove["consumer-2"]) == 0 {
		t.Fatalf("remaining consumer must have partitions after rebalance")
	}
}

// overlap checks whether two partition sets intersect.
func overlap(a, b []int) bool {
	m := make(map[int]struct{})
	for _, v := range a {
		m[v] = struct{}{}
	}
	for _, v := range b {
		if _, ok := m[v]; ok {
			return true
		}
	}
	return false
}
