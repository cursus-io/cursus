package util_test

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/util"
)

func TestConsistentHashRing_BasicAddGet(t *testing.T) {
	ring := util.NewConsistentHashRing(50, nil)
	ring.Add("node1", "node2", "node3")

	key := "my-key"
	result1 := ring.Get(key)
	result2 := ring.Get(key)
	if result1 != result2 {
		t.Fatalf("Get should be deterministic: got %s and %s", result1, result2)
	}

	if result1 == "" {
		t.Fatal("Get should return a non-empty node")
	}
}

func TestConsistentHashRing_EmptyRing(t *testing.T) {
	ring := util.NewConsistentHashRing(50, nil)

	if got := ring.Get("key"); got != "" {
		t.Fatalf("Get on empty ring should return empty string, got %s", got)
	}

	if got := ring.GetN("key", 3); got != nil {
		t.Fatalf("GetN on empty ring should return nil, got %v", got)
	}
}

func TestConsistentHashRing_Remove(t *testing.T) {
	ring := util.NewConsistentHashRing(50, nil)
	ring.Add("node1", "node2", "node3")

	// Record assignments before removal
	keys := make([]string, 100)
	before := make(map[string]string)
	for i := 0; i < 100; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		before[keys[i]] = ring.Get(keys[i])
	}

	// Remove node2
	ring.Remove("node2")

	if ring.Members() != 2 {
		t.Fatalf("Expected 2 members after remove, got %d", ring.Members())
	}

	// Keys previously on node1 or node3 should stay there
	movedCount := 0
	for _, k := range keys {
		after := ring.Get(k)
		if after == "node2" {
			t.Fatalf("Key %s mapped to removed node2", k)
		}
		if before[k] != "node2" && before[k] != after {
			movedCount++
		}
	}

	// Only keys that were on node2 should have moved
	if movedCount > 0 {
		t.Errorf("Expected 0 keys on non-removed nodes to move, but %d moved", movedCount)
	}
}

func TestConsistentHashRing_AddNodeStability(t *testing.T) {
	ring := util.NewConsistentHashRing(150, nil)
	nodes := []string{"node1", "node2", "node3"}
	ring.Add(nodes...)

	const numKeys = 1000
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("partition-key-%d", i)
		before[key] = ring.Get(key)
	}

	// Add a 4th node
	ring.Add("node4")

	movedCount := 0
	for key, oldNode := range before {
		newNode := ring.Get(key)
		if oldNode != newNode {
			movedCount++
		}
	}

	// With consistent hashing, roughly 1/4 of keys should move (±tolerance)
	maxExpected := numKeys / 2 // generous upper bound
	if movedCount > maxExpected {
		t.Errorf("Too many keys moved: %d/%d (expected < %d)", movedCount, numKeys, maxExpected)
	}
	t.Logf("Keys moved after adding node4: %d/%d (%.1f%%)", movedCount, numKeys, float64(movedCount)*100/float64(numKeys))
}

func TestConsistentHashRing_GetN(t *testing.T) {
	ring := util.NewConsistentHashRing(50, nil)
	ring.Add("node1", "node2", "node3", "node4", "node5")

	// GetN should return exactly n distinct nodes
	result := ring.GetN("some-key", 3)
	if len(result) != 3 {
		t.Fatalf("Expected 3 nodes, got %d: %v", len(result), result)
	}

	// All nodes must be unique
	seen := make(map[string]bool)
	for _, n := range result {
		if seen[n] {
			t.Fatalf("Duplicate node in GetN result: %s", n)
		}
		seen[n] = true
	}

	// First node should be same as Get
	single := ring.Get("some-key")
	if result[0] != single {
		t.Fatalf("GetN first element (%s) should match Get result (%s)", result[0], single)
	}
}

func TestConsistentHashRing_GetN_ExceedsMembers(t *testing.T) {
	ring := util.NewConsistentHashRing(50, nil)
	ring.Add("node1", "node2")

	result := ring.GetN("key", 5)
	if len(result) != 2 {
		t.Fatalf("GetN with n > members should return all members, got %d: %v", len(result), result)
	}
}

func TestConsistentHashRing_GetN_Deterministic(t *testing.T) {
	ring := util.NewConsistentHashRing(50, nil)
	ring.Add("node1", "node2", "node3")

	r1 := ring.GetN("key-x", 2)
	r2 := ring.GetN("key-x", 2)

	if r1[0] != r2[0] || r1[1] != r2[1] {
		t.Fatalf("GetN should be deterministic: %v vs %v", r1, r2)
	}
}

func TestConsistentHashRing_Members(t *testing.T) {
	ring := util.NewConsistentHashRing(50, nil)
	if ring.Members() != 0 {
		t.Fatal("Empty ring should have 0 members")
	}

	ring.Add("a", "b", "c")
	if ring.Members() != 3 {
		t.Fatalf("Expected 3 members, got %d", ring.Members())
	}

	ring.Remove("b")
	if ring.Members() != 2 {
		t.Fatalf("Expected 2 members after remove, got %d", ring.Members())
	}
}

func TestConsistentHashRing_Distribution(t *testing.T) {
	ring := util.NewConsistentHashRing(150, nil)
	nodes := []string{"broker-1", "broker-2", "broker-3"}
	ring.Add(nodes...)

	counts := make(map[string]int)
	const numKeys = 3000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("topic-partition-%d", i)
		node := ring.Get(key)
		counts[node]++
	}

	// Each node should get roughly 1/3 of keys (±30%)
	expected := numKeys / len(nodes)
	for node, count := range counts {
		ratio := float64(count) / float64(expected)
		if ratio < 0.5 || ratio > 1.5 {
			t.Errorf("Node %s has poor distribution: %d keys (expected ~%d, ratio=%.2f)", node, count, expected, ratio)
		}
	}

	t.Logf("Distribution: %v", counts)
}
