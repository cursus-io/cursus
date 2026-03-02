package coordinator

import (
	"reflect"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

type DummyPublisher struct{}

func (d *DummyPublisher) Publish(topic string, msg *types.Message) error {
	return nil
}
func (d *DummyPublisher) CreateTopic(topic string, partitionCount int) {}

func TestRebalanceRange_AssignsPartitionsEvenly(t *testing.T) {
	cfg := &config.Config{
		ConsumerSessionTimeoutMS: 30000,
	}
	c := NewCoordinator(cfg, &DummyPublisher{})

	groupName := "group1"
	partitionCount := 5
	if err := c.RegisterGroup("topic1", groupName, partitionCount); err != nil {
		t.Fatalf("RegisterGroup failed: %v", err)
	}

	if _, err := c.AddConsumer(groupName, "c1"); err != nil {
		t.Fatalf("AddConsumer c1 failed: %v", err)
	}
	if _, err := c.AddConsumer(groupName, "c2"); err != nil {
		t.Fatalf("AddConsumer c2 failed: %v", err)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]

	assignmentsC1 := group.Members["c1"].Assignments
	assignmentsC2 := group.Members["c2"].Assignments

	expectedC1 := []int{0, 1, 2}
	expectedC2 := []int{3, 4}

	if !reflect.DeepEqual(assignmentsC1, expectedC1) {
		t.Fatalf("c1 assignments wrong. got %v, want %v", assignmentsC1, expectedC1)
	}
	if !reflect.DeepEqual(assignmentsC2, expectedC2) {
		t.Fatalf("c2 assignments wrong. got %v, want %v", assignmentsC2, expectedC2)
	}
}

func TestRebalanceRange_NoMembers(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(cfg, &DummyPublisher{})

	groupName := "groupEmpty"
	if err := c.RegisterGroup("topicX", groupName, 3); err != nil {
		t.Fatalf("RegisterGroup failed: %v", err)
	}

	c.rebalanceRange(groupName)
}

func TestRebalanceRange_MoreMembersThanPartitions(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(cfg, &DummyPublisher{})

	groupName := "group2"
	if err := c.RegisterGroup("topicY", groupName, 2); err != nil {
		t.Fatalf("RegisterGroup failed: %v", err)
	}

	if _, err := c.AddConsumer(groupName, "c1"); err != nil {
		t.Fatalf("AddConsumer c1 failed: %v", err)
	}
	if _, err := c.AddConsumer(groupName, "c2"); err != nil {
		t.Fatalf("AddConsumer c2 failed: %v", err)
	}
	if _, err := c.AddConsumer(groupName, "c3"); err != nil {
		t.Fatalf("AddConsumer c3 failed: %v", err)
	}

	c.rebalanceRange(groupName)

	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]

	totalAssigned := 0
	for _, m := range group.Members {
		totalAssigned += len(m.Assignments)
	}

	if totalAssigned != 2 {
		t.Fatalf("total assigned partitions wrong. got %d, want 2", totalAssigned)
	}
}
