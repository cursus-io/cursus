package coordinator

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

type DummyPublisher struct{}

func (d *DummyPublisher) Publish(topic string, msg *types.Message) error {
	return nil
}
func (d *DummyPublisher) CreateTopic(topic string, partitionCount int, idempotent bool, eventSourcing bool) error {
	return nil
}

func TestRebalanceRange_AssignsPartitionsEvenly(t *testing.T) {
	cfg := &config.Config{
		ConsumerSessionTimeoutMS: 30000,
	}
	c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})

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
	c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})

	groupName := "groupEmpty"
	if err := c.RegisterGroup("topicX", groupName, 3); err != nil {
		t.Fatalf("RegisterGroup failed: %v", err)
	}

	c.rebalanceRange(groupName)
}

func TestRebalanceRange_MoreMembersThanPartitions(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})

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

type lifecycleCallbackPublisher struct {
	callback func()
}

func (p *lifecycleCallbackPublisher) Publish(string, *types.Message) error {
	if p.callback != nil {
		p.callback()
	}
	return nil
}

func (p *lifecycleCallbackPublisher) CreateTopic(string, int, bool, bool) error {
	return nil
}

func TestGroupLifecyclePersistenceRunsWithoutCoordinatorLocks(t *testing.T) {
	publisher := &lifecycleCallbackPublisher{}
	c := NewCoordinator(context.Background(), &config.Config{}, publisher)
	t.Cleanup(c.Stop)
	publisher.callback = func() {
		_ = c.ListGroups()
	}

	operations := []struct {
		name string
		run  func() error
	}{
		{name: "register", run: func() error { return c.RegisterGroup("events", "workers", 1) }},
		{name: "delete", run: func() error { return c.DeleteGroup("workers") }},
	}
	for _, operation := range operations {
		operation := operation
		t.Run(operation.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- operation.run() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("lifecycle operation failed: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("lifecycle persistence callback deadlocked on coordinator locks")
			}
		})
	}
}

type blockingLifecyclePublisher struct {
	started chan struct{}
	release chan struct{}
}

func (p *blockingLifecyclePublisher) Publish(string, *types.Message) error {
	close(p.started)
	<-p.release
	return nil
}

func (p *blockingLifecyclePublisher) CreateTopic(string, int, bool, bool) error {
	return nil
}

func TestOffsetCommitRejectedWhileGroupDeletionIsPending(t *testing.T) {
	c := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	t.Cleanup(c.Stop)
	if err := c.RegisterGroup("events", "workers", 1); err != nil {
		t.Fatalf("RegisterGroup failed: %v", err)
	}

	publisher := &blockingLifecyclePublisher{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	c.topicHandler = publisher
	deleted := make(chan error, 1)
	go func() {
		deleted <- c.DeleteGroup("workers")
	}()

	select {
	case <-publisher.started:
	case <-time.After(2 * time.Second):
		t.Fatal("DeleteGroup did not begin its durable tombstone write")
	}

	if err := c.CommitOffset("workers", "events", 0, 1); err == nil {
		close(publisher.release)
		<-deleted
		t.Fatal("CommitOffset succeeded while the group tombstone was pending")
	}

	close(publisher.release)
	select {
	case err := <-deleted:
		if err != nil {
			t.Fatalf("DeleteGroup failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DeleteGroup did not finish after the tombstone write was released")
	}

	if _, err := c.GetGroupStatus("workers"); err == nil {
		t.Fatal("deleted group remained visible")
	}
}

func TestOffsetPersistenceRunsWithoutCoordinatorLock(t *testing.T) {
	c := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	t.Cleanup(c.Stop)
	if err := c.RegisterGroup("events", "workers", 1); err != nil {
		t.Fatalf("RegisterGroup failed: %v", err)
	}

	publisher := &blockingLifecyclePublisher{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	c.topicHandler = publisher
	committed := make(chan error, 1)
	go func() {
		committed <- c.CommitOffset("workers", "events", 0, 1)
	}()

	select {
	case <-publisher.started:
	case <-time.After(2 * time.Second):
		t.Fatal("offset persistence did not start")
	}

	writerAcquired := make(chan struct{})
	go func() {
		c.mu.Lock()
		close(writerAcquired)
		c.mu.Unlock()
	}()
	select {
	case <-writerAcquired:
	case <-time.After(2 * time.Second):
		close(publisher.release)
		<-committed
		t.Fatal("durable offset write retained the coordinator lock")
	}

	close(publisher.release)
	select {
	case err := <-committed:
		if err != nil {
			t.Fatalf("CommitOffset failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CommitOffset did not finish after durable write release")
	}
}
