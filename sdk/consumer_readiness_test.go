package sdk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsumerMembershipTransitionsAndCopiesAssignment(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)

	require.Equal(t, ConsumerMembership{}, consumer.Membership())
	consumer.state.Store(uint32(ConsumerStateRunning))
	consumer.assignmentGeneration.Store(1)
	consumer.publishMembership(7, "member-7", 1, []int{2, 0, 1})

	ready := consumer.Membership()
	require.True(t, ready.Ready)
	require.Equal(t, int64(7), ready.Generation)
	require.Equal(t, uint64(1), ready.AssignmentGeneration)
	require.Equal(t, "member-7", ready.MemberID)
	require.Equal(t, []int{0, 1, 2}, ready.Partitions)
	ready.Partitions[0] = 99
	require.Equal(t, []int{0, 1, 2}, consumer.Membership().Partitions)

	next, ok := consumer.beginRebalance()
	require.True(t, ok)
	require.Equal(t, uint64(2), next)
	require.Equal(t, ConsumerMembership{}, consumer.Membership())

	consumer.finishRebalance()
	consumer.publishMembership(8, "member-8", next, []int{1})
	require.True(t, consumer.Membership().Ready)
	require.NoError(t, consumer.Close())
	require.Equal(t, ConsumerMembership{}, consumer.Membership())
}

func TestConsumerMembershipRequiresAssignedPartition(t *testing.T) {
	consumer := newTestConsumer(t)
	t.Cleanup(func() { _ = consumer.Close() })
	consumer.publishMembership(7, "member-7", 1, nil)
	require.False(t, consumer.Membership().Ready)
}

func TestConsumerMembershipBecomesReadyOnlyAfterSyncAssignment(t *testing.T) {
	addr, commands := startSDKCommandServer(
		t,
		"OK generation=7 member=member-7 assignments=[0]",
		"OK generation=7 member=member-7 assignments=[0]",
		"OK group=workers member=member-7 left=true",
	)
	consumer := newGroupContractConsumer(t, addr)
	consumer.state.Store(uint32(ConsumerStateRunning))

	generation, memberID, _, err := consumer.joinGroup()
	require.NoError(t, err)
	require.False(t, consumer.Membership().Ready)

	assignments, err := consumer.syncGroup(generation, memberID)
	require.NoError(t, err)
	require.False(t, consumer.Membership().Ready)

	consumer.mu.Lock()
	consumer.generation = generation
	consumer.memberID = memberID
	consumer.mu.Unlock()
	assignmentGeneration := consumer.assignmentGeneration.Add(1)
	consumer.publishMembership(generation, memberID, assignmentGeneration, assignments)
	ready := consumer.Membership()
	require.True(t, ready.Ready)
	require.Equal(t, int64(7), ready.Generation)
	require.Equal(t, []int{0}, ready.Partitions)

	require.Equal(t, "JOIN_GROUP topic=events group=workers member=worker", requireSDKCommand(t, commands))
	require.Equal(t, "SYNC_GROUP topic=events group=workers member=member-7 generation=7", requireSDKCommand(t, commands))
	require.NoError(t, consumer.Close())
	require.False(t, consumer.Membership().Ready)
	require.Equal(t, "LEAVE_GROUP topic=events group=workers member=member-7 generation=7", requireSDKCommand(t, commands))
}

func TestConsumerManualCommitDoesNotQueueAutomaticCommit(t *testing.T) {
	c := newTestConsumer(t)
	t.Cleanup(func() { _ = c.Close() })
	c.config.EnableAutoCommit = false
	handled := 0
	c.MessageHandler = func(Message) error { handled++; return nil }
	pc := &PartitionConsumer{consumer: c, partitionID: 0, dataCh: make(chan *messageBatch, 1)}
	require.True(t, pc.assignmentActive())
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 42, Payload: "manual"}}}
	close(pc.dataCh)
	c.wg.Add(1)
	done := make(chan struct{})
	go func() { pc.runWorker(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manual commit worker blocked on automatic commit")
	}
	require.Equal(t, 1, handled)
	require.Empty(t, c.commitCh)
	require.Empty(t, c.offsets)
}

func TestConsumerGenerationWorkersStopWithoutClosingConsumer(t *testing.T) {
	c := newTestConsumer(t)
	t.Cleanup(func() { _ = c.Close() })
	c.config.HeartbeatIntervalMS = int(time.Hour / time.Millisecond)
	c.config.MetadataRefreshInterval = time.Hour
	ctx := c.assignmentContext()
	generation := c.assignmentGeneration.Load()
	c.startAssignmentWorkers(ctx, generation)
	c.cancelAssignment()
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("generation workers did not stop")
	}
	select {
	case <-c.Done():
		t.Fatal("generation cancellation closed consumer")
	default:
	}
}
