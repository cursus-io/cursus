package sdk

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newManualBatchTestConsumer(t *testing.T, brokerAddr string) (*Consumer, *PartitionConsumer) {
	t.Helper()
	cfg := NewDefaultConsumerConfig()
	cfg.EnableAutoCommit = false
	if brokerAddr != "" {
		cfg.BrokerAddrs = []string{brokerAddr}
		cfg.CoordinatorAddr = brokerAddr
	}
	consumer, err := NewConsumer(cfg)
	require.NoError(t, err)
	consumer.state.Store(uint32(ConsumerStateRunning))
	consumer.assignmentGeneration.Store(1)
	consumer.generation = 7
	consumer.memberID = "member-7"
	if brokerAddr != "" {
		consumer.setCoordinatorCache(brokerAddr, coordinatorRouteCache, coordinatorReasonInitial)
	}
	pc := &PartitionConsumer{
		partitionID:          0,
		consumer:             consumer,
		assignmentGeneration: 1,
		ctx:                  consumer.assignmentContext(),
		dataCh:               make(chan *messageBatch, 2),
	}
	consumer.partitionConsumers[0] = pc
	consumer.offsets[0] = 0
	consumer.publishMembership(7, "member-7", 1, []int{0})
	t.Cleanup(func() {
		consumer.rootCancel()
		consumer.cancelAssignment()
		consumer.closeActiveConnections()
	})
	return consumer, pc
}

func runManualBatchWorker(t *testing.T, consumer *Consumer, pc *PartitionConsumer) {
	t.Helper()
	consumer.wg.Add(1)
	done := make(chan struct{})
	go func() {
		pc.runWorker()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("manual batch worker did not stop")
	}
}

func TestConsumerBatchCommitPersistsExplicitNextOffset(t *testing.T) {
	addr, commands := startSDKCommandServer(t, "OK committed=true")
	consumer, pc := newManualBatchTestConsumer(t, addr)
	commitResult := make(chan error, 1)
	consumer.batchHandler = func(ctx context.Context, batch ConsumerBatch) error {
		require.Equal(t, 0, batch.Partition)
		require.Equal(t, []uint64{1, 2}, []uint64{batch.Messages[0].Offset, batch.Messages[1].Offset})
		require.Equal(t, uint64(3), batch.NextOffset)
		commitResult <- consumer.CommitBatch(ctx, batch)
		return nil
	}
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 1, Payload: "one"}, {Offset: 2, Payload: "two"}}}
	close(pc.dataCh)

	runManualBatchWorker(t, consumer, pc)
	require.NoError(t, <-commitResult)
	require.Equal(t, "COMMIT_OFFSET topic=default-topic partition=0 group=default-group offset=3 generation=7 member=member-7", requireSDKCommand(t, commands))
	require.Equal(t, uint64(3), consumer.offsets[0])
}

func TestConsumerBatchHandlerFailureDoesNotCommit(t *testing.T) {
	consumer, pc := newManualBatchTestConsumer(t, "")
	consumer.batchHandler = func(context.Context, ConsumerBatch) error { return errors.New("database unavailable") }
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 4, Payload: "retry"}}}
	close(pc.dataCh)

	runManualBatchWorker(t, consumer, pc)
	var handlerErr *ConsumerBatchHandlerError
	require.ErrorAs(t, consumer.Err(), &handlerErr)
	require.Equal(t, uint64(0), consumer.offsets[0])
	require.Equal(t, uint64(0), atomic.LoadUint64(&pc.fetchOffset))
}

func TestConsumerBatchFailureIsRedeliveredToReplacementConsumer(t *testing.T) {
	const failedOffset = uint64(4)
	deliveries := 0

	first, firstPartition := newManualBatchTestConsumer(t, "")
	first.batchHandler = func(context.Context, ConsumerBatch) error {
		deliveries++
		return errors.New("database transaction rolled back")
	}
	firstPartition.dataCh <- &messageBatch{messages: []Message{{Offset: failedOffset, Payload: "retry"}}}
	close(firstPartition.dataCh)
	runManualBatchWorker(t, first, firstPartition)
	require.Equal(t, uint64(0), first.offsets[0])

	addr, commands := startSDKCommandServer(t, "OK committed=true")
	replacement, replacementPartition := newManualBatchTestConsumer(t, addr)
	replacement.batchHandler = func(ctx context.Context, batch ConsumerBatch) error {
		deliveries++
		require.Equal(t, failedOffset, batch.Messages[0].Offset)
		return replacement.CommitBatch(ctx, batch)
	}
	replacementPartition.dataCh <- &messageBatch{messages: []Message{{Offset: failedOffset, Payload: "retry"}}}
	close(replacementPartition.dataCh)
	runManualBatchWorker(t, replacement, replacementPartition)

	require.Equal(t, 2, deliveries)
	require.Equal(t, uint64(5), replacement.offsets[0])
	require.Equal(t, "COMMIT_OFFSET topic=default-topic partition=0 group=default-group offset=5 generation=7 member=member-7", requireSDKCommand(t, commands))
}

func TestConsumerBatchCancellationDoesNotCommit(t *testing.T) {
	consumer, pc := newManualBatchTestConsumer(t, "")
	started := make(chan struct{})
	consumer.batchHandler = func(ctx context.Context, _ ConsumerBatch) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 4, Payload: "cancel"}}}
	close(pc.dataCh)

	consumer.wg.Add(1)
	done := make(chan struct{})
	go func() { pc.runWorker(); close(done) }()
	<-started
	consumer.cancelAssignment()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled batch handler did not stop")
	}
	require.NoError(t, consumer.Err())
	require.Equal(t, uint64(0), consumer.offsets[0])
}

func TestConsumerBatchCommitRejectsRebalanceAndStaleGeneration(t *testing.T) {
	consumer, pc := newManualBatchTestConsumer(t, "")
	batch := consumer.makeConsumerBatch(pc, []Message{{Offset: 5}})

	nextAssignment, ok := consumer.beginRebalance()
	require.True(t, ok)
	err := consumer.CommitBatch(context.Background(), batch)
	require.ErrorIs(t, err, ErrConsumerRebalancing)
	var commitErr *ConsumerCommitError
	require.ErrorAs(t, err, &commitErr)

	consumer.finishRebalance()
	consumer.generation = 8
	consumer.memberID = "member-8"
	replacement := &PartitionConsumer{partitionID: 0, consumer: consumer, assignmentGeneration: nextAssignment}
	consumer.partitionConsumers[0] = replacement
	consumer.publishMembership(8, "member-8", nextAssignment, []int{0})
	err = consumer.CommitBatch(context.Background(), batch)
	require.ErrorIs(t, err, ErrConsumerStaleGeneration)
}

func TestConsumerBatchLateBrokerFailurePreservesReplacementMembership(t *testing.T) {
	consumer, pc := newManualBatchTestConsumer(t, "")
	batch := consumer.makeConsumerBatch(pc, []Message{{Offset: 5}})

	nextAssignment, ok := consumer.beginRebalance()
	require.True(t, ok)
	consumer.finishRebalance()
	consumer.generation = 8
	consumer.memberID = "member-8"
	consumer.partitionConsumers[0] = &PartitionConsumer{
		partitionID:          0,
		consumer:             consumer,
		assignmentGeneration: nextAssignment,
	}
	consumer.publishMembership(8, "member-8", nextAssignment, []int{0})

	err := consumer.classifyBatchCommitError(batch, &BrokerError{Code: "GEN_MISMATCH"})
	require.ErrorIs(t, err, ErrConsumerStaleGeneration)
	require.Equal(t, ConsumerMembership{
		Ready:                true,
		Generation:           8,
		AssignmentGeneration: nextAssignment,
		MemberID:             "member-8",
		Partitions:           []int{0},
	}, consumer.Membership())
}

func TestConsumerBatchHandlerIsSerialPerPartition(t *testing.T) {
	consumer, pc := newManualBatchTestConsumer(t, "")
	var active atomic.Int32
	var maximum atomic.Int32
	consumer.batchHandler = func(context.Context, ConsumerBatch) error {
		current := active.Add(1)
		observed := maximum.Load()
		for current > observed && !maximum.CompareAndSwap(observed, current) {
			observed = maximum.Load()
		}
		time.Sleep(10 * time.Millisecond)
		active.Add(-1)
		return nil
	}
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 1}}}
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 2}}}
	close(pc.dataCh)

	runManualBatchWorker(t, consumer, pc)
	require.Equal(t, int32(1), maximum.Load())
}

func TestConsumerStartBatchRequiresManualCommit(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	require.NoError(t, err)
	require.ErrorIs(t, consumer.StartBatch(func(context.Context, ConsumerBatch) error { return nil }), ErrConsumerManualCommitRequired)
	require.Equal(t, ConsumerStateNew, consumer.State())
	require.NoError(t, consumer.Close())
}
