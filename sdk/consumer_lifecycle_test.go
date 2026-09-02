package sdk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsumerLifecycleTransitionsAndAssignmentFence(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	require.NoError(t, err)
	require.Equal(t, ConsumerStateNew, consumer.State())
	require.Equal(t, "new", consumer.State().String())

	require.NoError(t, consumer.beginStart())
	first := consumer.assignmentGeneration.Add(1)
	require.True(t, consumer.assignmentActive(first))

	second, ok := consumer.beginRebalance()
	require.True(t, ok)
	require.Equal(t, first+1, second)
	require.Equal(t, ConsumerStateRebalancing, consumer.State())
	require.False(t, consumer.assignmentActive(first))
	require.False(t, consumer.assignmentActive(second))

	consumer.finishRebalance()
	require.Equal(t, ConsumerStateRunning, consumer.State())
	require.True(t, consumer.assignmentActive(second))
	require.NoError(t, consumer.Close())
	require.Equal(t, ConsumerStateClosed, consumer.State())
}

func TestConsumerCloseWinsConcurrentRebalanceCompletion(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	require.NoError(t, err)
	require.NoError(t, consumer.beginStart())
	consumer.assignmentGeneration.Add(1)
	_, ok := consumer.beginRebalance()
	require.True(t, ok)

	require.NoError(t, consumer.Close())
	consumer.finishRebalance()
	require.Equal(t, ConsumerStateClosed, consumer.State())
}

func TestConsumerCommitWorkerRejectsStaleAssignment(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	require.NoError(t, err)
	require.NoError(t, consumer.beginStart())
	current := consumer.assignmentGeneration.Add(2)
	consumer.startCommitWorker()

	result := make(chan error, 1)
	consumer.commitCh <- commitEntry{partition: 0, offset: 1, assignmentGeneration: current - 1, respCh: result}
	select {
	case err := <-result:
		require.True(t, errors.Is(err, ErrConsumerRebalancing))
	case <-time.After(time.Second):
		t.Fatal("stale commit was not rejected")
	}
	require.NoError(t, consumer.Close())
}

func TestPartitionConsumerRejectsStaleAssignmentBeforeDial(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	require.NoError(t, err)
	require.NoError(t, consumer.beginStart())
	current := consumer.assignmentGeneration.Add(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	partition := &PartitionConsumer{consumer: consumer, assignmentGeneration: current - 1, ctx: ctx}

	err = partition.ensureConnection()
	require.ErrorContains(t, err, "consumer shutting down")
	require.NoError(t, consumer.Close())
}
