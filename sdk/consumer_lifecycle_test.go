package sdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsumerStartRejectsNilHandlerBeforeBrokerIO(t *testing.T) {
	for _, mode := range []ConsumerMode{ModePolling, ModeStreaming} {
		for _, autoCommit := range []bool{true, false} {
			t.Run(string(mode)+"/auto_commit="+fmt.Sprint(autoCommit), func(t *testing.T) {
				listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
				require.NoError(t, err)
				defer func() { _ = listener.Close() }()

				cfg := NewDefaultConsumerConfig()
				cfg.BrokerAddrs = []string{listener.Addr().String()}
				cfg.Mode = mode
				cfg.EnableAutoCommit = autoCommit
				consumer, err := NewConsumer(cfg)
				require.NoError(t, err)
				consumer.offsets[0] = 42

				err = consumer.Start(nil)
				require.ErrorIs(t, err, ErrConsumerHandlerRequired)
				require.Equal(t, ConsumerStateClosed, consumer.State())
				require.Equal(t, uint64(42), consumer.offsets[0])
				require.Empty(t, consumer.partitionConsumers)
				require.Empty(t, consumer.memberID)
				require.Error(t, consumer.rootCtx.Err())
				select {
				case <-consumer.Done():
				default:
					t.Fatal("Done remained open after rejected Start cleanup")
				}
				select {
				case <-consumer.closeDone:
				default:
					t.Fatal("closeDone remained open after rejected Start cleanup")
				}

				require.NoError(t, listener.SetDeadline(time.Now().Add(25*time.Millisecond)))
				conn, acceptErr := listener.Accept()
				if conn != nil {
					_ = conn.Close()
				}
				var netErr net.Error
				require.ErrorAs(t, acceptErr, &netErr)
				require.True(t, netErr.Timeout(), "unexpected accept error: %v", acceptErr)
				require.NoError(t, consumer.Close())
			})
		}
	}
}

func TestConsumerLifecycleTransitionsAndAssignmentFence(t *testing.T) {
	require.Equal(t, ConsumerStateClosing, ConsumerState(3), "existing state value must remain stable")
	require.Equal(t, ConsumerStateClosed, ConsumerState(4), "existing state value must remain stable")
	require.Equal(t, "failed", ConsumerStateFailed.String())

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

func TestConsumerFailureKeepsFirstCauseAndStartWaitReturnsItAfterCleanup(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	require.NoError(t, err)
	require.NoError(t, consumer.beginStart())

	first := errors.New("first failure")
	consumer.fail(first)
	consumer.fail(errors.New("later failure"))
	require.Equal(t, ConsumerStateFailed, consumer.State())
	require.ErrorIs(t, consumer.Err(), first)

	returned := make(chan error, 1)
	go func() { returned <- consumer.waitForShutdown() }()
	select {
	case err := <-returned:
		require.ErrorIs(t, err, first)
	case <-time.After(time.Second):
		t.Fatal("consumer shutdown did not return the retained failure")
	}
	require.Equal(t, ConsumerStateClosed, consumer.State())
	select {
	case <-consumer.Done():
	default:
		t.Fatal("Done remained open after failed consumer cleanup")
	}
}

func TestConsumerConcurrentFailuresRetainOneStableCause(t *testing.T) {
	consumer, err := NewConsumer(NewDefaultConsumerConfig())
	require.NoError(t, err)
	require.NoError(t, consumer.beginStart())

	causes := []error{errors.New("partition 0"), errors.New("partition 1"), errors.New("partition 2")}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, cause := range causes {
		cause := cause
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			consumer.fail(cause)
		}()
	}
	close(start)
	wg.Wait()

	retained := consumer.Err()
	require.Error(t, retained)
	require.Contains(t, causes, retained)
	consumer.fail(errors.New("late failure"))
	require.Same(t, retained, consumer.Err())
	require.Equal(t, ConsumerStateFailed, consumer.State())
	require.Error(t, consumer.rootCtx.Err())
	require.NoError(t, consumer.Close())
}
