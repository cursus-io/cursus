package controller

import (
	"context"
	"errors"
	"math"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCommonArgsRejectsUnboundedFetchInputs(t *testing.T) {
	handler, _ := newTestHandler(t)
	base := map[string]string{"topic": "orders", "partition": "0"}

	for _, value := range []string{"", "0", "-1", "invalid", strconv.Itoa(wire.MaxFetchRecords + 1), strconv.Itoa(math.MaxInt)} {
		t.Run("batch_"+value, func(t *testing.T) {
			args := cloneStringMap(base)
			args["batch"] = value
			_, err := handler.parseCommonArgs(args)
			require.Error(t, err)
			if value == strconv.Itoa(wire.MaxFetchRecords+1) || value == strconv.Itoa(math.MaxInt) {
				assert.Contains(t, err.Error(), "fetch_batch_too_large")
			} else {
				assert.Contains(t, err.Error(), "invalid_batch")
			}
		})
	}

	for _, value := range []string{"", "0", "-1", "invalid", strconv.Itoa(wire.MaxFetchWaitMillis + 1), strconv.Itoa(math.MaxInt)} {
		t.Run("wait_"+value, func(t *testing.T) {
			args := cloneStringMap(base)
			args["wait_ms"] = value
			_, err := handler.parseCommonArgs(args)
			require.Error(t, err)
			if value == strconv.Itoa(wire.MaxFetchWaitMillis+1) || value == strconv.Itoa(math.MaxInt) {
				assert.Contains(t, err.Error(), "fetch_wait_too_large")
			} else {
				assert.Contains(t, err.Error(), "invalid_wait_ms")
			}
		})
	}

	args := cloneStringMap(base)
	args["batch"] = strconv.Itoa(wire.MaxFetchRecords)
	args["wait_ms"] = strconv.Itoa(wire.MaxFetchWaitMillis)
	parsed, err := handler.parseCommonArgs(args)
	require.NoError(t, err)
	assert.Equal(t, wire.MaxFetchRecords, parsed.BatchSize)
	assert.Equal(t, time.Duration(wire.MaxFetchWaitMillis)*time.Millisecond, parsed.WaitTimeout)
}

func TestConcurrentConsumeLongPollsReleaseOnBrokerContextCancellation(t *testing.T) {
	handler, topics := newTestHandler(t)
	require.NoError(t, topics.CreateTopic("orders", 1, false, false))

	const workers = 128
	brokerCtx, stopBroker := context.WithCancel(context.Background())
	results := make(chan error, workers)
	connections := make([]net.Conn, 0, workers*2)
	for index := 0; index < workers; index++ {
		server, client := net.Pipe()
		connections = append(connections, server, client)
		clientCtx := NewClientContext("orders-group", index)
		clientCtx.SetRequestContext(brokerCtx)
		go func(server net.Conn, clientCtx *ClientContext) {
			_, err := handler.HandleConsumeCommand(server, "CONSUME topic=orders partition=0 offset=0 group=orders-group member=member batch=1 wait_ms=30000", clientCtx)
			results <- err
		}(server, clientCtx)
	}
	t.Cleanup(func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	})

	time.Sleep(25 * time.Millisecond)
	started := time.Now()
	stopBroker()
	for index := 0; index < workers; index++ {
		select {
		case err := <-results:
			require.Error(t, err)
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatalf("worker %d did not release after broker cancellation", index)
		}
	}
	assert.Less(t, time.Since(started), 500*time.Millisecond)
}

func TestConsumeLongPollStopsOnRequestCancellation(t *testing.T) {
	handler, topics := newTestHandler(t)
	require.NoError(t, topics.CreateTopic("orders", 1, false, false))

	requestCtx, cancel := context.WithCancel(context.Background())
	clientCtx := NewClientContext("orders-group", 0)
	clientCtx.SetRequestContext(requestCtx)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	result := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := handler.HandleConsumeCommand(server, "CONSUME topic=orders partition=0 offset=0 group=orders-group member=member-1 batch=1 wait_ms=30000", clientCtx)
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-result:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled), "expected context cancellation, got %v", err)
		assert.Less(t, time.Since(started), 250*time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("consume long poll did not stop after request cancellation")
	}
}

func TestConsumeNotificationWaitWakesWithoutPollingDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	notification := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- waitForConsumeNotification(ctx, time.Second, []<-chan struct{}{notification}) }()

	close(notification)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("partition notification did not wake consume wait")
	}
}

func TestEffectiveConsumeWaitUsesRemainingRequestDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	wait, err := effectiveConsumeWait(ctx, time.Minute)
	require.NoError(t, err)
	assert.Positive(t, wait)
	assert.LessOrEqual(t, wait, 100*time.Millisecond)
}

func cloneStringMap(source map[string]string) map[string]string {
	cloned := make(map[string]string, len(source)+1)
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func TestConsumeNotificationDeadlineIsAnElapsedWait(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	require.ErrorIs(t, waitForConsumeNotification(ctx, time.Hour, nil), errConsumeWaitElapsed)
	canceled, stop := context.WithCancel(context.Background())
	stop()
	require.ErrorIs(t, waitForConsumeNotification(canceled, time.Hour, nil), context.Canceled)
}

func TestConsumeRetainsScanProgressThroughFilteredPages(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	storage := &authTestStorage{messages: []types.Message{
		{Offset: 0, Payload: "invisible", TransactionalID: "aborted", TransactionState: types.TransactionStateAborted},
		{Offset: 1, Payload: "visible"},
	}}
	topics := topic.NewTopicManager(cfg, &authStorageProvider{storage: storage}, nil)
	require.NoError(t, topics.CreateTopic("scan-progress", 1, false, false))
	handler := NewCommandHandler(topics, cfg, nil, nil, nil)
	defer func() { _ = handler.Close() }()
	partition, err := topics.GetTopic("scan-progress").GetPartition(0)
	require.NoError(t, err)
	partition.SetHWM(2)
	ctx := NewClientContext("g", 0)
	args := CommonArgs{TopicName: "scan-progress", PartitionID: 0, HasOffset: true, Offset: 0}
	first, _, err := handler.readFromTopicBounded("scan-progress", args, ctx, 1, 1, true)
	require.NoError(t, err)
	require.Empty(t, first)
	require.Equal(t, uint64(1), ctx.OffsetCache[consumerOffsetCacheKey("scan-progress", args)])
	next, _, err := handler.readFromTopicBounded("scan-progress", args, ctx, 1, 1, true)
	require.NoError(t, err)
	require.Len(t, next, 1)
	require.Equal(t, "visible", next[0].Payload)
}

// A small fixture models a disk page that consumes the complete byte budget.
type fullPageStorage struct{ *authTestStorage }

func (s *fullPageStorage) ReadMessagesBounded(offset uint64, max, maxBytes int, allow bool) ([]types.Message, int, error) {
	messages, err := s.ReadMessages(offset, 1)
	if len(messages) == 0 {
		return messages, 0, err
	}
	return messages, maxBytes, err
}

func TestStreamAdvancesPastEmptyFilteredPage(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	storage := &fullPageStorage{&authTestStorage{messages: []types.Message{
		{Offset: 0, Payload: "hidden", TransactionalID: "tx", TransactionState: types.TransactionStateAborted},
		{Offset: 1, Payload: "visible"},
	}}}
	topics := topic.NewTopicManager(cfg, &authStorageProvider{storage: storage}, nil)
	require.NoError(t, topics.CreateTopic("scan-stream", 1, false, false))
	partition, err := topics.GetTopic("scan-stream").GetPartition(0)
	require.NoError(t, err)
	partition.SetHWM(2)
	manager := stream.NewStreamManager(1, time.Minute)
	handler := NewCommandHandler(topics, cfg, nil, manager, nil)
	defer func() { _ = handler.Close() }()
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer manager.RemoveStream("scan-stream:0:g")
	defer func() { _ = client.Close() }()
	require.NoError(t, handler.HandleStreamCommand(server, "STREAM topic=scan-stream partition=0 group=g batch=1", NewClientContext("g", 0)))
	require.NoError(t, client.SetReadDeadline(time.Now().Add(2*time.Second)))
	payload, err := util.ReadWithLength(client)
	require.NoError(t, err)
	batch, err := util.DecodeBatchMessages(payload)
	require.NoError(t, err)
	require.Len(t, batch.Messages, 1)
	require.Equal(t, "visible", batch.Messages[0].Payload)
	require.Equal(t, uint64(1), batch.Messages[0].Offset)
}
