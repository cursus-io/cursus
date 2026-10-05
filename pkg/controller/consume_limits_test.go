package controller

import (
	"context"
	"errors"
	"math"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
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
