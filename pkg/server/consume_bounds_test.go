package server

import (
	"context"
	"math"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWireConsumeRejectsMaxIntBatchBeforeStorageRead(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	handler := newPublishTestHandler(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			handleConn(context.Background(), connection, handler)
		}
	}()

	connection, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	client := newWireTestClient(t, connection)
	response := wireRequest(t, client, wire.CommandConsume, []byte("CONSUME topic=ack-zero partition=0 offset=0 group=g member=m batch="+strconv.Itoa(math.MaxInt)))
	assert.Equal(t, "ERROR: fetch_batch_too_large", response)
	require.NoError(t, connection.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection worker did not exit")
	}
}

func TestWireConsumeLongPollStopsWhenPeerDisconnects(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	handler := newPublishTestHandler(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			handleConn(context.Background(), connection, handler)
		}
	}()

	connection, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	client := newWireTestClient(t, connection)
	writeWireCommand(t, client, wire.CommandConsume, 7, "CONSUME topic=ack-zero partition=0 offset=0 group=g member=m batch=1 wait_ms=30000")
	time.Sleep(20 * time.Millisecond)
	started := time.Now()
	require.NoError(t, connection.Close())

	select {
	case <-done:
		assert.Less(t, time.Since(started), 500*time.Millisecond)
	case <-time.After(time.Second):
		t.Fatal("long poll worker did not exit after peer disconnect")
	}
}

func TestWireConsumeLongPollWakesOnPartitionNotification(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	handler := newPublishTestHandler(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			handleConn(context.Background(), connection, handler)
		}
	}()

	connection, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	client := newWireTestClient(t, connection)
	partition, err := handler.TopicManager.GetTopic("ack-zero").GetPartition(0)
	require.NoError(t, err)
	started := time.Now()
	writeWireCommand(t, client, wire.CommandConsume, 8, "CONSUME topic=ack-zero partition=0 offset=0 group=g member=m batch=1 wait_ms=30000")
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, partition.EnqueueSync(types.Message{Payload: "ready"}))
	frame, err := client.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, wire.StatusOK, frame.Status)
	assert.Less(t, time.Since(started), 500*time.Millisecond)
	batch, err := wire.DecodeBatch(frame.Payload)
	require.NoError(t, err)
	require.Len(t, batch.Messages, 1)
	assert.Equal(t, "ready", batch.Messages[0].Payload)

	require.NoError(t, connection.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection worker did not exit")
	}
}

func TestWireConsumeLongPollClampsToRequestDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	handler := newPublishTestHandler(t)
	handler.Config.ClientRequestTimeoutMS = 50
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			handleConn(context.Background(), connection, handler)
		}
	}()

	connection, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	client := newWireTestClient(t, connection)
	started := time.Now()
	response := wireRequest(t, client, wire.CommandConsume, []byte("CONSUME topic=ack-zero partition=0 offset=0 group=g member=m batch=1 wait_ms=30000"))
	assert.Less(t, time.Since(started), 500*time.Millisecond)
	require.True(t, wire.IsBatch([]byte(response)))
	batch, err := wire.DecodeBatch([]byte(response))
	require.NoError(t, err)
	assert.Empty(t, batch.Messages)
	require.NoError(t, connection.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection worker did not exit")
	}
}

func TestWireConnectionPreservesPipelinedRequestOrder(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	handler := newPublishTestHandler(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			handleConn(context.Background(), connection, handler)
		}
	}()

	connection, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	client := newWireTestClient(t, connection)
	writeWireCommand(t, client, wire.CommandHelp, 41, "HELP")
	writeWireCommand(t, client, wire.CommandHelp, 42, "HELP")

	for _, requestID := range []uint64{41, 42} {
		frame, readErr := client.ReadFrame()
		require.NoError(t, readErr)
		assert.Equal(t, requestID, frame.RequestID)
		assert.Equal(t, wire.CommandHelp, frame.Command)
		assert.Equal(t, wire.StatusOK, frame.Status)
		assert.NotEmpty(t, frame.Payload)
	}

	require.NoError(t, connection.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection worker did not exit")
	}
}

func writeWireCommand(t *testing.T, client *wire.Connection, command wire.Command, requestID uint64, text string) {
	t.Helper()
	parsedCommand, payload, err := wire.ParseCommandText(text)
	require.NoError(t, err)
	require.Equal(t, command, parsedCommand)
	encoded, err := wire.EncodeCommandPayload(payload)
	require.NoError(t, err)
	require.NoError(t, client.WriteFrame(wire.Frame{Kind: wire.KindRequest, Command: command, RequestID: requestID, Payload: encoded}))
}
