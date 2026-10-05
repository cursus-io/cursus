package server

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func startPumpTestConnection(t *testing.T, handler *controller.CommandHandler) (net.Conn, *wire.Connection, <-chan struct{}) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	done := make(chan struct{})
	go func() { defer close(done); handleConn(context.Background(), server, handler) }()
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
	return client, newWireTestClient(t, client), done
}

func TestLongPollSurvivesIdleTimeoutAndIdleResumesAfterResponse(t *testing.T) {
	handler := newPublishTestHandler(t)
	handler.Config.ClientIdleTimeoutMS = 100
	handler.Config.ClientRequestTimeoutMS = 2000
	_, client, done := startPumpTestConnection(t, handler)
	started := time.Now()
	writeWireCommand(t, client, wire.CommandConsume, 1, "CONSUME topic=ack-zero partition=0 offset=0 group=g member=m batch=1 wait_ms=350")
	frame, err := client.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, wire.StatusOK, frame.Status)
	require.GreaterOrEqual(t, time.Since(started), 300*time.Millisecond)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle timer did not resume after response")
	}
}

func TestPipelinedPayloadWaitsForActiveHandler(t *testing.T) {
	handler := newPublishTestHandler(t)
	raw, client, _ := startPumpTestConnection(t, handler)
	writeWireCommand(t, client, wire.CommandConsume, 1, "CONSUME topic=ack-zero partition=0 offset=0 group=g member=m batch=1 wait_ms=300")
	command, args, err := wire.ParseCommandText("HELP")
	require.NoError(t, err)
	payload, err := wire.EncodeCommandPayload(args)
	require.NoError(t, err)
	codec, err := wire.NewCodec(wire.CompressionNone)
	require.NoError(t, err)
	encoded, err := codec.Encode(wire.Frame{Kind: wire.KindRequest, Command: command, RequestID: 2, Payload: payload})
	require.NoError(t, err)
	_, err = raw.Write(encoded[:wire.HeaderSize])
	require.NoError(t, err)
	written := make(chan error, 1)
	go func() { _, err := raw.Write(encoded[wire.HeaderSize:]); written <- err }()
	select {
	case err := <-written:
		t.Fatalf("payload read while CONSUME active: %v", err)
	case <-time.After(80 * time.Millisecond):
	}
	first, err := client.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.RequestID)
	require.NoError(t, <-written)
	second, err := client.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, uint64(2), second.RequestID)
}

func TestPayloadBudgetReservationsAreBoundedAndReusable(t *testing.T) {
	budget := payloadBudget{limit: 16}
	release, err := budget.reserve(12)
	require.NoError(t, err)
	_, err = budget.reserve(5)
	require.Error(t, err)
	release()
	release()
	release, err = budget.reserve(16)
	require.NoError(t, err)
	release()
	require.Zero(t, budget.used)
}

func TestRejectedStreamResumesRequestPump(t *testing.T) {
	handler := newPublishTestHandler(t)
	handler.StreamManager = stream.NewStreamManager(0, time.Second)
	_, client, _ := startPumpTestConnection(t, handler)
	writeWireCommand(t, client, wire.CommandStream, 1, "STREAM topic=ack-zero partition=0 group=g member=m batch=1")
	rejected, err := client.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, wire.StatusError, rejected.Status)
	writeWireCommand(t, client, wire.CommandHelp, 2, "HELP")
	response, err := client.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, uint64(2), response.RequestID)
	require.Equal(t, wire.StatusOK, response.Status)
}

type observedReadConn struct {
	net.Conn
	mu       sync.Mutex
	reads    int
	deadline time.Time
}

func (c *observedReadConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	c.reads++
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.reads--; c.mu.Unlock() }()
	return c.Conn.Read(p)
}
func (c *observedReadConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(t)
}

func TestStreamHandoffStopsReadsAndClearsDeadline(t *testing.T) {
	handler := newPublishTestHandler(t)
	handler.StreamManager = stream.NewStreamManager(1, time.Minute)
	defer handler.StreamManager.RemoveStream("ack-zero:0:g")
	server, clientConn := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = clientConn.Close() }()
	observed := &observedReadConn{Conn: server}
	done := make(chan struct{})
	go func() { defer close(done); handleConn(context.Background(), observed, handler) }()
	require.NoError(t, clientConn.SetDeadline(time.Now().Add(3*time.Second)))
	client := newWireTestClient(t, clientConn)
	writeWireCommand(t, client, wire.CommandStream, 1, "STREAM topic=ack-zero partition=0 group=g member=m batch=1")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream ownership was not transferred")
	}
	observed.mu.Lock()
	reads, deadline := observed.reads, observed.deadline
	observed.mu.Unlock()
	require.Zero(t, reads)
	require.True(t, deadline.IsZero())
	partition, err := handler.TopicManager.GetTopic("ack-zero").GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, partition.EnqueueSync(types.Message{Payload: "after handoff"}))
	response, err := client.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, wire.KindStream, response.Kind)
	require.Equal(t, wire.StatusOK, response.Status)
}
