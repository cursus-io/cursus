package sdk

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/protocol"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func cooperativeCommitFixture(t *testing.T) (*CooperativeConsumer, *wire.Connection) {
	t.Helper()
	cfg := NewDefaultConsumerConfig()
	cfg.Topic, cfg.GroupID = "orders", "workers"
	cfg.RequestTimeoutMS = 1000
	c, err := NewCooperativeConsumer(cfg)
	require.NoError(t, err)
	client, server := net.Pipe()
	require.NoError(t, server.SetDeadline(time.Now().Add(3*time.Second)))
	t.Cleanup(func() { c.transport.rootCancel(); _ = client.Close(); _ = server.Close() })
	ready := make(chan *wire.Connection, 1)
	go func() {
		framed, _ := wire.ServerHandshake(server, []wire.Compression{wire.CompressionNone})
		ready <- framed
	}()
	c.operationsRPC.conn, err = openWireConnection(client, 1000, "none")
	require.NoError(t, err)
	framed := <-ready
	require.NotNil(t, framed)
	return c, framed
}

func cooperativeCommitRecord(partition int) CooperativeMessage {
	return CooperativeMessage{
		Message: Message{Partition: partition, Offset: 7}, Group: "workers",
		Ownership: protocol.CooperativeToken{RegistrationEpoch: 1, Grant: protocol.CooperativeGrant{
			Partition: protocol.CooperativePartition{Topic: "orders", Partition: partition, LifecycleEpoch: 1},
			Member:    "a", Session: 2, Epoch: 2,
		}},
	}
}

func TestCooperativeCommitCoalescesAndIsolatesStaleGrant(t *testing.T) {
	c, server := cooperativeCommitFixture(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, listener.(*net.TCPListener).SetDeadline(time.Now().Add(3*time.Second)))
	t.Cleanup(func() { _ = listener.Close(); c.operationsRPC.close() })
	c.transport.coordinatorAddr = listener.Addr().String()
	requests := make([]*cooperativeCommitRequest, 2)
	for i := range requests {
		r := &cooperativeCommitRequest{ctx: context.Background(), record: cooperativeCommitRecord(i), done: make(chan error, 1)}
		requests[i] = r
		c.workers[i] = &cooperativeWorker{token: r.record.Ownership}
	}
	c.commitPending, c.commitRunning = append([]*cooperativeCommitRequest(nil), requests...), true
	flushed := make(chan struct{})
	go func() { defer close(flushed); c.flushProcessedBatches() }()
	request, err := server.ReadFrame()
	require.NoError(t, err)
	command, err := decodeWireTestCommand(request)
	require.NoError(t, err)
	require.Contains(t, command, "BATCH_COMMIT")
	require.Contains(t, command, "offsets=P0:8,P1:8")
	require.NoError(t, writeWireTestResponse(server, request, "ERROR: cooperative_fenced"))
	raw, err := listener.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	require.NoError(t, raw.SetDeadline(time.Now().Add(3*time.Second)))
	server, request, command, err = acceptWireTestRequest(raw)
	require.NoError(t, err)
	require.Contains(t, command, "partition=0")
	require.NoError(t, writeWireTestResponse(server, request, "OK"))
	request, err = server.ReadFrame()
	require.NoError(t, err)
	command, err = decodeWireTestCommand(request)
	require.NoError(t, err)
	require.Contains(t, command, "partition=1")
	require.NoError(t, writeWireTestResponse(server, request, "ERROR: cooperative_fenced"))
	require.NoError(t, <-requests[0].done)
	require.ErrorContains(t, <-requests[1].done, "cooperative_fenced")
	<-flushed
	require.Equal(t, uint64(3), c.Stats().AutoCommitRequests)
	require.Equal(t, uint64(4), c.Stats().AutoCommitPartitions)
}

func TestCooperativeCommitWaitsForInflightBeforeRevocationAck(t *testing.T) {
	c, server := cooperativeCommitFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.commitProcessedBatch(ctx, cooperativeCommitRecord(0)) }()
	request, err := server.ReadFrame()
	require.NoError(t, err)
	cancel()
	select {
	case err := <-done:
		t.Fatalf("revoked worker overtook in-flight commit: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, writeWireTestResponse(server, request, "OK batched=1"))
	require.NoError(t, <-done)
	require.Equal(t, uint64(1), c.Stats().AutoCommitRequests)
}

func TestCooperativeCommitSkipsCancelledQueue(t *testing.T) {
	c, _ := cooperativeCommitFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, c.commitProcessedBatch(ctx, cooperativeCommitRecord(0)), context.Canceled)
	require.Zero(t, c.Stats().AutoCommitRequests)
}

func TestCooperativeCommitLostResponseRetriesOriginalOffsets(t *testing.T) {
	c, _ := cooperativeCommitFixture(t)
	c.operationsRPC.close()
	addr, commands := startSDKCommandServer(t, "", "OK batched=1")
	c.transport.coordinatorAddr = addr
	t.Cleanup(c.operationsRPC.close)
	record := cooperativeCommitRecord(0)
	err := c.commitProcessedBatch(context.Background(), record)
	require.Error(t, err)
	require.True(t, cooperativeRetryable(err), "%v", err)
	first := requireSDKCommand(t, commands)
	require.NoError(t, c.commitProcessedBatch(context.Background(), record))
	require.Equal(t, first, requireSDKCommand(t, commands), "response loss cannot substitute newer offsets or ownership")
}
