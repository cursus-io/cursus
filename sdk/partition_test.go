package sdk

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createPipe() (net.Conn, net.Conn) {
	return net.Pipe()
}

func newTestConsumer(t *testing.T) *Consumer {
	t.Helper()
	cfg := NewDefaultConsumerConfig()
	c, err := NewConsumer(cfg)
	require.NoError(t, err)
	return c
}

func TestPartitionConsumer_Close(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	pc.close()
	assert.True(t, pc.closed)
	assert.Nil(t, pc.conn)
}

func TestPartitionConsumer_CloseIdempotent(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	pc.close()
	pc.close()
	assert.True(t, pc.closed)
}

func TestPartitionConsumer_CloseConnection(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	pc.closeConnection()
	assert.Nil(t, pc.conn)
	assert.False(t, pc.closed)
}

func TestPartitionConsumer_HandleBrokerError_NotError(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	assert.False(t, pc.handleBrokerError([]byte("OK")))
	assert.False(t, pc.handleBrokerError([]byte("")))
	assert.False(t, pc.handleBrokerError([]byte("some data")))
}

func TestPartitionConsumer_HandleBrokerError_GenericError(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	assert.True(t, pc.handleBrokerError([]byte("ERROR: something went wrong")))
	assert.Nil(t, pc.conn)
}

func TestPartitionConsumer_HandleBrokerError_NotLeader(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	assert.True(t, pc.handleBrokerError([]byte("ERROR NOT_LEADER LEADER_IS broker-2:9000")))

	info := c.client.leader.Load()
	assert.Equal(t, "broker-2:9000", info.addr)
}

func TestPartitionConsumer_HandleBrokerError_GenMismatch(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	result := pc.handleBrokerError([]byte("ERROR GEN_MISMATCH"))
	assert.True(t, result)
	assert.True(t, pc.closed)
}

func TestPartitionConsumer_HandleBrokerError_RebalanceRequired(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	result := pc.handleBrokerError([]byte("ERROR REBALANCE_REQUIRED"))
	assert.True(t, result)
	assert.True(t, pc.closed)
}

func TestPartitionConsumer_GetBackoff(t *testing.T) {
	c := newTestConsumer(t)
	c.config.ConnectRetryBackoffMS = 500

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	bo := pc.getBackoff()
	assert.NotNil(t, bo)
	assert.Equal(t, bo, pc.bo)

	bo2 := pc.getBackoff()
	assert.Equal(t, bo, bo2)
}

func TestPartitionConsumer_GetBackoff_MinClamped(t *testing.T) {
	c := newTestConsumer(t)
	c.config.ConnectRetryBackoffMS = 10

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	bo := pc.getBackoff()
	assert.NotNil(t, bo)
}

func TestPartitionConsumer_WaitWithBackoff_CancelledContext(t *testing.T) {
	c := newTestConsumer(t)
	c.mainCancel()

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	bo := newBackoff(10*time.Second, 30*time.Second)
	assert.False(t, pc.waitWithBackoff(bo))
}

func TestPartitionConsumer_WaitDuration_CancelledContext(t *testing.T) {
	c := newTestConsumer(t)
	c.mainCancel()

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	assert.False(t, pc.waitDuration(10*time.Second))
}

func TestPartitionConsumer_EnsureConnection_ShuttingDown(t *testing.T) {
	c := newTestConsumer(t)
	c.mainCancel()

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	err := pc.ensureConnection()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consumer shutting down")
}

func TestPartitionConsumer_EnsureConnection_AlreadyClosed(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		closed:      true,
	}

	err := pc.ensureConnection()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrConsumerClosed)
}

func TestPartitionConsumer_PrintConsumedMessage_Empty(t *testing.T) {
	c := newTestConsumer(t)
	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	pc.PrintConsumedMessage(&messageBatch{})
}

func TestPartitionConsumer_PrintConsumedMessage_FewMessages(t *testing.T) {
	c := newTestConsumer(t)
	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	batch := &messageBatch{
		topic: "test-topic",
		messages: []Message{
			{Payload: "msg1", Offset: 1},
			{Payload: "msg2", Offset: 2, Key: "key1"},
		},
	}
	pc.PrintConsumedMessage(batch)
}

func TestPartitionConsumer_PrintConsumedMessage_LongPayload(t *testing.T) {
	c := newTestConsumer(t)
	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	longPayload := "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ12345"
	batch := &messageBatch{
		topic: "test-topic",
		messages: []Message{
			{Payload: longPayload, Offset: 1},
		},
	}
	pc.PrintConsumedMessage(batch)
}

func TestPartitionConsumer_PrintConsumedMessage_MoreThanFive(t *testing.T) {
	c := newTestConsumer(t)
	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	msgs := make([]Message, 8)
	for i := range msgs {
		msgs[i] = Message{Payload: "data", Offset: uint64(i)}
	}
	batch := &messageBatch{
		topic:    "test-topic",
		messages: msgs,
	}
	pc.PrintConsumedMessage(batch)
}

func TestPartitionConsumer_CloseDataCh_NilChannel(t *testing.T) {
	c := newTestConsumer(t)
	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}
	pc.closeDataCh()
}

func TestPartitionConsumer_CloseDataCh_WithChannel(t *testing.T) {
	c := newTestConsumer(t)
	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		dataCh:      make(chan *messageBatch, 10),
	}
	pc.closeDataCh()
	pc.closeDataCh()
}

func TestConsumer_ResetHeartbeatConn_NilConn(t *testing.T) {
	c := newTestConsumer(t)
	c.resetHeartbeatConn()
	assert.Nil(t, c.hbConn)
}

func TestConsumer_ValidateCommitConn_Nil(t *testing.T) {
	c := newTestConsumer(t)
	assert.False(t, c.validateCommitConn())
}

func TestConsumer_FlushOffsets_Empty(t *testing.T) {
	c := newTestConsumer(t)
	c.flushOffsets()
}

func TestConsumer_FlushOffsets_DuringRebalance(t *testing.T) {
	c := newTestConsumer(t)
	atomic.StoreInt32(&c.rebalancing, 1)
	c.offsetsMu.Lock()
	c.currentOffsets[0] = 100
	c.offsetsMu.Unlock()
	c.flushOffsets()

	c.offsetsMu.Lock()
	assert.Equal(t, uint64(100), c.currentOffsets[0])
	c.offsetsMu.Unlock()
}

func TestConsumer_FlushOffsets_WithOffsets(t *testing.T) {
	c := newTestConsumer(t)

	c.offsetsMu.Lock()
	c.currentOffsets[0] = 100
	c.offsetsMu.Unlock()

	c.mu.Lock()
	c.offsets[0] = 50
	c.mu.Unlock()

	c.flushOffsets()

	c.offsetsMu.Lock()
	assert.Empty(t, c.currentOffsets)
	c.offsetsMu.Unlock()
}

func TestConsumer_FlushOffsets_NoAdvance(t *testing.T) {
	c := newTestConsumer(t)

	c.offsetsMu.Lock()
	c.currentOffsets[0] = 50
	c.offsetsMu.Unlock()

	c.mu.Lock()
	c.offsets[0] = 100
	c.mu.Unlock()

	c.flushOffsets()

	c.offsetsMu.Lock()
	assert.Empty(t, c.currentOffsets)
	c.offsetsMu.Unlock()
}

func TestConsumer_ProcessRetryQueue_Empty(t *testing.T) {
	c := newTestConsumer(t)
	c.processRetryQueue()
}

func TestConsumer_ProcessRetryQueue_DuringRebalance(t *testing.T) {
	c := newTestConsumer(t)
	atomic.StoreInt32(&c.rebalancing, 1)
	c.commitMu.Lock()
	c.commitRetryMap[0] = 100
	c.commitMu.Unlock()

	c.processRetryQueue()

	c.commitMu.Lock()
	assert.Equal(t, uint64(100), c.commitRetryMap[0])
	c.commitMu.Unlock()
}

func TestConsumer_Close_AlreadyClosed(t *testing.T) {
	c := newTestConsumer(t)
	ctx, cancel := context.WithCancel(context.Background())
	c.mainCtx = ctx
	c.mainCancel = cancel

	atomic.StoreInt32(&c.closed, 1)

	err := c.Close()
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrConsumerClosed)
}

func TestPartitionConsumer_CloseWithConn(t *testing.T) {
	c := newTestConsumer(t)

	server, client := createPipe()
	defer func() { _ = server.Close() }()

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		conn:        client,
	}

	pc.close()
	assert.True(t, pc.closed)
	assert.Nil(t, pc.conn)
}

func TestPartitionConsumer_CloseConnectionWithConn(t *testing.T) {
	c := newTestConsumer(t)

	server, client := createPipe()
	defer func() { _ = server.Close() }()

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		conn:        client,
	}

	pc.closeConnection()
	assert.Nil(t, pc.conn)
	assert.False(t, pc.closed)
}

func TestConsumer_ResetHeartbeatConn_WithConn(t *testing.T) {
	c := newTestConsumer(t)

	server, client := createPipe()
	defer func() { _ = server.Close() }()

	c.hbConn = client
	c.resetHeartbeatConn()
	assert.Nil(t, c.hbConn)
}

func TestConsumer_CleanupHbConn(t *testing.T) {
	c := newTestConsumer(t)

	server, client := createPipe()
	defer func() { _ = server.Close() }()

	c.hbConn = client
	c.cleanupHbConn(client)
	assert.Nil(t, c.hbConn)
}

func TestConsumer_CleanupHbConn_DifferentConn(t *testing.T) {
	c := newTestConsumer(t)

	server1, client1 := createPipe()
	defer func() { _ = server1.Close() }()

	server2, client2 := createPipe()
	defer func() { _ = server2.Close() }()

	c.hbConn = client1
	c.cleanupHbConn(client2)
	assert.Equal(t, client1, c.hbConn)
}

func TestPartitionConsumer_WaitWithBackoff_DoneChan(t *testing.T) {
	c := newTestConsumer(t)
	close(c.doneCh)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	bo := newBackoff(10*time.Second, 30*time.Second)
	assert.False(t, pc.waitWithBackoff(bo))
}

func TestPartitionConsumer_WaitDuration_DoneChan(t *testing.T) {
	c := newTestConsumer(t)
	close(c.doneCh)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	assert.False(t, pc.waitDuration(10*time.Second))
}

func TestConsumer_FlushOffsets_CommitChFull(t *testing.T) {
	c := newTestConsumer(t)

	for i := 0; i < cap(c.commitCh); i++ {
		c.commitCh <- commitEntry{}
	}

	c.offsetsMu.Lock()
	c.currentOffsets[0] = 100
	c.offsetsMu.Unlock()

	c.mu.Lock()
	c.offsets[0] = 50
	c.mu.Unlock()

	c.flushOffsets()

	c.offsetsMu.Lock()
	assert.Empty(t, c.currentOffsets)
	c.offsetsMu.Unlock()
}

func TestNewConsumer_WithMetrics(t *testing.T) {
	cfg := NewDefaultConsumerConfig()
	cfg.EnableMetrics = true
	c, err := NewConsumer(cfg)
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestPartitionConsumer_EnsureConnection_HasConn(t *testing.T) {
	c := newTestConsumer(t)

	server, client := createPipe()
	defer func() { _ = server.Close() }()

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		conn:        client,
	}

	err := pc.ensureConnection()
	assert.NoError(t, err)
}
