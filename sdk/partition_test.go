package sdk

import (
	"errors"
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
	c.state.Store(uint32(ConsumerStateRunning))
	c.assignmentGeneration.Store(1)
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

	assert.False(t, pc.handleBrokerError(errors.New("transport failure")))
	assert.False(t, pc.handleBrokerError(nil))
}

func TestPartitionConsumer_HandleBrokerError_GenericError(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	assert.True(t, pc.handleBrokerError(&BrokerError{Code: "broker_error", Class: ErrorClassInternal}))
	assert.Nil(t, pc.conn)
}

func TestPartitionConsumer_HandleBrokerError_NotLeader(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	assert.True(t, pc.handleBrokerError(&BrokerError{
		Code: "NOT_LEADER", Class: ErrorClassRouting, Retryable: true,
		Fields: map[string]string{"leader": "broker-2:9000"},
	}))

	assert.Equal(t, "broker-2:9000", c.getPartitionLeaderAddr(0))
}

func TestPartitionConsumer_HandleBrokerError_GenMismatch(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	result := pc.handleBrokerError(&BrokerError{Code: "GEN_MISMATCH", Class: ErrorClassFencing})
	assert.True(t, result)
	assert.True(t, pc.closed)
}

func TestPartitionConsumer_HandleBrokerError_RebalanceRequired(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	result := pc.handleBrokerError(&BrokerError{Code: "REBALANCE_REQUIRED", Class: ErrorClassFencing})
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
	c.state.Store(uint32(ConsumerStateRebalancing))
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
	c.state.Store(uint32(ConsumerStateRebalancing))
	c.commitMu.Lock()
	c.commitRetryMap[0] = retryCommit{offset: 100, assignmentGeneration: 1}
	c.commitMu.Unlock()

	c.processRetryQueue()

	c.commitMu.Lock()
	assert.Equal(t, uint64(100), c.commitRetryMap[0].offset)
	c.commitMu.Unlock()
}

func TestConsumer_ProcessRetryQueue_DropsSupersededOffset(t *testing.T) {
	c := newTestConsumer(t)
	c.commitMu.Lock()
	c.commitRetryMap[0] = retryCommit{offset: 13, assignmentGeneration: 1}
	c.commitMu.Unlock()
	c.recordCommittedOffsets(map[int]uint64{0: 35}, 1)

	c.processRetryQueue()

	c.commitMu.Lock()
	assert.Empty(t, c.commitRetryMap, "committed offset 35 supersedes retry 13")
	c.commitMu.Unlock()
}

func TestConsumer_DropSupersededCommitRetries_PreservesEqualAndUnknown(t *testing.T) {
	c := newTestConsumer(t)
	c.mu.Lock()
	c.offsets[0] = 35
	c.offsets[1] = 35
	c.mu.Unlock()
	retries := map[int]uint64{0: 13, 1: 35, 2: 0}

	c.dropSupersededCommitRetries(retries)

	assert.Equal(t, map[int]uint64{1: 35, 2: 0}, retries)
}

func TestConsumer_Close_AlreadyClosed(t *testing.T) {
	c := newTestConsumer(t)

	assert.NoError(t, c.Close())
	assert.NoError(t, c.Close())
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

func TestEffectivePollBatchSize(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ConsumerConfig
		want int
	}{
		{
			name: "batch size below max poll records",
			cfg:  &ConsumerConfig{BatchSize: 100, MaxPollRecords: 500},
			want: 100,
		},
		{
			name: "max poll records caps batch size",
			cfg:  &ConsumerConfig{BatchSize: 5000, MaxPollRecords: 1000},
			want: 1000,
		},
		{
			name: "max poll records supplies missing batch size",
			cfg:  &ConsumerConfig{BatchSize: 0, MaxPollRecords: 250},
			want: 250,
		},
		{
			name: "fallback",
			cfg:  &ConsumerConfig{},
			want: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, effectivePollBatchSize(tt.cfg))
		})
	}
}

func TestEffectiveStreamBatchSize(t *testing.T) {
	assert.Equal(t, 500, effectiveStreamBatchSize(&ConsumerConfig{BatchSize: 500}))
	assert.Equal(t, 100, effectiveStreamBatchSize(&ConsumerConfig{}))
}

func TestParseStreamControlFrameClose(t *testing.T) {
	frame, ok := parseStreamControlFrame([]byte("STREAM_CONTROL type=CLOSE reason=timeout offset=123"))
	require.True(t, ok)
	assert.Equal(t, "CLOSE", frame.Type)
	assert.Equal(t, "timeout", frame.Reason)
	assert.True(t, frame.HasOffset)
	assert.Equal(t, uint64(123), frame.Offset)
}

func TestPartitionConsumer_HandleStreamControlClose(t *testing.T) {
	c := newTestConsumer(t)
	server, client := createPipe()
	defer func() { _ = server.Close() }()

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		conn:        client,
	}

	assert.True(t, pc.handleStreamControl([]byte("STREAM_CONTROL type=CLOSE reason=removed offset=456")))
	assert.Nil(t, pc.conn)
	assert.Equal(t, uint64(456), atomic.LoadUint64(&pc.fetchOffset))
}

func TestPartitionConsumer_HandleBrokerError_NotOwner(t *testing.T) {
	c := newTestConsumer(t)

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
	}

	result := pc.handleBrokerError(&BrokerError{Code: "NOT_OWNER", Class: ErrorClassFencing})
	assert.True(t, result)
	assert.True(t, pc.closed)
}

func TestParseOffsetOutOfRangeFrame(t *testing.T) {
	frame, ok := brokerOffsetOutOfRangeFrame(offsetOutOfRangeBrokerError())
	require.True(t, ok)
	assert.Equal(t, uint64(1), frame.Requested)
	assert.Equal(t, uint64(5), frame.Earliest)
	assert.Equal(t, uint64(9), frame.Latest)
}

func TestPartitionConsumer_HandleBrokerError_OffsetOutOfRangeEarliest(t *testing.T) {
	c := newTestConsumer(t)
	c.config.AutoOffsetReset = AutoOffsetResetEarliest
	pc := &PartitionConsumer{partitionID: 0, consumer: c, fetchOffset: 1}

	result := pc.handleBrokerError(offsetOutOfRangeBrokerError())
	assert.True(t, result)
	assert.Equal(t, uint64(5), atomic.LoadUint64(&pc.fetchOffset))
	c.mu.RLock()
	assert.Equal(t, uint64(5), c.offsets[0])
	c.mu.RUnlock()
}

func TestPartitionConsumer_HandleBrokerError_OffsetOutOfRangeLatest(t *testing.T) {
	c := newTestConsumer(t)
	c.config.AutoOffsetReset = AutoOffsetResetLatest
	pc := &PartitionConsumer{partitionID: 0, consumer: c, fetchOffset: 1}

	result := pc.handleBrokerError(offsetOutOfRangeBrokerError())
	assert.True(t, result)
	assert.Equal(t, uint64(9), atomic.LoadUint64(&pc.fetchOffset))
}

func TestPartitionConsumer_HandleBrokerError_OffsetOutOfRangeError(t *testing.T) {
	c := newTestConsumer(t)
	c.config.AutoOffsetReset = AutoOffsetResetError
	pc := &PartitionConsumer{partitionID: 0, consumer: c, fetchOffset: 1}

	result := pc.handleBrokerError(offsetOutOfRangeBrokerError())
	assert.True(t, result)
	assert.Error(t, c.mainCtx.Err())
	assert.Error(t, c.rootCtx.Err())
	assert.Equal(t, ConsumerStateFailed, c.State())
	var offsetErr *ConsumerOffsetOutOfRangeError
	require.ErrorAs(t, c.Err(), &offsetErr)
	assert.Equal(t, 0, offsetErr.Partition)
	assert.Equal(t, uint64(1), offsetErr.Requested)
	assert.Equal(t, uint64(5), offsetErr.Earliest)
	assert.Equal(t, uint64(9), offsetErr.Latest)
	assert.Equal(t, uint64(1), atomic.LoadUint64(&pc.fetchOffset))
	select {
	case <-c.rebalanceSig:
		t.Fatal("offset failure unexpectedly requested a rebalance")
	default:
	}
}

func offsetOutOfRangeBrokerError() *BrokerError {
	return &BrokerError{
		Code: "OFFSET_OUT_OF_RANGE", Class: ErrorClassConflict,
		Fields: map[string]string{"requested": "1", "earliest": "5", "latest": "9"},
	}
}

func TestPartitionConsumer_HandleStreamControl_OffsetOutOfRange(t *testing.T) {
	c := newTestConsumer(t)
	c.config.AutoOffsetReset = AutoOffsetResetEarliest
	pc := &PartitionConsumer{partitionID: 0, consumer: c, fetchOffset: 1}

	result := pc.handleStreamControl([]byte("STREAM_CONTROL type=CLOSE reason=offset_out_of_range offset=1 requested=1 earliest=5 latest=9"))
	assert.True(t, result)
	assert.Equal(t, uint64(5), atomic.LoadUint64(&pc.fetchOffset))
}

func TestPartitionConsumer_HandleStreamControl_OffsetOutOfRangeError(t *testing.T) {
	c := newTestConsumer(t)
	c.config.AutoOffsetReset = AutoOffsetResetError
	pc := &PartitionConsumer{partitionID: 2, consumer: c, fetchOffset: 4}

	result := pc.handleStreamControl([]byte("STREAM_CONTROL type=CLOSE reason=offset_out_of_range offset=4 requested=4 earliest=8 latest=12"))
	require.True(t, result)
	require.Equal(t, ConsumerStateFailed, c.State())
	var offsetErr *ConsumerOffsetOutOfRangeError
	require.ErrorAs(t, c.Err(), &offsetErr)
	require.Equal(t, 2, offsetErr.Partition)
	require.Equal(t, uint64(4), atomic.LoadUint64(&pc.fetchOffset))
}

func TestPartitionConsumer_HandlerFailureStopsConsumerWithoutCommitOrRebalance(t *testing.T) {
	c := newTestConsumer(t)
	c.config.HandlerMaxRetries = 2
	c.config.HandlerRetryBackoff = time.Millisecond
	c.config.HandlerRetryMaxBackoff = time.Millisecond
	c.mu.Lock()
	c.offsets[0] = 3
	c.mu.Unlock()
	attempts := 0
	c.MessageHandler = func(Message) error {
		attempts++
		return assert.AnError
	}

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		fetchOffset: 6,
		dataCh:      make(chan *messageBatch, 1),
	}
	c.wg.Add(1)
	go pc.runWorker()
	pc.dataCh <- &messageBatch{
		topic: "test-topic",
		messages: []Message{
			{Offset: 3, Payload: "first"},
			{Offset: 4, Payload: "second"},
		},
	}

	select {
	case <-c.rootCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("handler failure did not stop the consumer")
	}
	c.wg.Wait()

	assert.Equal(t, 3, attempts)
	assert.Equal(t, uint64(3), atomic.LoadUint64(&pc.fetchOffset))
	assert.Equal(t, ConsumerStateFailed, c.State())
	var handlerErr *ConsumerHandlerError
	require.ErrorAs(t, c.Err(), &handlerErr)
	assert.Equal(t, 0, handlerErr.Partition)
	assert.Equal(t, uint64(3), handlerErr.Offset)
	assert.Equal(t, 3, handlerErr.Attempts)
	assert.ErrorIs(t, handlerErr, assert.AnError)
	select {
	case <-c.rebalanceSig:
		t.Fatal("handler failure unexpectedly requested a rebalance")
	default:
	}
	select {
	case commit := <-c.commitCh:
		t.Fatalf("handler failure unexpectedly queued commit: %+v", commit)
	default:
	}
}

func TestPartitionConsumerNilHandlerDoesNotAdvanceOrCommit(t *testing.T) {
	c := newTestConsumer(t)
	c.mu.Lock()
	c.offsets[0] = 41
	c.mu.Unlock()

	pc := &PartitionConsumer{
		partitionID:          0,
		consumer:             c,
		fetchOffset:          42,
		assignmentGeneration: c.assignmentGeneration.Load(),
		dataCh:               make(chan *messageBatch, 1),
	}
	c.wg.Add(1)
	go pc.runWorker()
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 41, Payload: "must-not-be-discarded"}}}

	select {
	case <-c.rebalanceSig:
	case <-time.After(time.Second):
		t.Fatal("nil handler did not stop the partition worker")
	}
	c.wg.Wait()

	assert.Equal(t, uint64(41), atomic.LoadUint64(&pc.fetchOffset))
	assert.Equal(t, uint64(0), atomic.LoadUint64(&pc.commitOffset))
	select {
	case commit := <-c.commitCh:
		t.Fatalf("nil handler unexpectedly queued commit: %+v", commit)
	default:
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	assert.Equal(t, uint64(41), c.offsets[0])
}

func TestPartitionConsumer_HandlerRetriesLocallyAndContinues(t *testing.T) {
	c := newTestConsumer(t)
	c.config.EnableAutoCommit = false
	c.config.HandlerMaxRetries = 2
	c.config.HandlerRetryBackoff = time.Millisecond
	c.config.HandlerRetryMaxBackoff = time.Millisecond
	attempts := 0
	c.MessageHandler = func(Message) error {
		attempts++
		if attempts < 3 {
			return assert.AnError
		}
		return nil
	}

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		dataCh:      make(chan *messageBatch, 1),
	}
	c.wg.Add(1)
	go pc.runWorker()
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 7, Payload: "transient"}}}
	close(pc.dataCh)
	c.wg.Wait()

	assert.Equal(t, 3, attempts)
	assert.NoError(t, c.Err())
	assert.NoError(t, c.rootCtx.Err())
	assert.Equal(t, ConsumerStateRunning, c.State())
	select {
	case <-c.rebalanceSig:
		t.Fatal("transient handler error unexpectedly requested a rebalance")
	default:
	}
}

func TestPartitionConsumer_ShutdownDuringHandlerBackoffIsNotFatal(t *testing.T) {
	c := newTestConsumer(t)
	c.config.HandlerMaxRetries = 10
	c.config.HandlerRetryBackoff = time.Second
	c.config.HandlerRetryMaxBackoff = time.Second
	handlerCalled := make(chan struct{})
	var attempts atomic.Int32
	c.MessageHandler = func(Message) error {
		if attempts.Add(1) == 1 {
			close(handlerCalled)
		}
		return assert.AnError
	}

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		dataCh:      make(chan *messageBatch, 1),
	}
	c.wg.Add(1)
	go pc.runWorker()
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 0, Payload: "shutdown"}}}
	select {
	case <-handlerCalled:
	case <-time.After(time.Second):
		t.Fatal("handler was not called")
	}

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- c.Close() }()
	select {
	case err := <-closeReturned:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt handler retry backoff")
	}
	require.Equal(t, int32(1), attempts.Load())
	require.NoError(t, c.Err())
	require.Equal(t, ConsumerStateClosed, c.State())
}

func TestPartitionConsumerManualCommitDoesNotQueueCommit(t *testing.T) {
	c := newTestConsumer(t)
	c.config.EnableAutoCommit = false
	c.MessageHandler = func(Message) error { return nil }

	pc := &PartitionConsumer{
		partitionID: 0,
		consumer:    c,
		dataCh:      make(chan *messageBatch, 1),
	}
	c.wg.Add(1)
	go pc.runWorker()
	pc.dataCh <- &messageBatch{messages: []Message{{Offset: 7, Payload: "manual"}}}
	close(pc.dataCh)
	c.wg.Wait()

	select {
	case commit := <-c.commitCh:
		t.Fatalf("manual commit unexpectedly queued: %+v", commit)
	default:
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.offsets[0]; ok {
		t.Fatalf("manual commit unexpectedly advanced stored offset to %d", c.offsets[0])
	}
}

func TestConsumerCommitOffsetRejectsUnassignedPartition(t *testing.T) {
	c := newTestConsumer(t)
	err := c.CommitOffset(9, 10)
	assert.EqualError(t, err, "partition 9 is not assigned to this consumer")
}

func TestConsumerCommitOffsetRejectsZeroOffset(t *testing.T) {
	c := newTestConsumer(t)
	err := c.CommitOffset(0, 0)
	assert.EqualError(t, err, "commit offset must be greater than zero")
}
