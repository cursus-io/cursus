package sdk

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingAckProducer struct {
	producer    *Producer
	requestSeen <-chan struct{}
	releaseAck  func()
	brokerDone  <-chan error
}

func newBlockingAckProducer(t *testing.T, response string) blockingAckProducer {
	t.Helper()
	cfg := NewDefaultPublisherConfig()
	cfg.Topic = "producer-flush-close-race"
	cfg.Partitions = 1
	cfg.BatchSize = 100
	cfg.BufferSize = 10
	cfg.LingerMS = int(time.Hour / time.Millisecond)
	cfg.FlushTimeoutMS = 5_000
	cfg.AckTimeoutMS = 5_000
	cfg.MaxRetries = 0
	cfg.EnableIdempotence = false

	client := mustNewProducerClient(cfg)
	brokerConn, producerConn := net.Pipe()
	requestSeen := make(chan struct{})
	releaseAck := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseAck) })
	brokerDone := make(chan error, 1)
	go func() {
		connection, request, _, err := acceptWireTestRequest(brokerConn)
		if err != nil {
			brokerDone <- err
			return
		}
		messages, _, _, err := DecodeBatchMessages(request.Payload)
		if err != nil {
			brokerDone <- err
			return
		}
		close(requestSeen)
		<-releaseAck
		body := response
		if body == "" {
			ack, marshalErr := json.Marshal(AckResponse{
				Status: "OK", ProducerID: messages[0].ProducerID, ProducerEpoch: messages[0].Epoch,
				SeqStart: messages[0].SeqNum, SeqEnd: messages[len(messages)-1].SeqNum,
			})
			if marshalErr != nil {
				brokerDone <- marshalErr
				return
			}
			body = string(ack)
		}
		brokerDone <- writeWireTestResponse(connection, request, body)
	}()
	framed, err := openWireConnection(producerConn, 1000, "none")
	require.NoError(t, err)
	connections := []net.Conn{framed}
	client.conns.Store(&connections)

	producer := &Producer{
		config:               cfg,
		client:               client,
		partitions:           1,
		buffers:              []*partitionBuffer{newPartitionBuffer()},
		inFlight:             make([]int32, 1),
		partitionSentMus:     make([]sync.Mutex, 1),
		partitionSentSeqs:    []map[uint64]struct{}{{}},
		partitionBatchStates: []map[string]*BatchState{{}},
		partitionBatchMus:    make([]sync.Mutex, 1),
		gcTicker:             time.NewTicker(time.Hour),
		partitionLeaders:     make(map[int]string),
		done:                 make(chan struct{}),
		closeDone:            make(chan struct{}),
		bmTotalTime:          make(map[int]time.Duration),
		bmTotalCount:         make(map[int]int),
		bmLatencies:          make([]time.Duration, 0),
	}
	producer.sendersWG.Add(1)
	go producer.partitionSender(0)
	t.Cleanup(func() {
		release()
		_ = producer.Close()
		_ = brokerConn.Close()
		_ = producerConn.Close()
	})
	return blockingAckProducer{producer: producer, requestSeen: requestSeen, releaseAck: release, brokerDone: brokerDone}
}

func TestProducerFlushDuringCloseWaitsForInflightAck(t *testing.T) {
	harness := newBlockingAckProducer(t, "")
	producer := harness.producer
	_, err := producer.Send("must-be-acknowledged")
	require.NoError(t, err)
	closeResult := make(chan error, 1)
	go func() { closeResult <- producer.Close() }()

	select {
	case <-harness.requestSeen:
	case <-time.After(time.Second):
		t.Fatal("Close did not drain the batch to the broker")
	}
	require.Equal(t, ProducerStateClosing, producer.State())
	_, err = producer.Send("too-late")
	require.ErrorIs(t, err, ErrProducerClosed)

	flushResult := make(chan error, 1)
	go func() { flushResult <- producer.Flush() }()
	select {
	case err := <-flushResult:
		t.Fatalf("Flush returned before the acknowledgement: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.Zero(t, producer.GetUniqueAckCount())

	harness.releaseAck()
	require.NoError(t, <-flushResult)
	require.NoError(t, <-closeResult)
	require.NoError(t, <-harness.brokerDone)
	require.Equal(t, ProducerStateClosed, producer.State())
	require.Equal(t, uint64(1), producer.GetUniqueAckCount())
	require.NoError(t, producer.Flush())
	require.NoError(t, producer.Close())
}

func TestProducerFlushDuringCloseReturnsPermanentDeliveryFailure(t *testing.T) {
	harness := newBlockingAckProducer(t, "ERROR: NOT_AUTHORIZED_FOR_TOPIC class=authorization retryable=false")
	producer := harness.producer
	_, err := producer.Send("rejected")
	require.NoError(t, err)
	closeResult := make(chan error, 1)
	go func() { closeResult <- producer.Close() }()
	<-harness.requestSeen
	flushResult := make(chan error, 1)
	go func() { flushResult <- producer.Flush() }()
	select {
	case err := <-flushResult:
		t.Fatalf("Flush returned before the broker rejection: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	harness.releaseAck()
	flushErr := <-flushResult
	closeErr := <-closeResult
	require.ErrorContains(t, flushErr, "NOT_AUTHORIZED_FOR_TOPIC")
	require.EqualError(t, closeErr, flushErr.Error())
	require.NoError(t, <-harness.brokerDone)
	require.EqualError(t, producer.Flush(), closeErr.Error())
	require.EqualError(t, producer.Close(), closeErr.Error())
}

func TestProducerContextCloseSharesDrainWithFlush(t *testing.T) {
	harness := newBlockingAckProducer(t, "")
	producer := harness.producer
	ctx, cancel := context.WithCancel(context.Background())
	producer.closeOnContext(ctx)
	_, err := producer.Send("context-close")
	require.NoError(t, err)
	cancel()
	<-harness.requestSeen
	require.Equal(t, ProducerStateClosing, producer.State())

	flushResult := make(chan error, 1)
	go func() { flushResult <- producer.Flush() }()
	select {
	case err := <-flushResult:
		t.Fatalf("Flush returned before context-triggered Close drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	harness.releaseAck()
	require.NoError(t, <-flushResult)
	require.NoError(t, <-harness.brokerDone)
	require.Equal(t, ProducerStateClosed, producer.State())
}

func TestProducerFlushDuringCloseReturnsSameDrainTimeout(t *testing.T) {
	harness := newBlockingAckProducer(t, "")
	producer := harness.producer
	producer.config.FlushTimeoutMS = 50
	_, err := producer.Send("withheld")
	require.NoError(t, err)
	closeResult := make(chan error, 1)
	go func() { closeResult <- producer.Close() }()
	<-harness.requestSeen

	flushResult := make(chan error, 1)
	go func() { flushResult <- producer.Flush() }()
	closeErr := <-closeResult
	flushErr := <-flushResult
	require.ErrorContains(t, closeErr, "drain timeout")
	require.EqualError(t, flushErr, closeErr.Error())
	require.Equal(t, ProducerStateClosed, producer.State())

	harness.releaseAck()
	<-harness.brokerDone
}
