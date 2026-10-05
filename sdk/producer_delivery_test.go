package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestProducerBatchAckRequiresExactIdentity(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	p := &Producer{config: cfg, client: mustNewProducerClient(cfg)}
	first := Message{ProducerID: p.client.ID, Epoch: p.client.Epoch, SeqNum: 11}
	last := first
	last.SeqNum = 13
	valid := AckResponse{Status: "OK", ProducerID: first.ProducerID, ProducerEpoch: first.Epoch, SeqStart: 11, SeqEnd: 13}
	for _, idempotent := range []bool{false, true} {
		cfg.EnableIdempotence = idempotent
		for _, field := range []string{"valid", "producer", "epoch", "start", "end", "missing", "partial", "status"} {
			t.Run(fmt.Sprintf("%t/%s", idempotent, field), func(t *testing.T) {
				ack := valid
				switch field {
				case "producer":
					ack.ProducerID = "other"
				case "epoch":
					ack.ProducerEpoch++
				case "start":
					ack.SeqStart--
				case "end":
					ack.SeqEnd--
				case "missing":
					ack = AckResponse{Status: "OK"}
				case "partial":
					ack.Status = "PARTIAL"
				case "status":
					ack.Status = ""
				}
				data, err := json.Marshal(ack)
				require.NoError(t, err)
				_, err = p.parseAckResponseForBatch(data, 0, first, last)
				if field == "valid" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			})
		}
	}
}

func TestProducerBatchRetryDiscardsUnusableConnection(t *testing.T) {
	for _, failure := range []string{"producer", "epoch", "sequence", "header", "empty-body", "body", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() { _ = listener.Close() }()
			cfg := NewDefaultPublisherConfig()
			cfg.BrokerAddrs = []string{listener.Addr().String()}
			cfg.MaxRetries = 1
			cfg.RetryBackoffMS = 1
			cfg.AckTimeoutMS = 100
			require.True(t, cfg.EnableIdempotence)
			require.Equal(t, "all", cfg.Acks)
			client := mustNewProducerClient(cfg)
			defer func() { _ = client.Close() }()
			p := &Producer{config: cfg, client: client, done: make(chan struct{})}
			first := Message{ProducerID: client.ID, Epoch: client.Epoch, SeqNum: 11, Payload: "one"}
			last := first
			last.SeqNum = 12
			payload, err := EncodeBatchMessages(cfg.Topic, 0, cfg.Acks, cfg.EnableIdempotence, []Message{first, last})
			require.NoError(t, err)
			finished := make(chan error, 1)
			go func() {
				for attempt := 0; attempt < 2; attempt++ {
					conn, err := listener.Accept()
					if err != nil {
						finished <- err
						return
					}
					defer func() { _ = conn.Close() }()
					_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
					framed, request, _, err := acceptWireTestRequest(conn)
					if err != nil {
						finished <- err
						return
					}
					if string(request.Payload) != string(payload) {
						finished <- fmt.Errorf("retry changed batch")
						return
					}
					ack := AckResponse{Status: "OK", ProducerID: first.ProducerID, ProducerEpoch: first.Epoch, SeqStart: first.SeqNum, SeqEnd: last.SeqNum}
					if attempt == 0 {
						switch failure {
						case "producer":
							ack.ProducerID = "old"
						case "epoch":
							ack.ProducerEpoch--
						case "sequence":
							ack.SeqEnd--
						}
					}
					data, err := json.Marshal(ack)
					if err != nil {
						finished <- err
						return
					}
					if attempt == 0 && (failure == "header" || failure == "empty-body" || failure == "body" || failure == "timeout") {
						codec, _ := wire.NewCodec(wire.CompressionNone)
						encoded, encodeErr := codec.Encode(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: data})
						if encodeErr != nil {
							finished <- encodeErr
							return
						}
						count := 0
						switch failure {
						case "header":
							count = 1
						case "empty-body":
							count = wire.HeaderSize
						case "body":
							count = wire.HeaderSize + 1
						}
						if count > 0 {
							_, err = conn.Write(encoded[:count])
						}
					} else {
						err = writeWireTestResponse(framed, request, string(data))
					}
					if err != nil {
						finished <- err
						return
					}
					if attempt == 0 {
						_, err = conn.Read(make([]byte, 1))
						if err != io.EOF {
							finished <- fmt.Errorf("old connection was reused or not closed: %v", err)
							return
						}
					}
				}
				finished <- nil
			}()
			_, err = p.sendWithRetryForBatch(payload, 0, first, last)
			require.NoError(t, err)
			select {
			case err := <-finished:
				require.NoError(t, err)
			case <-time.After(4 * time.Second):
				t.Fatal("broker retry did not finish")
			}
		})
	}
}

func TestNonIdempotentProducerDoesNotRetryLostAcknowledgement(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	serverDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		_, _, command, requestErr := acceptWireTestRequest(conn)
		_ = conn.Close()
		if requestErr != nil {
			serverDone <- requestErr
			return
		}
		if command != "PUBLISH_BATCH" {
			serverDone <- fmt.Errorf("unexpected command %q", command)
			return
		}

		tcpListener := listener.(*net.TCPListener)
		_ = tcpListener.SetDeadline(time.Now().Add(200 * time.Millisecond))
		second, secondErr := listener.Accept()
		if secondErr == nil {
			_ = second.Close()
			serverDone <- fmt.Errorf("non-idempotent batch was retried after lost acknowledgement")
			return
		}
		if netErr, ok := secondErr.(net.Error); !ok || !netErr.Timeout() {
			serverDone <- secondErr
			return
		}
		serverDone <- nil
	}()

	cfg := NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	cfg.Acks = "1"
	cfg.EnableIdempotence = false
	cfg.MaxRetries = 3
	cfg.RetryBackoffMS = 1
	cfg.AckTimeoutMS = 100
	client := mustNewProducerClient(cfg)
	defer func() { _ = client.Close() }()
	require.NoError(t, client.ConnectPartition(0, listener.Addr().String()))
	p := &Producer{config: cfg, client: client, done: make(chan struct{}), partitionLeaders: map[int]string{0: listener.Addr().String()}}
	message := Message{ProducerID: client.ID, Epoch: client.Epoch, SeqNum: 1, Payload: "order-created"}
	payload, err := EncodeBatchMessages(cfg.Topic, 0, cfg.Acks, false, []Message{message})
	require.NoError(t, err)

	_, err = p.sendWithRetryForBatch(payload, 0, message, message)
	require.ErrorIs(t, err, ErrProducerOutcomeUnknown)
	var outcomeErr *ProducerOutcomeUnknownError
	require.ErrorAs(t, err, &outcomeErr)
	require.Equal(t, 0, outcomeErr.Partition)
	require.Equal(t, "acknowledgement", outcomeErr.Stage)
	require.NoError(t, <-serverDone)
}

type partialWriteAfterHandshakeConn struct {
	net.Conn
	failWrites bool
}

func (c *partialWriteAfterHandshakeConn) Write(payload []byte) (int, error) {
	if !c.failWrites {
		return c.Conn.Write(payload)
	}
	count := len(payload) / 2
	if count == 0 && len(payload) > 0 {
		count = 1
	}
	written, err := c.Conn.Write(payload[:count])
	if err != nil {
		return written, err
	}
	return written, io.ErrUnexpectedEOF
}

func TestNonIdempotentProducerReturnsUnknownOutcomeAfterPartialWrite(t *testing.T) {
	clientRaw, serverRaw := net.Pipe()
	wrapped := &partialWriteAfterHandshakeConn{Conn: clientRaw}
	serverDone := make(chan error, 1)
	go func() {
		_, err := wire.ServerHandshake(serverRaw, []wire.Compression{wire.CompressionNone})
		if err == nil {
			_, err = io.Copy(io.Discard, serverRaw)
		}
		_ = serverRaw.Close()
		serverDone <- err
	}()
	framed, err := wire.NewClientConn(wrapped, "none")
	require.NoError(t, err)
	wrapped.failWrites = true

	cfg := NewDefaultPublisherConfig()
	cfg.Acks = "1"
	cfg.EnableIdempotence = false
	cfg.MaxRetries = 3
	client := mustNewProducerClient(cfg)
	connections := []net.Conn{framed}
	client.conns.Store(&connections)
	p := &Producer{config: cfg, client: client, done: make(chan struct{})}
	message := Message{ProducerID: client.ID, Epoch: client.Epoch, SeqNum: 1, Payload: "order-created"}
	payload, err := EncodeBatchMessages(cfg.Topic, 0, cfg.Acks, false, []Message{message})
	require.NoError(t, err)

	_, err = p.sendWithRetryForBatch(payload, 0, message, message)
	require.ErrorIs(t, err, ErrProducerOutcomeUnknown)
	var outcomeErr *ProducerOutcomeUnknownError
	require.ErrorAs(t, err, &outcomeErr)
	require.Equal(t, "request write", outcomeErr.Stage)
	require.Nil(t, client.GetConn(0), "a partially written connection must be discarded")
	require.NoError(t, <-serverDone)
}

func TestNonIdempotentProducerRetriesExplicitLeaderRejection(t *testing.T) {
	leader, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = leader.Close() }()
	stale, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = stale.Close() }()

	serverDone := make(chan error, 2)
	go func() {
		conn, acceptErr := stale.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer func() { _ = conn.Close() }()
		framed, request, _, requestErr := acceptWireTestRequest(conn)
		if requestErr == nil {
			requestErr = writeWireTestResponse(framed, request, "ERROR: NOT_LEADER class=routing retryable=true leader="+leader.Addr().String())
		}
		serverDone <- requestErr
	}()
	go func() {
		conn, acceptErr := leader.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer func() { _ = conn.Close() }()
		framed, request, _, requestErr := acceptWireTestRequest(conn)
		if requestErr != nil {
			serverDone <- requestErr
			return
		}
		batch, decodeErr := wire.DecodeBatch(request.Payload)
		if decodeErr != nil {
			serverDone <- decodeErr
			return
		}
		if batch.IsIdempotent {
			serverDone <- errors.New("test batch unexpectedly idempotent")
			return
		}
		ack, marshalErr := json.Marshal(AckResponse{
			Status: "OK", ProducerID: batch.Messages[0].ProducerID, ProducerEpoch: batch.Messages[0].Epoch,
			SeqStart: batch.Messages[0].SeqNum, SeqEnd: batch.Messages[len(batch.Messages)-1].SeqNum,
		})
		if marshalErr == nil {
			marshalErr = writeWireTestResponse(framed, request, string(ack))
		}
		serverDone <- marshalErr
	}()

	cfg := NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{stale.Addr().String()}
	cfg.Acks = "1"
	cfg.EnableIdempotence = false
	cfg.MaxRetries = 1
	cfg.RetryBackoffMS = 1
	client := mustNewProducerClient(cfg)
	defer func() { _ = client.Close() }()
	require.NoError(t, client.ConnectPartition(0, stale.Addr().String()))
	p := &Producer{config: cfg, client: client, done: make(chan struct{}), partitionLeaders: map[int]string{0: stale.Addr().String()}}
	message := Message{ProducerID: client.ID, Epoch: client.Epoch, SeqNum: 1, Payload: "order-created"}
	payload, err := EncodeBatchMessages(cfg.Topic, 0, cfg.Acks, false, []Message{message})
	require.NoError(t, err)

	ack, err := p.sendWithRetryForBatch(payload, 0, message, message)
	require.NoError(t, err)
	require.Equal(t, "OK", ack.Status)
	require.Equal(t, leader.Addr().String(), p.getPartitionLeaderAddr(0))
	require.NoError(t, <-serverDone)
	require.NoError(t, <-serverDone)
}

func TestProducerFlushAndCloseReportPermanentDeliveryFailure(t *testing.T) {
	p, result := newProducerDrainTestHarness(t, "ERROR: NOT_AUTHORIZED_FOR_TOPIC class=authorization retryable=false")
	p.config.MaxRetries = 3
	_, err := p.Send("rejected")
	require.NoError(t, err)
	require.ErrorContains(t, p.Flush(), "NOT_AUTHORIZED_FOR_TOPIC")
	require.ErrorContains(t, p.Close(), "NOT_AUTHORIZED_FOR_TOPIC")
	require.Zero(t, p.GetUniqueAckCount())
	require.NoError(t, (<-result).err)
}

// TestProducerRetryBudgetBoundsLogicalBatchDelivery preserves the final broker error.
func TestProducerRetryBudgetBoundsLogicalBatchDelivery(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var requests atomic.Int32
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			framed, request, _, requestErr := acceptWireTestRequest(conn)
			if requestErr == nil {
				requests.Add(1)
				requestErr = writeWireTestResponse(
					framed,
					request,
					"ERROR: insufficient_in_sync_replicas current=1 required=2",
				)
			}
			_ = conn.Close()
			if requestErr != nil {
				return
			}
		}
	}()

	cfg := NewDefaultPublisherConfig()
	cfg.BrokerAddrs = []string{listener.Addr().String()}
	cfg.Topic = "producer-retry-budget"
	cfg.Partitions = 1
	cfg.BatchSize = 1
	cfg.BufferSize = 4
	cfg.LingerMS = 0
	cfg.MaxRetries = 0
	cfg.RetryBackoffMS = 2_000
	cfg.MaxBackoffMS = 2_000
	cfg.AckTimeoutMS = 250
	cfg.WriteTimeoutMS = 250
	cfg.FlushTimeoutMS = 500

	client := mustNewProducerClient(cfg)
	require.NoError(t, client.ConnectPartition(0, listener.Addr().String()))

	p := &Producer{
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
		partitionLeaders:     map[int]string{0: listener.Addr().String()},
		done:                 make(chan struct{}),
		closeDone:            make(chan struct{}),
		bmTotalTime:          make(map[int]time.Duration),
		bmTotalCount:         make(map[int]int),
		bmLatencies:          make([]time.Duration, 0),
	}
	p.sendersWG.Add(1)
	go p.partitionSender(0)

	_, err = p.Send("one-logical-record")
	require.NoError(t, err)

	flushErr := p.Flush()
	require.ErrorContains(t, flushErr, "insufficient_in_sync_replicas")
	require.NotContains(t, flushErr.Error(), "producer flush timeout")
	require.Equal(t, int32(cfg.MaxRetries+1), requests.Load())
	var brokerErr *BrokerError
	require.True(t, errors.As(flushErr, &brokerErr))
	require.Equal(t, "insufficient_in_sync_replicas", brokerErr.Code)
	require.Equal(t, ErrorClassAvailability, brokerErr.Class)
	require.True(t, brokerErr.Retryable)

	closeErr := p.Close()
	require.ErrorContains(t, closeErr, "insufficient_in_sync_replicas")
	require.NotContains(t, closeErr.Error(), "producer close: drain timeout")

	_ = listener.Close()
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("test broker did not stop")
	}
}

func TestProducerFlushAndCloseExposeUnknownDeliveryOutcome(t *testing.T) {
	p, result := newProducerDrainTestHarness(t, "{invalid-ack")
	p.config.Acks = "1"
	p.config.EnableIdempotence = false
	p.config.MaxRetries = 3
	_, err := p.Send("uncertain")
	require.NoError(t, err)
	require.ErrorIs(t, p.Flush(), ErrProducerOutcomeUnknown)
	require.ErrorIs(t, p.Close(), ErrProducerOutcomeUnknown)
	require.Zero(t, p.GetUniqueAckCount())
	brokerResult := <-result
	require.NoError(t, brokerResult.err)
	require.Len(t, brokerResult.messages, 1, "unknown batch must not be requeued")
}

func TestProducerReconnectFailureRemovesOldConnection(t *testing.T) {
	cfg := NewDefaultPublisherConfig()
	client := mustNewProducerClient(cfg)
	cfg.BrokerAddrs = nil
	server, conn := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()
	connections := []net.Conn{conn}
	client.conns.Store(&connections)
	require.Error(t, client.ReconnectPartition(0, ""))
	require.Nil(t, client.GetConn(0))
	_, err := server.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

// TestProducerFinalFailureDoesNotReconnect covers terminal write, read, and parse errors.
func TestProducerFinalFailureDoesNotReconnect(t *testing.T) {
	for _, failure := range []string{"write", "read", "parse"} {
		t.Run(failure, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() { _ = listener.Close() }()
			cfg := NewDefaultPublisherConfig()
			cfg.BrokerAddrs = []string{listener.Addr().String()}
			cfg.MaxRetries = 0
			cfg.AckTimeoutMS = 50
			cfg.HandshakeTimeoutMS = 1000
			client := mustNewProducerClient(cfg)
			defer func() { _ = client.Close() }()
			server, conn := net.Pipe()
			defer func() { _ = server.Close() }()
			client.conns.Store(&[]net.Conn{conn})
			p := &Producer{config: cfg, client: client, done: make(chan struct{})}
			go func() {
				if failure == "write" {
					_ = server.Close()
					return
				}
				if _, err := ReadWithLength(server); err != nil {
					return
				}
				if failure == "parse" {
					_ = WriteWithLength(server, []byte("invalid ack"))
				}
				_ = server.Close()
			}()
			started := time.Now()
			_, err = p.sendWithRetryForBatch([]byte("batch"), 0, Message{}, Message{})
			require.Error(t, err)
			require.Less(t, time.Since(started), 500*time.Millisecond)
			require.Nil(t, client.GetConn(0))
			require.NoError(t, listener.(*net.TCPListener).SetDeadline(time.Now().Add(20*time.Millisecond)))
			unexpected, acceptErr := listener.Accept()
			if unexpected != nil {
				_ = unexpected.Close()
			}
			require.Error(t, acceptErr, "terminal failure must not open a replacement connection")
		})
	}
}
