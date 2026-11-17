package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const commitTestAssignmentGeneration uint64 = 1

type writeFailureConn struct {
	net.Conn
	failures  atomic.Int32
	armed     atomic.Bool
	onFailure func()
}

func (c *writeFailureConn) Write(payload []byte) (int, error) {
	if !c.armed.Load() {
		return c.Conn.Write(payload)
	}
	written, _ := c.Conn.Write(payload)
	c.failures.Add(1)
	if c.onFailure != nil {
		c.onFailure()
	}
	return written, syscall.EPIPE
}

func newCommitTestConsumer(t *testing.T, ctx context.Context) *Consumer {
	t.Helper()
	cfg := NewDefaultConsumerConfig()
	cfg.BrokerAddrs = []string{"127.0.0.1:1"}
	cfg.Topic = "commit-topic"
	cfg.GroupID = "commit-group"
	cfg.RequestTimeoutMS = 1000
	c, err := NewConsumerWithContext(ctx, cfg)
	require.NoError(t, err)
	c.mu.Lock()
	c.generation = 7
	c.memberID = "member-1"
	c.mu.Unlock()
	c.state.Store(uint32(ConsumerStateRunning))
	c.assignmentGeneration.Store(commitTestAssignmentGeneration)
	t.Cleanup(func() {
		c.rootCancel()
		c.commitConnMu.Lock()
		if c.commitConn != nil {
			_ = c.commitConn.Close()
			c.commitConn = nil
		}
		c.commitConnMu.Unlock()
	})
	return c
}

func serveCommitRequest(conn net.Conn, commands chan<- string, response string) error {
	defer func() { _ = conn.Close() }()
	connection, request, command, err := acceptWireTestRequest(conn)
	if err != nil {
		return err
	}
	if commands != nil {
		commands <- command
	}
	if response == "" {
		return nil
	}
	return writeWireTestResponse(connection, request, response)
}

func newCommitPipe(t *testing.T, commands chan<- string, response string) (net.Conn, <-chan error) {
	t.Helper()
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- serveCommitRequest(server, commands, response) }()
	framed, err := openWireConnection(client, 1000, "none")
	require.NoError(t, err)
	return framed, done
}

func newWriteFailureCommitPipe(t *testing.T, commands chan<- string) (net.Conn, *writeFailureConn, <-chan error) {
	t.Helper()
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- serveCommitRequest(server, commands, "") }()
	failing := &writeFailureConn{Conn: client}
	framed, err := openWireConnection(failing, 1000, "none")
	require.NoError(t, err)
	failing.armed.Store(true)
	return framed, failing, done
}

func expectNoConnection(t *testing.T, listener net.Listener) {
	t.Helper()
	tcpListener, ok := listener.(*net.TCPListener)
	require.True(t, ok)
	require.NoError(t, tcpListener.SetDeadline(time.Now().Add(100*time.Millisecond)))
	conn, err := tcpListener.Accept()
	if conn != nil {
		_ = conn.Close()
	}
	require.Error(t, err)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	assert.True(t, netErr.Timeout())
}

func TestBatchCommitReconnectsAfterIdleConnectionClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	idleClosed := make(chan struct{})
	commands := make(chan string, 1)
	serverDone := make(chan error, 1)
	go func() {
		idleConn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		if _, handshakeErr := wire.ServerHandshake(idleConn, []wire.Compression{wire.CompressionNone}); handshakeErr != nil {
			serverDone <- handshakeErr
			return
		}
		_ = idleConn.Close()
		close(idleClosed)

		retryConn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		serverDone <- serveCommitRequest(retryConn, commands, "OK")
	}()

	c := newCommitTestConsumer(t, context.Background())
	idleConn, err := c.client.ConnectContext(context.Background(), listener.Addr().String())
	require.NoError(t, err)
	<-idleClosed
	c.mu.Lock()
	c.coordinatorAddr = listener.Addr().String()
	c.mu.Unlock()
	c.commitConn = idleConn

	require.True(t, c.sendBatchCommit(map[int]uint64{0: 42}, commitTestAssignmentGeneration))
	require.NoError(t, <-serverDone)
	assert.Equal(t, "BATCH_COMMIT topic=commit-topic group=commit-group generation=7 member=member-1 offsets=P0:42", <-commands)
}

func TestBatchCommitRetriesBrokenPipeOnceWithSameBatch(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	firstCommands := make(chan string, 1)
	firstConn, failing, firstDone := newWriteFailureCommitPipe(t, firstCommands)
	retryCommands := make(chan string, 1)
	retryDone := make(chan error, 1)
	var accepts atomic.Int32
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			retryDone <- acceptErr
			return
		}
		accepts.Add(1)
		retryDone <- serveCommitRequest(conn, retryCommands, "OK")
	}()

	c := newCommitTestConsumer(t, context.Background())
	c.mu.Lock()
	c.coordinatorAddr = listener.Addr().String()
	c.mu.Unlock()
	c.commitConn = firstConn

	require.True(t, c.sendBatchCommit(map[int]uint64{2: 91, 0: 42}, commitTestAssignmentGeneration))
	require.NoError(t, <-firstDone)
	require.NoError(t, <-retryDone)
	assert.Equal(t, int32(1), failing.failures.Load())
	assert.Equal(t, int32(1), accepts.Load())
	assert.Equal(t, <-firstCommands, <-retryCommands)
}

func TestBatchCommitRetriesSameOffsetsAfterResponseEOF(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	firstCommands := make(chan string, 1)
	firstConn, firstDone := newCommitPipe(t, firstCommands, "")
	retryCommands := make(chan string, 1)
	retryDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			retryDone <- acceptErr
			return
		}
		retryDone <- serveCommitRequest(conn, retryCommands, "OK")
	}()

	c := newCommitTestConsumer(t, context.Background())
	c.mu.Lock()
	c.coordinatorAddr = listener.Addr().String()
	c.mu.Unlock()
	c.commitConn = firstConn

	require.True(t, c.sendBatchCommit(map[int]uint64{0: 42}, commitTestAssignmentGeneration))
	require.NoError(t, <-firstDone)
	require.NoError(t, <-retryDone)
	assert.Equal(t, <-firstCommands, <-retryCommands)
}

func TestBatchCommitReconnectFailureUsesRetryQueue(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	closedAddr := listener.Addr().String()
	require.NoError(t, listener.Close())

	firstCommands := make(chan string, 1)
	firstConn, failing, firstDone := newWriteFailureCommitPipe(t, firstCommands)
	c := newCommitTestConsumer(t, context.Background())
	c.config.BrokerAddrs = []string{closedAddr}
	c.mu.Lock()
	c.coordinatorAddr = closedAddr
	c.mu.Unlock()
	c.commitConn = firstConn

	c.commitBatch(map[int]uint64{0: 42}, map[int][]chan error{0: nil}, commitTestAssignmentGeneration)
	require.NoError(t, <-firstDone)

	c.commitMu.Lock()
	retry := c.commitRetryMap[0]
	c.commitMu.Unlock()
	assert.Equal(t, uint64(42), retry.offset)
	assert.Equal(t, commitTestAssignmentGeneration, retry.assignmentGeneration)
	assert.Equal(t, int32(1), failing.failures.Load())
}

func TestBatchCommitCancellationDoesNotReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := newCommitTestConsumer(t, ctx)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	c.mu.Lock()
	c.coordinatorAddr = listener.Addr().String()
	c.mu.Unlock()

	firstCommands := make(chan string, 1)
	firstConn, failing, firstDone := newWriteFailureCommitPipe(t, firstCommands)
	failing.onFailure = cancel
	c.commitConn = firstConn

	assert.False(t, c.sendBatchCommit(map[int]uint64{0: 42}, commitTestAssignmentGeneration))
	assert.Equal(t, int32(1), failing.failures.Load())
	require.NoError(t, <-firstDone)
	assert.Contains(t, <-firstCommands, "offsets=P0:42")
	expectNoConnection(t, listener)
}

func TestBatchCommitTimeoutDoesNotReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newCommitTestConsumer(t, ctx)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	c.mu.Lock()
	c.coordinatorAddr = listener.Addr().String()
	c.mu.Unlock()

	commands := make(chan string, 1)
	server, client := net.Pipe()
	serverDone := make(chan error, 1)
	go func() {
		defer func() { _ = server.Close() }()
		_, _, command, serveErr := acceptWireTestRequest(server)
		if serveErr == nil {
			commands <- command
			<-ctx.Done()
		}
		serverDone <- serveErr
	}()
	framed, err := openWireConnection(client, 1000, "none")
	require.NoError(t, err)
	c.commitConn = framed
	cancelTimer := time.AfterFunc(50*time.Millisecond, cancel)
	defer cancelTimer.Stop()

	assert.False(t, c.sendBatchCommit(map[int]uint64{0: 42}, commitTestAssignmentGeneration))
	select {
	case serveErr := <-serverDone:
		require.NoError(t, serveErr)
	case <-time.After(time.Second):
		_ = framed.Close()
		t.Fatal("timed out waiting for commit test server to stop")
	}
	assert.Contains(t, <-commands, "offsets=P0:42")
	expectNoConnection(t, listener)
}

func TestBatchCommitSuccessReusesConnection(t *testing.T) {
	c := newCommitTestConsumer(t, context.Background())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	c.mu.Lock()
	c.coordinatorAddr = listener.Addr().String()
	c.mu.Unlock()

	commands := make(chan string, 1)
	client, serverDone := newCommitPipe(t, commands, "OK")
	c.commitConn = client

	require.True(t, c.sendBatchCommit(map[int]uint64{0: 42}, commitTestAssignmentGeneration))
	require.NoError(t, <-serverDone)
	assert.Same(t, client, c.commitConn)
	expectNoConnection(t, listener)
}

func TestBatchCommitProtocolErrorDoesNotReconnect(t *testing.T) {
	c := newCommitTestConsumer(t, context.Background())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	c.mu.Lock()
	c.coordinatorAddr = listener.Addr().String()
	c.mu.Unlock()

	commands := make(chan string, 1)
	client, serverDone := newCommitPipe(t, commands, "ERROR: GEN_MISMATCH current=8")
	c.commitConn = client

	assert.False(t, c.sendBatchCommit(map[int]uint64{0: 42}, commitTestAssignmentGeneration))
	require.NoError(t, <-serverDone)
	select {
	case <-c.rebalanceSig:
	default:
		t.Fatal("generation mismatch did not signal rebalance")
	}
	expectNoConnection(t, listener)
}

func TestRetryableCommitTransportErrorClassification(t *testing.T) {
	assert.True(t, isRetryableCommitTransportError(errors.New("write: broken pipe")))
	assert.True(t, isRetryableCommitTransportError(syscall.ECONNRESET))
	assert.True(t, isRetryableCommitTransportError(fmt.Errorf("wrapped: %w", io.EOF)))
}
