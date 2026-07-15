package server

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestConnPair creates a connected pair of net.Conn for testing.
func newTestConnPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	connCh := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			connCh <- nil
			return
		}
		connCh <- c
	}()

	client, err = net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	server = <-connCh
	require.NotNil(t, server, "Accept() failed")
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

// sendFramed writes a length-prefixed message to conn.
func sendFramed(t *testing.T, conn net.Conn, msg []byte) {
	t.Helper()
	buf := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(msg)))
	copy(buf[4:], msg)
	_, err := conn.Write(buf)
	require.NoError(t, err)
}

// readFramed reads a length-prefixed response from conn.
func readFramed(t *testing.T, conn net.Conn) string {
	t.Helper()
	lenBuf := make([]byte, 4)
	_, err := io.ReadFull(conn, lenBuf)
	require.NoError(t, err)
	length := binary.BigEndian.Uint32(lenBuf)
	msgBuf := make([]byte, length)
	_, err = io.ReadFull(conn, msgBuf)
	require.NoError(t, err)
	return string(msgBuf)
}

func TestIsBatchMessage(t *testing.T) {
	data := make([]byte, 10)
	data[0] = 0xBA
	data[1] = 0x7C
	binary.BigEndian.PutUint16(data[2:4], 2)
	assert.True(t, isBatchMessage(data))

	data[0] = 0x00
	assert.False(t, isBatchMessage(data))

	assert.False(t, isBatchMessage([]byte{0xBA, 0x7C}))
}

func TestIsBatchMessage_ZeroTopicLen(t *testing.T) {
	data := make([]byte, 10)
	data[0] = 0xBA
	data[1] = 0x7C
	binary.BigEndian.PutUint16(data[2:4], 0)
	assert.False(t, isBatchMessage(data))
}

func TestIsBatchMessage_TopicLenExceedsData(t *testing.T) {
	data := make([]byte, 6)
	data[0] = 0xBA
	data[1] = 0x7C
	binary.BigEndian.PutUint16(data[2:4], 200)
	assert.False(t, isBatchMessage(data))
}

func TestIsBatchMessage_ExactlySixBytes(t *testing.T) {
	data := []byte{0xBA, 0x7C, 0x00, 0x01, 0x41, 0x00}
	assert.True(t, isBatchMessage(data))
}

func TestIsBatchMessage_SecondMagicByteMismatch(t *testing.T) {
	data := make([]byte, 10)
	data[0] = 0xBA
	data[1] = 0x00
	binary.BigEndian.PutUint16(data[2:4], 2)
	assert.False(t, isBatchMessage(data))
}

func TestIsCommand(t *testing.T) {
	assert.True(t, isCommand("CREATE topic=t1"))
	assert.True(t, isCommand("list"))
	assert.True(t, isCommand("PUBLISH topic=t1 message=hi"))
	assert.False(t, isCommand("NOT_A_COMMAND"))
	assert.False(t, isCommand(""))
}

func TestIsCommand_AllKeywords(t *testing.T) {
	keywords := []string{
		"CREATE t", "DELETE t", "LIST", "LIST_CLUSTER", "PUBLISH t",
		"CONSUME t", "STREAM t", "HELP", "HEARTBEAT x",
		"JOIN_GROUP g", "LEAVE_GROUP g", "COMMIT_OFFSET t",
		"BATCH_COMMIT t", "REGISTER_GROUP g", "GROUP_STATUS g",
		"FETCH_OFFSET t", "LIST_GROUPS", "SYNC_GROUP g", "DESCRIBE t",
		"APPEND_STREAM t", "READ_STREAM t", "SAVE_SNAPSHOT t",
		"READ_SNAPSHOT t", "STREAM_VERSION t",
		"INIT_PRODUCER_ID transactional_id=tx-1",
		"BEGIN_TXN transactional_id=tx-1 producerId=p1 epoch=0",
		"TXN_PUBLISH transactional_id=tx-1 topic=t1 partition=0 producerId=p1 seqNum=1 epoch=0 message=value",
		"SEND_OFFSETS_TO_TXN transactional_id=tx-1 producerId=p1 epoch=0 topic=t1 group=g1 member=m1 generation=1 P0:1",
		"END_TXN transactional_id=tx-1 producerId=p1 epoch=0 result=commit",
		"TXN_STATUS transactional_id=tx-1",
	}
	for _, kw := range keywords {
		assert.True(t, isCommand(kw), "expected %q to be a command", kw)
	}
}

func TestIsCommand_CaseInsensitive(t *testing.T) {
	assert.True(t, isCommand("create topic=t1"))
	assert.True(t, isCommand("Publish topic=t1 message=hi"))
	assert.True(t, isCommand("heartbeat x"))
	assert.True(t, isCommand("stream t"))
}

func TestHealthHandlerSeparatesLivenessAndReadiness(t *testing.T) {
	state := NewHealthState()
	dependencyReady := false
	state.AddCheck("cluster_leader", func(context.Context) error {
		if !dependencyReady {
			return errors.New("no leader")
		}
		return nil
	})
	handler := newHealthHandler(state)

	live := httptest.NewRecorder()
	handler.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/live", nil))
	assert.Equal(t, http.StatusOK, live.Code)

	legacy := httptest.NewRecorder()
	handler.ServeHTTP(legacy, httptest.NewRequest(http.MethodGet, "/health", nil))
	assert.Equal(t, http.StatusServiceUnavailable, legacy.Code)
	assert.Contains(t, legacy.Body.String(), "broker=starting")

	state.SetReady(true)
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	assert.Equal(t, http.StatusServiceUnavailable, ready.Code)
	assert.Contains(t, ready.Body.String(), `"cluster_leader":"no leader"`)

	dependencyReady = true
	ready = httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	assert.Equal(t, http.StatusOK, ready.Code)
	assert.Contains(t, ready.Body.String(), `"status":"ready"`)

	legacy = httptest.NewRecorder()
	handler.ServeHTTP(legacy, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusOK, legacy.Code)
	assert.Equal(t, "OK", legacy.Body.String())
}

func TestHealthHandlerRejectsMutationMethods(t *testing.T) {
	recorder := httptest.NewRecorder()
	newHealthHandler(NewHealthState()).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/ready", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	assert.Equal(t, "GET, HEAD", recorder.Header().Get("Allow"))
}

func TestHealthCheckServerReportsBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	server, err := startHealthCheckServerAddress(listener.Addr().String(), NewHealthState())
	assert.Error(t, err)
	assert.Nil(t, server)
}

func TestWriteResponse(t *testing.T) {
	client, server := newTestConnPair(t)

	done := make(chan bool)
	go func() {
		writeResponse(server, "OK")
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Equal(t, "OK", msg)
	<-done
}

func TestWriteResponse_LongMessage(t *testing.T) {
	client, server := newTestConnPair(t)

	longMsg := ""
	for i := 0; i < 1000; i++ {
		longMsg += "A"
	}

	done := make(chan bool)
	go func() {
		writeResponse(server, longMsg)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Equal(t, longMsg, msg)
	<-done
}

func TestWriteResponse_EmptyMessage(t *testing.T) {
	client, server := newTestConnPair(t)

	done := make(chan bool)
	go func() {
		writeResponse(server, "")
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Equal(t, "", msg)
	<-done
}

func TestWriteResponse_ClosedConn(t *testing.T) {
	_, server := newTestConnPair(t)
	_ = server.Close()
	writeResponse(server, "should not panic")
}

func TestWriteResponseWithTimeout(t *testing.T) {
	client, server := newTestConnPair(t)

	done := make(chan bool)
	go func() {
		writeResponseWithTimeout(server, "HELLO", 5*time.Second)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Equal(t, "HELLO", msg)
	<-done
}

func TestWriteResponseWithTimeout_ClosedConn(t *testing.T) {
	_, server := newTestConnPair(t)
	_ = server.Close()
	writeResponseWithTimeout(server, "should not panic", 1*time.Second)
}

func TestReadMessage(t *testing.T) {
	client, server := newTestConnPair(t)

	go func() {
		msg := "test-message"
		buf := make([]byte, 4+len(msg))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(msg)))
		copy(buf[4:], []byte(msg))
		_, _ = server.Write(buf)
	}()

	data, err := readMessage(client, "none")
	assert.NoError(t, err)
	assert.Equal(t, "test-message", string(data))
}

func TestReadMessage_EOF(t *testing.T) {
	client, server := newTestConnPair(t)
	_ = server.Close()

	_, err := readMessage(client, "none")
	assert.Error(t, err)
}

func TestReadMessage_PartialLength(t *testing.T) {
	client, server := newTestConnPair(t)

	go func() {
		_, _ = server.Write([]byte{0x00, 0x00})
		_ = server.Close()
	}()

	_, err := readMessage(client, "none")
	assert.Error(t, err)
}

func TestReadMessage_PartialBody(t *testing.T) {
	client, server := newTestConnPair(t)

	go func() {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, 100)
		_, _ = server.Write(lenBuf)
		_, _ = server.Write([]byte("short"))
		_ = server.Close()
	}()

	_, err := readMessage(client, "none")
	assert.Error(t, err)
}

func TestReadMessage_RejectsOversizedFrameBeforeReadingBody(t *testing.T) {
	client, server := newTestConnPair(t)

	go func() {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(util.MaxMessageSize+1))
		_, _ = server.Write(lenBuf)
	}()

	_, err := readMessage(client, "none")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

func TestReadMessage_WithGzipCompression(t *testing.T) {
	client, server := newTestConnPair(t)

	msg := "compressed-message"
	compressed, err := util.CompressMessage([]byte(msg), "gzip")
	require.NoError(t, err)

	go func() {
		buf := make([]byte, 4+len(compressed))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(compressed)))
		copy(buf[4:], compressed)
		_, _ = server.Write(buf)
	}()

	data, err := readMessage(client, "gzip")
	assert.NoError(t, err)
	assert.Equal(t, msg, string(data))
}

func TestReadMessage_InvalidCompression(t *testing.T) {
	client, server := newTestConnPair(t)

	go func() {
		msg := []byte("not-compressed-data")
		buf := make([]byte, 4+len(msg))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(msg)))
		copy(buf[4:], msg)
		_, _ = server.Write(buf)
	}()

	_, err := readMessage(client, "gzip")
	assert.Error(t, err)
}

func TestProcessMessage_HeartbeatEncoded(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	encoded := util.EncodeMessage("ignored", "HEARTBEAT")
	done := make(chan bool)
	go func() {
		shouldExit, err := processMessage(encoded, cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Contains(t, msg, "ERROR:")
	<-done
}

func TestProcessMessage_UnrecognizedInput(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	encoded := util.EncodeMessage("topic1", "some random data")
	done := make(chan bool)
	go func() {
		shouldExit, err := processMessage(encoded, cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.True(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Contains(t, msg, "ERROR")
	<-done
}

func TestProcessMessage_RawCommand(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	rawData := []byte("HELP")
	done := make(chan bool)
	go func() {
		shouldExit, err := processMessage(rawData, cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.NotEmpty(t, msg)
	<-done
}

func TestProcessMessage_EncodedCommand(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	encoded := util.EncodeMessage("topic1", "HELP")
	done := make(chan bool)
	go func() {
		shouldExit, err := processMessage(encoded, cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.NotEmpty(t, msg)
	<-done
}

func TestProcessMessage_JoinGroup(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	encoded := util.EncodeMessage("topic", "JOIN_GROUP group=test-group")
	done := make(chan bool)
	go func() {
		shouldExit, err := processMessage(encoded, cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.NotEmpty(t, msg)
	<-done
}

func TestProcessMessage_SyncGroup(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	encoded := util.EncodeMessage("topic", "SYNC_GROUP group=test-group")
	done := make(chan bool)
	go func() {
		shouldExit, err := processMessage(encoded, cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.NotEmpty(t, msg)
	<-done
}

func TestProcessMessage_LeaveGroup(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	encoded := util.EncodeMessage("topic", "LEAVE_GROUP group=test-group")
	done := make(chan bool)
	go func() {
		shouldExit, err := processMessage(encoded, cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.NotEmpty(t, msg)
	<-done
}

func TestHandleCommandMessage_HelpCommand(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	done := make(chan bool)
	go func() {
		shouldExit, err := handleCommandMessage("HELP", cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.NotEmpty(t, msg)
	<-done
}

func TestHandleCommandMessage_ListCluster(t *testing.T) {
	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	_, server := newTestConnPair(t)

	shouldExit, err := handleCommandMessage("LIST_CLUSTER", cmdHandler, cmdCtx, server)
	assert.NoError(t, err)
	assert.False(t, shouldExit)
}

func TestHandleConnection_Exit(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	cfg := config.DefaultConfig()
	done := make(chan struct{})

	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		HandleConnection(context.Background(), conn, nil, cfg, nil, nil, nil)
	}()

	conn, _ := net.Dial("tcp", l.Addr().String())
	msg := "MALFORMED"
	buf := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(msg)))
	copy(buf[4:], []byte(msg))
	_, _ = conn.Write(buf)
	_ = conn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConnection failed to exit")
	}
}

func TestHandleConnection_ContextCancel(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	cfg := config.DefaultConfig()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		HandleConnection(ctx, conn, nil, cfg, nil, nil, nil)
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("HandleConnection did not exit after context cancel")
	}
}

func TestHandleConn_ImmediateClose(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	done := make(chan struct{})

	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		handleConn(context.Background(), conn, cmdHandler)
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	_ = conn.Close()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handleConn did not exit after connection close")
	}
}

func TestHandleConn_MalformedInput(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	done := make(chan struct{})

	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		handleConn(context.Background(), conn, cmdHandler)
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	msg := "MALFORMED"
	buf := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(msg)))
	copy(buf[4:], []byte(msg))
	_, _ = conn.Write(buf)
	_ = conn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not exit")
	}
}

func TestHandleConn_HelpCommand(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	done := make(chan struct{})

	go func() {
		defer close(done)
		sConn, err := l.Accept()
		if err != nil {
			return
		}
		handleConn(context.Background(), sConn, cmdHandler)
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	encoded := util.EncodeMessage("t", "HELP")
	sendFramed(t, conn, encoded)

	msg := readFramed(t, conn)
	assert.NotEmpty(t, msg)

	_ = conn.Close()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handleConn did not exit")
	}
}

func TestHandleCommandMessage_StreamCommand(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	done := make(chan bool)
	go func() {
		shouldExit, err := handleCommandMessage("STREAM topic=test partition=0 group=g1", cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Contains(t, msg, "ERROR")
	<-done
}

func TestHandleCommandMessage_StreamCommandInvalidSyntax(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	done := make(chan bool)
	go func() {
		shouldExit, err := handleCommandMessage("STREAM invalid", cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Contains(t, msg, "ERROR")
	<-done
}

func TestHandleCommandMessage_ConsumeCommandInvalidSyntax2(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	done := make(chan bool)
	go func() {
		shouldExit, err := handleCommandMessage(
			"CONSUME topic=test",
			cmdHandler, cmdCtx, server,
		)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Contains(t, msg, "ERROR")
	<-done
}

func TestHandleCommandMessage_ConsumeCommandInvalidSyntax(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	done := make(chan bool)
	go func() {
		shouldExit, err := handleCommandMessage("CONSUME invalid", cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Contains(t, msg, "ERROR")
	<-done
}

func TestProcessMessage_HeartbeatWithPadding(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	encoded := util.EncodeMessage("ignored", "  HEARTBEAT  ")
	done := make(chan bool)
	go func() {
		shouldExit, err := processMessage(encoded, cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Contains(t, msg, "ERROR:")
	<-done
}

func TestHandleConn_StreamCommandSetsIsStreamed(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	done := make(chan struct{})

	go func() {
		defer close(done)
		sConn, err := l.Accept()
		if err != nil {
			return
		}
		handleConn(context.Background(), sConn, cmdHandler)
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	encoded := util.EncodeMessage("t", "STREAM topic=test partition=0 group=g1")
	sendFramed(t, conn, encoded)

	msg := readFramed(t, conn)
	assert.Contains(t, msg, "ERROR")
	_ = conn.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleConn did not exit")
	}
}

func TestHandleConnection_StreamCommandSetsIsStreamed(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	cfg := config.DefaultConfig()
	done := make(chan struct{})

	go func() {
		defer close(done)
		sConn, err := l.Accept()
		if err != nil {
			return
		}
		HandleConnection(context.Background(), sConn, nil, cfg, nil, nil, nil)
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	encoded := util.EncodeMessage("t", "STREAM topic=test partition=0 group=g1")
	sendFramed(t, conn, encoded)

	msg := readFramed(t, conn)
	assert.Contains(t, msg, "ERROR")
	_ = conn.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("HandleConnection did not exit")
	}
}

func TestHandleConn_ContextCancel(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		handleConn(ctx, conn, cmdHandler)
	}()

	conn, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handleConn did not exit after context cancel")
	}
}

func TestProcessMessage_DecodeErrorNonCommand(t *testing.T) {
	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	_, server := newTestConnPair(t)

	data := []byte{0x00}
	_, err := processMessage(data, cmdHandler, cmdCtx, server)
	assert.NoError(t, err) // Error response is sent to conn, not returned
}

func TestWriteResponseWithTimeout_LongMessage(t *testing.T) {
	client, server := newTestConnPair(t)

	longMsg := ""
	for i := 0; i < 500; i++ {
		longMsg += "X"
	}

	done := make(chan bool)
	go func() {
		writeResponseWithTimeout(server, longMsg, 5*time.Second)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Equal(t, longMsg, msg)
	<-done
}

func TestProcessMessage_BatchMessage(t *testing.T) {
	client, server := newTestConnPair(t)

	cfg := config.DefaultConfig()
	cmdHandler := controller.NewCommandHandler(nil, cfg, nil, nil, nil)
	cmdCtx := controller.NewClientContext("default-group", 0)

	batchData := []byte{0xBA, 0x7C, 0x00, 0x03, 0x66, 0x6F, 0x6F}

	done := make(chan bool)
	go func() {
		shouldExit, err := processMessage(batchData, cmdHandler, cmdCtx, server)
		assert.NoError(t, err)
		assert.False(t, shouldExit)
		done <- true
	}()

	msg := readFramed(t, client)
	assert.Contains(t, msg, "ERROR")
	<-done
}

func TestInitializeConnection(t *testing.T) {
	cfg := config.DefaultConfig()
	cmdHandler, cmdCtx := initializeConnection(cfg, nil, nil, nil, nil)
	assert.NotNil(t, cmdHandler)
	assert.NotNil(t, cmdCtx)
	assert.Equal(t, cfg, cmdHandler.Config)
	_ = cmdHandler.Close()
}

func TestConstants(t *testing.T) {
	assert.Equal(t, 1000, maxWorkers)
	assert.Equal(t, 9080, DefaultHealthCheckPort)
}
