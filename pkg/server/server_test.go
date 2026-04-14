package server

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestIsBatchMessage(t *testing.T) {
	// Valid batch
	data := make([]byte, 10)
	data[0] = 0xBA
	data[1] = 0x7C
	binary.BigEndian.PutUint16(data[2:4], 2) // topic len 2
	assert.True(t, isBatchMessage(data))

	// Invalid magic
	data[0] = 0x00
	assert.False(t, isBatchMessage(data))

	// Too short
	assert.False(t, isBatchMessage([]byte{0xBA, 0x7C}))
}

func TestIsCommand(t *testing.T) {
	assert.True(t, isCommand("CREATE topic=t1"))
	assert.True(t, isCommand("list"))
	assert.True(t, isCommand("PUBLISH topic=t1 message=hi"))
	assert.False(t, isCommand("NOT_A_COMMAND"))
	assert.False(t, isCommand(""))
}

func TestHealthCheckServer(t *testing.T) {
	ready := &atomic.Bool{}
	ready.Store(false)
	
	port := 19080
	startHealthCheckServer(port, ready)
	
	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	// Test Not Ready
	resp, err := http.Get("http://localhost:19080/health")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	
	// Test Ready
	ready.Store(true)
	resp, err = http.Get("http://localhost:19080/health")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestWriteResponse(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	done := make(chan bool)
	go func() {
		conn, _ := l.Accept()
		writeResponse(conn, "OK")
		conn.Close()
		done <- true
	}()

	conn, _ := net.Dial("tcp", l.Addr().String())
	defer conn.Close()

	lenBuf := make([]byte, 4)
	io.ReadFull(conn, lenBuf)
	length := binary.BigEndian.Uint32(lenBuf)
	assert.Equal(t, uint32(2), length)

	msgBuf := make([]byte, length)
	io.ReadFull(conn, msgBuf)
	assert.Equal(t, "OK", string(msgBuf))
	<-done
}

func TestReadMessage(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	go func() {
		conn, _ := l.Accept()
		msg := "test-message"
		buf := make([]byte, 4+len(msg))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(msg)))
		copy(buf[4:], []byte(msg))
		conn.Write(buf)
		conn.Close()
	}()

	conn, _ := net.Dial("tcp", l.Addr().String())
	defer conn.Close()

	data, err := readMessage(conn, "none")
	assert.NoError(t, err)
	assert.Equal(t, "test-message", string(data))
}

func TestHandleConnection_Exit(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	cfg := config.DefaultConfig()
	
	go func() {
		conn, _ := l.Accept()
		HandleConnection(context.Background(), conn, nil, cfg, nil, nil, nil)
	}()

	conn, _ := net.Dial("tcp", l.Addr().String())
	// Send malformed or something that causes exit
	msg := "MALFORMED"
	buf := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(msg)))
	copy(buf[4:], []byte(msg))
	conn.Write(buf)
	
	// Wait for HandleConnection to process and close
	time.Sleep(100 * time.Millisecond)
	conn.Close()
}
