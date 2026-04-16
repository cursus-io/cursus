package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

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

	// Use dynamic port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	startHealthCheckServer(port, ready)

	// Test Not Ready
	url := fmt.Sprintf("http://127.0.0.1:%d/health", port)
	resp, err := http.Get(url)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	resp.Body.Close()

	// Test Ready
	ready.Store(true)
	resp, err = http.Get(url)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
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
	_, _ = io.ReadFull(conn, lenBuf)
	length := binary.BigEndian.Uint32(lenBuf)
	assert.Equal(t, uint32(2), length)

	msgBuf := make([]byte, length)
	_, _ = io.ReadFull(conn, msgBuf)
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
		_, _ = conn.Write(buf)
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
	// Send malformed msg
	msg := "MALFORMED"
	buf := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(msg)))
	copy(buf[4:], []byte(msg))
	_, _ = conn.Write(buf)
	conn.Close()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConnection failed to exit")
	}
}
