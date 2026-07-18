package client

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTCPClusterClient(t *testing.T) {
	client := NewTCPClusterClient()
	assert.NotNil(t, client)
	assert.Equal(t, 5*time.Second, client.timeout)
}

func TestExtractSeedHosts(t *testing.T) {
	client := NewTCPClusterClient()
	peers := []string{"node1@127.0.0.1:8000", "node2@127.0.0.2:8000", "127.0.0.3:8000"}
	localAddr := "127.0.0.1:8000"

	seeds := client.extractSeedHosts(peers, localAddr)
	assert.Len(t, seeds, 2)
	assert.Contains(t, seeds, "127.0.0.2:8000")
	assert.Contains(t, seeds, "127.0.0.3:8000")
}

func TestJoinCluster_Success(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()
	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	_, _ = fmt.Sscanf(portStr, "%d", &port)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

			// Read length
			lenBuf := make([]byte, 4)
			_, _ = io.ReadFull(conn, lenBuf)
			length := binary.BigEndian.Uint32(lenBuf)

			// Read message
			msgBuf := make([]byte, length)
			_, _ = io.ReadFull(conn, msgBuf)

			// Send success response
			resp := map[string]interface{}{
				"success": true,
			}
			respData, _ := json.Marshal(resp)

			respLenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(respLenBuf, uint32(len(respData)))
			_, _ = conn.Write(respLenBuf)
			_, _ = conn.Write(respData)

	}()

	client := NewTCPClusterClient()
	peers := []string{addr}
	err = client.JoinCluster(peers, "test-node", "127.0.0.1:9000", port)
	assert.NoError(t, err)
}

func TestJoinCluster_Fail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()
	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	_, _ = fmt.Sscanf(portStr, "%d", &port)

	go func() {
		for i := 0; i < 5; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			// Read request
			lenBuf := make([]byte, 4)
			_, _ = io.ReadFull(conn, lenBuf)
			length := binary.BigEndian.Uint32(lenBuf)
			msgBuf := make([]byte, length)
			_, _ = io.ReadFull(conn, msgBuf)

			// Send failure response
			resp := map[string]interface{}{
				"success": false,
				"error":   "already joined",
			}
			respData, _ := json.Marshal(resp)

			respLenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(respLenBuf, uint32(len(respData)))
			_, _ = conn.Write(respLenBuf)
			_, _ = conn.Write(respData)
			_ = conn.Close()
		}
	}()

	client := NewTCPClusterClient()
	peers := []string{addr}

	// Create a short-lived context for faster testing of failure
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = client.joinClusterWithContext(ctx, peers, "test-node", "127.0.0.1:9000", port)
	assert.Error(t, err)
}

func TestJoinCluster_FailsOverFromUnresponsiveSeed(t *testing.T) {
	stallListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stallListener.Close() }()

	_, portStr, err := net.SplitHostPort(stallListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port %q: %v", portStr, err)
	}

	// Cluster membership stores hosts while the discovery port is shared by all
	// seeds, so bind the healthy seed to a second loopback address on the same
	// port.
	healthyListener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.2", portStr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = healthyListener.Close() }()

	stalled := make(chan struct{})
	stallDone := make(chan struct{})
	go func() {
		defer close(stallDone)
		conn, err := stallListener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return
		}
		msgBuf := make([]byte, binary.BigEndian.Uint32(lenBuf))
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			return
		}
		close(stalled)
		// Keep the connection open until the client's per-seed deadline closes it.
		_, _ = io.Copy(io.Discard, conn)
	}()

	healthyAccepted := make(chan struct{})
	go func() {
		conn, err := healthyListener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return
		}
		msgBuf := make([]byte, binary.BigEndian.Uint32(lenBuf))
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			return
		}
		close(healthyAccepted)

		respData, err := json.Marshal(map[string]bool{"success": true})
		if err != nil {
			return
		}
		respLenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(respLenBuf, uint32(len(respData)))
		_, _ = conn.Write(respLenBuf)
		_, _ = conn.Write(respData)
	}()

	client := NewTCPClusterClient()
	client.timeout = 75 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = client.joinClusterWithContext(
		ctx,
		[]string{"127.0.0.1:ignored", "127.0.0.2:ignored"},
		"test-node",
		"127.0.0.3:9000",
		port,
	)
	assert.NoError(t, err)

	select {
	case <-stalled:
	case <-time.After(time.Second):
		t.Fatal("unresponsive seed was not attempted")
	}
	select {
	case <-healthyAccepted:
	case <-time.After(time.Second):
		t.Fatal("healthy seed was not attempted after the stalled seed")
	}
	select {
	case <-stallDone:
	case <-time.After(time.Second):
		t.Fatal("stalled connection did not close after its attempt timed out")
	}
}

func TestStartHeartbeat(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()
	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	_, _ = fmt.Sscanf(portStr, "%d", &port)

	received := make(chan bool, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		received <- true
	}()

	client := NewTCPClusterClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client.StartHeartbeat(ctx, []string{addr}, "node-hb", "127.0.0.1:9001", port)

	select {
	case <-received:
		// Success
	case <-time.After(3 * time.Second):
		t.Fatal("Heartbeat not received")
	}
}
