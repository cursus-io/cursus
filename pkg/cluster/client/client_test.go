package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/assert"
)

func serveJoinResponse(conn net.Conn, success bool, errorMessage string) {
	connection, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return
	}
	request, err := connection.ReadFrame()
	if err != nil {
		return
	}
	response, err := json.Marshal(map[string]any{"success": success, "error": errorMessage})
	if err != nil {
		return
	}
	_ = connection.WriteFrame(wire.Frame{
		Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK,
		RequestID: request.RequestID, Payload: response,
	})
}

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

		serveJoinResponse(conn, true, "")
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

			serveJoinResponse(conn, false, "already joined")
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

		codec, err := wire.NewCodec(wire.CompressionNone)
		if err != nil {
			return
		}
		if _, err := codec.ReadFrame(conn); err != nil {
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

		connection, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
		if err != nil {
			return
		}
		request, err := connection.ReadFrame()
		if err != nil {
			return
		}
		close(healthyAccepted)
		respData, err := json.Marshal(map[string]bool{"success": true})
		if err != nil {
			return
		}
		_ = connection.WriteFrame(wire.Frame{
			Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK,
			RequestID: request.RequestID, Payload: respData,
		})
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

	received := make(chan wire.CommandPayload, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		connection, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
		if err != nil {
			return
		}
		request, err := connection.ReadFrame()
		if err != nil {
			return
		}
		payload, err := wire.DecodeCommandPayload(request.Payload)
		if err != nil {
			return
		}
		received <- payload
		response, _ := json.Marshal(map[string]bool{"success": true})
		_ = connection.WriteFrame(wire.Frame{
			Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK,
			RequestID: request.RequestID, Payload: response,
		})
	}()

	client := NewTCPClusterClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proof := fsm.ISRCatchupProof{Topic: "orders", Partition: 0, BrokerID: "node-hb"}
	client.StartHeartbeat(ctx, nil, "node-hb", "process-2", addr, port, func() []fsm.ISRCatchupProof {
		return []fsm.ISRCatchupProof{proof}
	})

	select {
	case payload := <-received:
		assert.Equal(t, "node-hb", payload.Fields["node_id"])
		assert.Equal(t, "process-2", payload.Fields["incarnation_id"])
		var proofs []fsm.ISRCatchupProof
		assert.NoError(t, json.Unmarshal([]byte(payload.Fields["catchup_proofs"]), &proofs))
		assert.Equal(t, []fsm.ISRCatchupProof{proof}, proofs)
	case <-time.After(3 * time.Second):
		t.Fatal("Heartbeat not received")
	}
}
