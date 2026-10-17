package client

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestJoinTransmitsOriginalProcessRegistration(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)
	received := make(chan wire.CommandPayload, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
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
		_ = connection.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: request.Command, Status: wire.StatusOK, RequestID: request.RequestID, Payload: []byte(`{"success":true}`)})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	registration := []byte(`{"id":"new","incarnation_id":"process-now","recovery_protocol":2}`)
	err = NewTCPClusterClient().JoinClusterRegistrationContext(ctx, []string{listener.Addr().String()}, "new", "127.0.0.2:7000", port, 7, registration)
	require.NoError(t, err)
	<-done
	select {
	case payload := <-received:
		require.Equal(t, string(registration), payload.Fields["registration"])
		require.Equal(t, "7", payload.Fields["transaction_coordinator_shards"])
		require.Equal(t, "new", payload.Fields["node_id"])
	default:
		t.Fatal("JOIN did not arrive")
	}
}
