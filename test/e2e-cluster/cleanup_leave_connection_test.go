package e2e_cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

// TestCleanupLeaveAfterIdleConnectionClose preserves membership and sends one
// LEAVE over fresh transport after the original management socket is closed.
func TestCleanupLeaveAfterIdleConnectionClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	closed := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		for _, command := range []wire.Command{wire.CommandHeartbeat, wire.CommandLeaveGroup} {
			if err := serveCleanupSessionCommand(listener, command); err != nil {
				result <- err
				return
			}
			if command == wire.CommandHeartbeat {
				close(closed)
			}
		}
		result <- nil
	}()
	client := e2e.NewBrokerClient([]string{listener.Addr().String()})
	defer client.Close()
	_, err = client.SendCommand("", fmt.Sprintf("HEARTBEAT topic=%s group=%s member=original generation=7", cleanupRecoveryTopic, cleanupRecoveryGroup), time.Second)
	require.NoError(t, err)
	select {
	case <-closed:
	case err := <-result:
		t.Fatalf("fixture failed before closing idle connection: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("fixture did not close the management connection")
	}
	leaveCleanupGroup(t, client, 7, "original")
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("LEAVE did not arrive on a fresh connection")
	}
}

// serveCleanupSessionCommand validates the exact membership without accepting a
// rejoin, then closes the socket as the broker's idle reader would.
func serveCleanupSessionCommand(listener net.Listener, command wire.Command) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	server, err := wire.ServerHandshake(conn, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	request, err := server.ReadFrame()
	if err != nil {
		return err
	}
	if request.Command != command {
		return fmt.Errorf("got %s, want %s", request.Command, command)
	}
	payload, err := wire.DecodeCommandPayload(request.Payload)
	if err != nil {
		return err
	}
	for key, want := range map[string]string{"topic": cleanupRecoveryTopic, "group": cleanupRecoveryGroup, "member": "original", "generation": "7"} {
		if payload.Fields[key] != want {
			return fmt.Errorf("changed %s: got %q want %q", key, payload.Fields[key], want)
		}
	}
	return server.WriteFrame(wire.Frame{Kind: wire.KindResponse, Command: command, RequestID: request.RequestID, Status: wire.StatusOK, Payload: []byte("OK")})
}
