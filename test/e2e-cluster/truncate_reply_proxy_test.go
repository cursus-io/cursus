package e2e_cluster

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

// Forward one real request and consume its committed response, then close the
// client connection without forwarding that response. No broker result is faked.
func dropTruncateReplyProxy(t *testing.T, upstream string) (string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() { done <- forwardAndDropTruncateReply(listener, upstream) }()
	return listener.Addr().String(), done
}

func forwardAndDropTruncateReply(listener net.Listener, upstream string) error {
	client, err := listener.Accept()
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	_ = client.SetDeadline(time.Now().Add(30 * time.Second))
	down, err := wire.ServerHandshake(client, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	broker, err := net.DialTimeout("tcp", upstream, 5*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = broker.Close() }()
	_ = broker.SetDeadline(time.Now().Add(30 * time.Second))
	up, err := wire.ClientHandshake(broker, []wire.Compression{wire.CompressionNone})
	if err != nil {
		return err
	}
	request, err := down.ReadFrame()
	if err != nil {
		return err
	}
	if request.Command != wire.CommandTruncateInstance {
		return fmt.Errorf("unexpected command %s", request.Command)
	}
	if err := up.WriteFrame(request); err != nil {
		return err
	}
	response, err := up.ReadFrame()
	if err != nil {
		return err
	}
	if response.Status != wire.StatusOK || !strings.Contains(string(response.Payload), "truncated=true") {
		return fmt.Errorf("truncate did not commit: %s", response.Payload)
	}
	return nil
}
