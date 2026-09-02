package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestDialEstablishesCanonicalWireConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	serverDone := make(chan error, 1)
	go func() {
		raw, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer func() { _ = raw.Close() }()
		server, handshakeErr := wire.ServerHandshake(raw, []wire.Compression{wire.CompressionNone})
		if handshakeErr != nil {
			serverDone <- handshakeErr
			return
		}
		request, readErr := server.ReadFrame()
		if readErr != nil {
			serverDone <- readErr
			return
		}
		serverDone <- server.WriteFrame(wire.Frame{
			Kind:      wire.KindResponse,
			Command:   request.Command,
			Status:    wire.StatusOK,
			RequestID: request.RequestID,
			Payload:   []byte("OK commands=HELP"),
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, err := Dial(ctx, listener.Addr().String(), DialConfig{
		DialTimeout:      time.Second,
		HandshakeTimeout: time.Second,
		Compression:      "none",
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	require.NoError(t, client.Send([]byte("HELP")))
	response, err := client.Receive()
	require.NoError(t, err)
	require.Equal(t, "OK commands=HELP", string(response))
	require.NoError(t, <-serverDone)
}

func TestDialRejectsInvalidInputBeforeOpeningSocket(t *testing.T) {
	_, err := Dial(nil, "127.0.0.1:1", DialConfig{}) //nolint:staticcheck // intentionally exercises the nil-context guard
	require.ErrorContains(t, err, "context is nil")

	_, err = Dial(context.Background(), "", DialConfig{})
	require.ErrorContains(t, err, "address is empty")
}
