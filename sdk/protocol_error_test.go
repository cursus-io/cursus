package sdk

import (
	"errors"
	"net"
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/sdk/internal/transport"
	"github.com/stretchr/testify/require"
)

func TestReadWithLengthPreservesStructuredWireError(t *testing.T) {
	clientNet, serverNet := net.Pipe()
	defer func() { _ = clientNet.Close() }()
	defer func() { _ = serverNet.Close() }()

	serverDone := make(chan error, 1)
	go func() {
		server, err := wire.ServerHandshake(serverNet, []wire.Compression{wire.CompressionNone})
		if err != nil {
			serverDone <- err
			return
		}
		request, err := server.ReadFrame()
		if err != nil {
			serverDone <- err
			return
		}
		payload, err := wire.EncodeError(wire.ErrorPayload{
			Code:      "NOT_COORDINATOR",
			Class:     wire.ErrorClassRouting,
			Retryable: true,
			Message:   "coordinator moved",
			Fields:    map[string]string{"host": "broker-2", "port": "9092"},
		})
		if err != nil {
			serverDone <- err
			return
		}
		serverDone <- server.WriteFrame(wire.Frame{
			Kind:      wire.KindResponse,
			Command:   request.Command,
			Status:    wire.StatusError,
			RequestID: request.RequestID,
			Payload:   payload,
		})
	}()

	client, err := transport.NewClient(clientNet, "none")
	require.NoError(t, err)
	require.NoError(t, WriteWithLength(client, []byte("HELP")))
	response, err := ReadWithLength(client)
	require.Nil(t, response)
	var brokerErr *BrokerError
	require.True(t, errors.As(err, &brokerErr), "%T %v", err, err)
	require.Equal(t, "NOT_COORDINATOR", brokerErr.Code)
	require.Equal(t, ErrorClassRouting, brokerErr.Class)
	require.True(t, brokerErr.Retryable)
	require.Equal(t, "coordinator moved", brokerErr.Message)
	require.Equal(t, "broker-2", brokerErr.Fields["host"])
	require.NoError(t, <-serverDone)
}
