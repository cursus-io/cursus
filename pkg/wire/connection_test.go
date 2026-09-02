package wire_test

import (
	"net"
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestNegotiatedConnectionRoundTrip(t *testing.T) {
	clientRaw, serverRaw := net.Pipe()
	t.Cleanup(func() { _ = clientRaw.Close() })
	t.Cleanup(func() { _ = serverRaw.Close() })

	serverResult := make(chan *wire.Connection, 1)
	serverError := make(chan error, 1)
	go func() {
		connection, err := wire.ServerHandshake(serverRaw, []wire.Compression{wire.CompressionNone, wire.CompressionGZIP})
		serverResult <- connection
		serverError <- err
	}()
	client, err := wire.ClientHandshake(clientRaw, []wire.Compression{wire.CompressionGZIP, wire.CompressionNone})
	require.NoError(t, err)
	server := <-serverResult
	require.NoError(t, <-serverError)
	require.Equal(t, wire.CompressionGZIP, client.Compression())
	require.Equal(t, client.Compression(), server.Compression())

	request := wire.Frame{Version: wire.ProtocolVersion, Kind: wire.KindRequest, Command: wire.CommandHelp, RequestID: 7, Payload: []byte("HELP")}
	go func() { serverError <- client.WriteFrame(request) }()
	got, err := server.ReadFrame()
	require.NoError(t, err)
	require.NoError(t, <-serverError)
	require.Equal(t, request, got)

	response := wire.Frame{Version: wire.ProtocolVersion, Kind: wire.KindResponse, Command: got.Command, Status: wire.StatusOK, RequestID: got.RequestID, Payload: []byte("OK")}
	go func() { serverError <- server.WriteFrame(response) }()
	got, err = client.ReadFrame()
	require.NoError(t, err)
	require.NoError(t, <-serverError)
	require.Equal(t, response, got)
}
