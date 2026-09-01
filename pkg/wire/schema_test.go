package wire_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestCommandRegistryIsExactAndRoundTrips(t *testing.T) {
	commands := wire.Commands()
	require.NotEmpty(t, commands)
	seen := make(map[string]struct{}, len(commands))
	for _, command := range commands {
		name := command.String()
		_, duplicate := seen[name]
		require.False(t, duplicate, name)
		seen[name] = struct{}{}
		parsed, err := wire.ParseCommand(name)
		require.NoError(t, err)
		require.Equal(t, command, parsed)
	}
	_, err := wire.ParseCommand("HELP extra")
	require.Error(t, err)
}

func TestNegotiationAndErrorPayloadsAreDeterministic(t *testing.T) {
	request := wire.NegotiationRequest{
		MinimumVersion: 2, MaximumVersion: 2,
		Compressions: []wire.Compression{wire.CompressionSnappy, wire.CompressionNone},
	}
	encoded, err := wire.EncodeNegotiationRequest(request)
	require.NoError(t, err)
	decoded, err := wire.DecodeNegotiationRequest(encoded)
	require.NoError(t, err)
	require.Equal(t, request, decoded)
	selected, err := wire.SelectCompression(request, []wire.Compression{wire.CompressionNone, wire.CompressionSnappy})
	require.NoError(t, err)
	require.Equal(t, wire.CompressionSnappy, selected)

	errorPayload := wire.ErrorPayload{
		Code: "NOT_PARTITION_LEADER", Class: wire.ErrorClassRouting, Retryable: true,
		Message: "partition leader changed", Fields: map[string]string{"leader": "b2:9000", "partition": "3"},
	}
	first, err := wire.EncodeError(errorPayload)
	require.NoError(t, err)
	second, err := wire.EncodeError(errorPayload)
	require.NoError(t, err)
	require.Equal(t, first, second)
	decodedError, err := wire.DecodeError(first)
	require.NoError(t, err)
	require.Equal(t, errorPayload, decodedError)
}
