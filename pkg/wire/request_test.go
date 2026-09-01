package wire_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestCommandPayloadDeterministicRoundTrip(t *testing.T) {
	command, payload, err := wire.ParseCommandText("CREATE topic=orders partitions=3 read_acl=")
	require.NoError(t, err)
	require.Equal(t, wire.CommandCreate, command)
	encoded, err := wire.EncodeCommandPayload(payload)
	require.NoError(t, err)
	require.True(t, wire.IsCommandPayload(encoded))
	decoded, err := wire.DecodeCommandPayload(encoded)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
	rendered, err := wire.RenderCommand(command, decoded)
	require.NoError(t, err)
	require.Equal(t, "CREATE topic=orders partitions=3 read_acl=", rendered)

	encodedAgain, err := wire.EncodeCommandPayload(wire.CommandPayload{Fields: map[string]string{
		"read_acl": "", "topic": "orders", "partitions": "3",
	}})
	require.NoError(t, err)
	require.NotEqual(t, encoded, encodedAgain)
	encodedThird, err := wire.EncodeCommandPayload(wire.CommandPayload{Fields: map[string]string{
		"partitions": "3", "read_acl": "", "topic": "orders",
	}})
	require.NoError(t, err)
	require.Equal(t, encodedAgain, encodedThird)
}

func TestCommandPayloadRejectsDuplicateAndLegacyText(t *testing.T) {
	_, _, err := wire.ParseCommandText("CREATE topic=one topic=two")
	require.ErrorContains(t, err, "duplicate")
	_, err = wire.DecodeCommandPayload([]byte("CREATE topic=orders"))
	require.Error(t, err)
}

func TestCommandPayloadPreservesOpaqueMetadataAndMessage(t *testing.T) {
	command, payload, err := wire.ParseCommandText(
		`APPEND_STREAM topic=orders metadata={"trace": "abc 123"} message={"name": "Ada Lovelace"}`,
	)
	require.NoError(t, err)
	require.Equal(t, wire.CommandAppendStream, command)
	require.Equal(t, `{"trace": "abc 123"}`, payload.Fields["metadata"])
	require.Equal(t, `{"name": "Ada Lovelace"}`, payload.Fields["message"])
	encoded, err := wire.EncodeCommandPayload(payload)
	require.NoError(t, err)
	decoded, err := wire.DecodeCommandPayload(encoded)
	require.NoError(t, err)
	rendered, err := wire.RenderCommand(command, decoded)
	require.NoError(t, err)
	require.Equal(t,
		`APPEND_STREAM topic=orders metadata={"trace": "abc 123"} message={"name": "Ada Lovelace"}`,
		rendered,
	)
}
