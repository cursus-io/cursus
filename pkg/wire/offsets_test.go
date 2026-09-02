package wire_test

import (
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func TestOffsetPairsUseCanonicalNamedFieldValue(t *testing.T) {
	encoded, err := wire.EncodeOffsetPairs([]wire.OffsetPair{
		{Partition: 10, Offset: 90},
		{Partition: 0, Offset: 11},
		{Partition: 2, Offset: 30},
	})
	require.NoError(t, err)
	require.Equal(t, "P0:11,P2:30,P10:90", encoded)

	decoded, err := wire.DecodeOffsetPairs(encoded)
	require.NoError(t, err)
	require.Equal(t, []wire.OffsetPair{
		{Partition: 0, Offset: 11},
		{Partition: 2, Offset: 30},
		{Partition: 10, Offset: 90},
	}, decoded)
}

func TestOffsetPairsRejectMissingMalformedAndDuplicateValues(t *testing.T) {
	for _, value := range []string{"", "0:1", "P-1:1", "P0:nope", "P0:1,P0:2"} {
		_, err := wire.DecodeOffsetPairs(value)
		require.Error(t, err, value)
	}
	_, err := wire.EncodeOffsetPairs([]wire.OffsetPair{{Partition: -1, Offset: 1}})
	require.Error(t, err)
	_, err = wire.DecodeOffsetPairs(strings.Repeat("P0:1,", wire.MaxOffsetPairs) + "P1:1")
	require.ErrorContains(t, err, "exceeds maximum")
}

func TestNamedOffsetFieldSurvivesWireCommandRoundTrip(t *testing.T) {
	command, payload, err := wire.ParseCommandText(
		"SEND_OFFSETS_TO_TXN transactional_id=tx offsets=P0:1,P1:2 generation=3",
	)
	require.NoError(t, err)
	encoded, err := wire.EncodeCommandPayload(payload)
	require.NoError(t, err)
	decoded, err := wire.DecodeCommandPayload(encoded)
	require.NoError(t, err)
	rendered, err := wire.RenderCommand(command, decoded)
	require.NoError(t, err)
	require.Contains(t, rendered, "offsets=P0:1,P1:2")
	pairs, err := wire.DecodeOffsetPairs(decoded.Fields["offsets"])
	require.NoError(t, err)
	require.Len(t, pairs, 2)
}
