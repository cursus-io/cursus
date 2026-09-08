package sdk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTopicAssignments(t *testing.T) {
	assignments, err := parseTopicAssignments("orders:P0,orders:P2,payments:P1")
	require.NoError(t, err)
	require.Equal(t, []TopicPartition{
		{Topic: "orders", Partition: 0},
		{Topic: "orders", Partition: 2},
		{Topic: "payments", Partition: 1},
	}, assignments)
}

func TestParseTopicAssignmentsRejectsMalformedEntry(t *testing.T) {
	_, err := parseTopicAssignments("orders:P0,malformed,payments:P1")
	require.ErrorContains(t, err, "invalid topic assignment")
}

func TestDecodedMessageCarriesTopicPartitionIdentity(t *testing.T) {
	encoded, err := EncodeBatchMessages("orders", 3, "1", false, []Message{{Payload: "created"}})
	require.NoError(t, err)
	messages, _, _, err := DecodeBatchMessages(encoded)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "orders", messages[0].Topic)
	require.Equal(t, 3, messages[0].Partition)
}
