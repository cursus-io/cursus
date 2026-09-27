package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublishCommandWithPartitionPreservesAllClientFields(t *testing.T) {
	original := "PUBLISH topic=orders acks=all producerId=p1 seqNum=3 epoch=2 key=order-7 event_type=OrderPaid schema_version=4 metadata={trace:abc} aggregate_version=9 transaction_id=tx-2 message=payload with spaces"
	forwarded := publishCommandWithPartition(original, 2)
	args := parseKeyValueArgs(forwarded[len("PUBLISH "):])
	require.Equal(t, "2", args["partition"])
	require.Equal(t, "order-7", args["key"])
	require.Equal(t, "OrderPaid", args["event_type"])
	require.Equal(t, "4", args["schema_version"])
	require.Equal(t, "{trace:abc}", args["metadata"])
	require.Equal(t, "9", args["aggregate_version"])
	require.Equal(t, "tx-2", args["transaction_id"])
	require.Equal(t, "payload with spaces", args["message"])
}
