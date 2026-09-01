package controller

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestBrokerRejectsIdempotenceWithWeakAcknowledgementsBeforeAppend(t *testing.T) {
	handler, manager := newTestHandler(t)
	require.NoError(t, manager.CreateTopic("idempotent", 1, true, false))
	partition, err := manager.GetTopic("idempotent").GetPartition(0)
	require.NoError(t, err)

	for _, acks := range []string{"0", "1"} {
		before := partition.NextOffset()
		response := handler.HandleCommand(
			"PUBLISH topic=idempotent partition=0 acks="+acks+" producerId=p1 isIdempotent=true seqNum=1 epoch=1 message=value",
			NewClientContext("", 0),
		)
		require.Contains(t, response, "ERROR: invalid_acks")
		require.Equal(t, before, partition.NextOffset())
	}
}

func TestBrokerRejectsIdempotentBatchWithWeakAcknowledgementsBeforeAppend(t *testing.T) {
	handler, manager := newTestHandler(t)
	require.NoError(t, manager.CreateTopic("idempotent-batch", 1, true, false))
	partition, err := manager.GetTopic("idempotent-batch").GetPartition(0)
	require.NoError(t, err)

	for _, acks := range []string{"0", "1"} {
		data, encodeErr := util.EncodeBatchMessages(
			"idempotent-batch",
			0,
			acks,
			true,
			[]types.Message{{Payload: "value", ProducerID: "p1", SeqNum: 1, Epoch: 1}},
		)
		require.NoError(t, encodeErr)
		before := partition.NextOffset()
		response, handleErr := handler.HandleBatchMessage(data, nil, NewClientContext("", 0))
		require.NoError(t, handleErr)
		require.Contains(t, response, "ERROR: invalid_acks")
		require.Equal(t, before, partition.NextOffset())
	}
}
