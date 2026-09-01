package wire_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestBrokerAndSDKUseIdenticalWireV2BatchCodec(t *testing.T) {
	message := wire.Message{
		ProducerID: "p1", SeqNum: 7, Payload: "value", Key: "key",
		EventType: "Created", SchemaVersion: 2, AggregateVersion: 5, Metadata: "{}",
		TransactionalID: "tx-1", TransactionState: wire.TransactionStateAborted,
		TransactionMarker: wire.TransactionMarkerAbort, ControlBatchType: wire.ControlBatchTransaction,
		ControlBatchVersion: wire.ControlBatchVersionCursusV2,
	}
	brokerEncoded, err := util.EncodeBatchMessages("orders", 2, "all", true, []types.Message{message})
	require.NoError(t, err)
	sdkEncoded, err := sdk.EncodeBatchMessages("orders", 2, "all", true, []sdk.Message{message})
	require.NoError(t, err)
	require.Equal(t, brokerEncoded, sdkEncoded)

	brokerDecoded, err := util.DecodeBatchMessages(sdkEncoded)
	require.NoError(t, err)
	sdkDecoded, topicName, partition, err := sdk.DecodeBatchMessages(brokerEncoded)
	require.NoError(t, err)
	require.Equal(t, "orders", topicName)
	require.Equal(t, 2, partition)
	require.Equal(t, brokerDecoded.Messages, sdkDecoded)
	require.Equal(t, wire.TransactionStateAborted, sdkDecoded[0].TransactionState)
	require.Equal(t, wire.TransactionMarkerAbort, sdkDecoded[0].TransactionMarker)
	require.Equal(t, wire.MaxFramePayload, util.MaxMessageSize)
	require.Equal(t, wire.MaxFramePayload, sdk.MaxMessageSize)
}
