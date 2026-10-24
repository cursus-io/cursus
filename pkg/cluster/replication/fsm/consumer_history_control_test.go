package fsm

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryControlIdempotencyRequiresActivation(t *testing.T) {
	f := newTestFSM()
	f.partitionMetadata[config.ConsumerOffsetsTopicName+"-0"] = &PartitionMetadata{PartitionCount: 1, Idempotent: true}
	key, value, err := types.TransactionMarkerControlBytes(types.TransactionMarkerCommit, 7)
	require.NoError(t, err)
	m := types.Message{Payload: types.TransactionControlMarkerPayload, ProducerID: "txn-history:tx:commit", Epoch: 3, SeqNum: 7, TransactionalID: "tx", TransactionMarker: types.TransactionMarkerCommit, TransactionState: types.TransactionStateCommitted, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 7, ControlBatchKey: key, ControlBatchValue: value}
	cmd := &types.MessageCommand{Topic: config.ConsumerOffsetsTopicName, Messages: []types.Message{m}, SequenceScope: "partition"}
	require.Error(t, f.validateMessageCommand(cmd))
	f.consumerTransactionHistoryActivated = true
	require.NoError(t, f.validateMessageCommand(cmd))
	f.updateProducerState(cmd.Topic, 0, m.ProducerID, 3, 2)
	require.NoError(t, f.validateMessageCommand(cmd))
	f.updateProducerState(cmd.Topic, 0, m.ProducerID, 3, 7)
	require.NoError(t, f.validateMessageCommand(cmd), "exact retry is idempotent")
	f.updateProducerState(cmd.Topic, 0, m.ProducerID, 4, 7)
	require.ErrorContains(t, f.validateMessageCommand(cmd), "stale_producer_epoch")
	cmd.Messages[0].Epoch = 5
	require.NoError(t, f.validateMessageCommand(cmd))
	cmd.SequenceScope = "global"
	require.Error(t, f.validateMessageCommand(cmd))
}
