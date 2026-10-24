package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryControlSequenceAcrossCoordinatorMoves(t *testing.T) {
	p := &Partition{topic: config.ConsumerOffsetsTopicName, isIdempotent: true}
	key, value, err := types.TransactionMarkerControlBytes(types.TransactionMarkerCommit, 7)
	require.NoError(t, err)
	m := types.Message{Payload: types.TransactionControlMarkerPayload, ProducerID: "txn-history:tx:commit", Epoch: 3, SeqNum: 7, TransactionalID: "tx", TransactionMarker: types.TransactionMarkerCommit, TransactionState: types.TransactionStateCommitted, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 7, ControlBatchKey: key, ControlBatchValue: value}
	duplicate, err := p.validateProducerMessage(&m)
	require.NoError(t, err)
	require.False(t, duplicate)
	duplicate, err = p.validateAgainstProducerState(&m, 3, 2, true)
	require.NoError(t, err)
	require.False(t, duplicate, "coordinator epochs need not be consecutive")
	duplicate, err = p.validateAgainstProducerState(&m, 3, 7, true)
	require.NoError(t, err)
	require.True(t, duplicate)
	_, err = p.validateAgainstProducerState(&m, 4, 1, true)
	require.ErrorContains(t, err, "stale_producer_epoch")
	m.Epoch = 4
	_, err = p.validateAgainstProducerState(&m, 3, 7, true)
	require.NoError(t, err)
	p.topic = "ordinary"
	_, err = p.validateProducerMessage(&m)
	require.Error(t, err, "ordinary topics keep dense sequence rules")
	p.topic = config.ConsumerOffsetsTopicName
	m.Payload = "invalid"
	_, err = p.validateProducerMessage(&m)
	require.Error(t, err, "a producer prefix alone is not authority")
}
