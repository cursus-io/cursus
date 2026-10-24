package coordinator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerReplayIndexTransactionEvidenceAndDecisionChange(t *testing.T) {
	c, r := newIndexFixture(t, 100)
	intent := replayOffset("g", 1, 101, 101)
	intent.Version, intent.Type = ConsumerMetadataRecordVersionTransactionHistory, ConsumerMetadataRecordTransactionalOffsetSnapshot
	intent.TransactionalID, intent.ProducerID, intent.ProducerEpoch, intent.CoordinatorEpoch = "tx", "producer", 3, 7
	m := encodedMetadataMessage(t, intent, 0)
	m.TransactionalID, m.ProducerID, m.Epoch, m.SeqNum, m.TransactionState = "tx", "producer", 3, 1, types.TransactionStateOpen
	r.messages[1] = []types.Message{m}
	r.hidePending = true
	first, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.Positive(t, first.status.IndexWriteBytes)
	require.NotContains(t, first.scratch.metadataSources["g"], 1)
	data, err := os.ReadFile(filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json"))
	require.NoError(t, err)
	var envelope cleanupCheckpointEnvelope
	require.NoError(t, json.Unmarshal(data, &envelope))
	var saved consumerReplayIndex
	require.NoError(t, json.Unmarshal(envelope.Payload, &saved))
	require.Equal(t, 2, saved.Version)
	require.Len(t, saved.Transactions, 1)
	// Decision visibility changes with no committed end or disk generation change.
	r.hidePending = false
	next, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.True(t, next.status.IndexHit)
	require.Contains(t, next.scratch.metadataSources["g"], 1)
	require.Equal(t, uint64(100), next.scratch.groups["g"].Offsets["events"][0], "intent never replaces an ordinary materialization")
	require.Less(t, next.status.IndexSourceRecords, 10, "current raw evidence is resolved without rereading ordinary history")
	r.hidePending = true
	next, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.True(t, next.status.IndexHit)
	require.NotContains(t, next.scratch.metadataSources["g"], 1, "cache cannot retain an obsolete visibility decision")
}

func TestConsumerReplayIndexTransactionHistorySuffixAndFallback(t *testing.T) {
	c, r := newIndexFixture(t, 1)
	for epoch := int64(1); epoch <= 3; epoch++ {
		record := replayOffset("g", 1, uint64(epoch), 1)
		record.Version, record.Type = ConsumerMetadataRecordVersionTransactionHistory, ConsumerMetadataRecordTransactionalOffsetSnapshot
		record.TransactionalID, record.ProducerID, record.ProducerEpoch, record.CoordinatorEpoch = "tx", "producer", epoch, 7
		intent := encodedMetadataMessage(t, record, uint64(len(r.messages[0])))
		intent.TransactionalID, intent.ProducerID, intent.Epoch, intent.SeqNum, intent.TransactionState = "tx", "producer", epoch, 1, types.TransactionStateOpen
		r.messages[0] = append(r.messages[0], intent)
		key, value, err := types.TransactionMarkerControlBytes(types.TransactionMarkerAbort, 7)
		require.NoError(t, err)
		r.messages[0] = append(r.messages[0], types.Message{Offset: uint64(len(r.messages[0])), Payload: types.TransactionControlMarkerPayload, ProducerID: "txn-history:tx:abort", Epoch: epoch, SeqNum: 7, TransactionalID: "tx", TransactionMarker: types.TransactionMarkerAbort, TransactionState: types.TransactionStateAborted, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 7, ControlBatchKey: key, ControlBatchValue: value})
	}
	first, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.Positive(t, first.status.IndexWriteBytes)
	next, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.True(t, next.status.IndexHit)
	require.Equal(t, first.scratch.ExportState(), next.scratch.ExportState())
	require.Equal(t, first.scratch.metadataSources, next.scratch.metadataSources)
	// Raw generation replacement invalidates all saved controls and intents.
	r.identity = "bootstrap-replacement"
	next, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.False(t, next.status.IndexHit)
	require.Equal(t, first.scratch.ExportState(), next.scratch.ExportState())
	// A legacy/incompatible suffix cannot be silently omitted by a v2 cache.
	invalid := encodedMetadataMessage(t, replayOffset("g", 1, 99, 99), 8)
	invalid.TransactionalID, invalid.TransactionState = "unknown", types.TransactionStateOpen
	r.messages[0] = append(r.messages[0], invalid)
	r.hidePending = true
	next, err = c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.False(t, next.status.IndexHit)
	require.Zero(t, next.status.IndexWriteBytes)
}
