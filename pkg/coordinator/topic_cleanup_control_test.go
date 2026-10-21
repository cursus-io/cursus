package coordinator

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func cleanupControlMessage(t *testing.T, marker string, processing bool) types.Message {
	t.Helper()
	const epoch = 7
	key, value, err := types.TransactionMarkerControlBytes(marker, epoch)
	require.NoError(t, err)
	state := types.TransactionStateCommitted
	if marker == types.TransactionMarkerAbort {
		state = types.TransactionStateAborted
	}
	producer := "txn-marker:history:" + marker
	if processing {
		producer = fmt.Sprintf("txn-marker:history:%d:%s", epoch, marker)
	}
	return types.Message{
		Payload: "__cursus_txn_control_marker__", ProducerID: producer, SeqNum: 1, Epoch: epoch,
		TransactionalID: "history", TransactionState: state, TransactionMarker: marker,
		ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2,
		ControlBatchCoordinatorEpoch: epoch, ControlBatchKey: key, ControlBatchValue: value,
	}
}

func TestTopicCleanupAdvancesPastTransactionControls(t *testing.T) {
	for _, processing := range []bool{false, true} {
		t.Run(fmt.Sprintf("processing=%t", processing), func(t *testing.T) {
			c, log := newCleanupFollower(t)
			c.standalone = true
			c.SetOffsetRecordWriter(nil)
			require.NoError(t, c.RegisterGroup("payments", "unrelated", 1))
			_, err := c.AddConsumer("unrelated", "live-member")
			require.NoError(t, err)
			require.NoError(t, c.CommitOffset("unrelated", "payments", 0, 29))
			c.standalone = false
			unrelated := c.ExportState()["unrelated"]
			// Raw committed metadata includes transaction data and non-JSON
			// control records, even when they belong to unrelated groups.
			for i := range topicCleanupRecords + 1 {
				log.append(types.Message{Payload: `{"group":"unrelated"}`, TransactionalID: "history", TransactionState: types.TransactionStateOpen})
				marker := types.TransactionMarkerCommit
				if i%2 != 0 {
					marker = types.TransactionMarkerAbort
				}
				log.append(cleanupControlMessage(t, marker, processing))
			}
			log.append(cleanupMessage(t, cleanupDeletion()))
			for range 2 {
				before := log.records
				deleted, err := c.ReconcileTopicGroups("orders")
				require.ErrorContains(t, err, "budget")
				require.Empty(t, deleted)
				require.NotNil(t, c.GetGroup("workers"))
				require.Equal(t, topicCleanupRecords, log.records-before)
				require.Equal(t, uint64(log.records), c.cleanupProgress["orders"].next[0])
			}
			deleted, err := c.ReconcileTopicGroups("orders")
			require.NoError(t, err)
			require.Equal(t, []string{"workers"}, deleted)
			require.Equal(t, len(log.messages), log.records, "control records count against the budget and are not reread")
			require.Equal(t, []uint64{0, 128, 256}, log.reads)
			require.Equal(t, unrelated, c.ExportState()["unrelated"])
		})
	}
}

func TestTopicCleanupControlCannotSupplyDeletion(t *testing.T) {
	c, log := newCleanupFollower(t)
	log.append(cleanupControlMessage(t, types.TransactionMarkerCommit, true))
	log.append(cleanupControlMessage(t, types.TransactionMarkerAbort, true))
	before := c.ExportState()
	for range 2 {
		deleted, err := c.ReconcileTopicGroups("orders")
		require.ErrorContains(t, err, "waiting for a newer committed group tombstone")
		require.Empty(t, deleted)
		require.Equal(t, before, c.ExportState())
	}
	require.Equal(t, []uint64{0}, log.reads)
}

func TestTopicCleanupRejectsMalformedControlBeforeDeletion(t *testing.T) {
	mutations := map[string]func(*types.Message){
		"missing transaction": func(m *types.Message) { m.TransactionalID = "" },
		"unknown marker":      func(m *types.Message) { m.TransactionMarker = "invalid" },
		"missing marker":      func(m *types.Message) { m.TransactionMarker = "" },
		"wrong state":         func(m *types.Message) { m.TransactionState = types.TransactionStateAborted },
		"unknown type":        func(m *types.Message) { m.ControlBatchType = "invalid" },
		"missing type":        func(m *types.Message) { m.ControlBatchType = "" },
		"wrong version":       func(m *types.Message) { m.ControlBatchVersion++ },
		"wrong epoch":         func(m *types.Message) { m.ControlBatchCoordinatorEpoch++ },
		"overflow epoch":      func(m *types.Message) { m.ControlBatchCoordinatorEpoch = 1 << 31 },
		"wrong key bytes":     func(m *types.Message) { m.ControlBatchKey[3]++ },
		"wrong value bytes":   func(m *types.Message) { m.ControlBatchValue[5]++ },
		"wrong producer":      func(m *types.Message) { m.ProducerID = "client" },
		"wrong sequence":      func(m *types.Message) { m.SeqNum++ },
		"unknown payload":     func(m *types.Message) { m.Payload = "malformed" },
		"tracked key":         func(m *types.Message) { m.Key = consumerMetadataRecordKey(cleanupDeletion()) },
		"unrelated key":       func(m *types.Message) { m.Key = "another-key" },
		"lifecycle payload":   func(m *types.Message) { m.Payload = cleanupMessage(t, cleanupDeletion()).Payload },
		"event metadata":      func(m *types.Message) { m.Metadata = `{"group":"workers"}` },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c, log := newCleanupFollower(t)
			m := cleanupControlMessage(t, types.TransactionMarkerCommit, true)
			mutate(&m)
			log.append(m)
			log.append(cleanupMessage(t, cleanupDeletion()))
			before := c.ExportState()
			for range 2 {
				deleted, err := c.ReconcileTopicGroups("orders")
				require.Error(t, err)
				require.Empty(t, deleted)
				require.Equal(t, before, c.ExportState())
			}
		})
	}
}
