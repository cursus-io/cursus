package coordinator

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type historyTransactionReader struct {
	*replayIndexTestReader
}

func (r *historyTransactionReader) ReadCommittedConsumerMetadata(topic string, p int, next uint64, max int) ([]types.Message, error) {
	messages, err := r.replayIndexTestReader.ReadCommittedConsumerMetadata(topic, p, next, max)
	return r.committedPrefix(p, messages), err
}

func (r *historyTransactionReader) ReadRawConsumerMetadataIndex(p int, next uint64, max int) ([]types.Message, error) {
	messages, err := r.replayIndexTestReader.ReadRawConsumerMetadataIndex(p, next, max)
	return r.committedPrefix(p, messages), err
}

func (r *historyTransactionReader) committedPrefix(p int, messages []types.Message) []types.Message {
	if end, known := r.end[p]; known {
		for i, m := range messages {
			if m.Offset >= end {
				return messages[:i]
			}
		}
	}
	return messages
}

func historyTransactionFixture(t *testing.T, epochs int, markerType string) (*Coordinator, *historyTransactionReader) {
	t.Helper()
	c, base := newIndexFixture(t, 1)
	r := &historyTransactionReader{base}
	for epoch := 1; epoch <= epochs; epoch++ {
		intent := replayOffset("g", 1, uint64(epoch), uint64(epoch))
		intent.Version, intent.Type = ConsumerMetadataRecordVersionTransactionHistory, ConsumerMetadataRecordTransactionalOffsetSnapshot
		intent.TransactionalID, intent.ProducerID, intent.ProducerEpoch, intent.CoordinatorEpoch = "tx", "producer", int64(epoch), 7
		m := encodedMetadataMessage(t, intent, uint64(len(r.messages[0])))
		m.TransactionalID, m.ProducerID, m.Epoch, m.SeqNum, m.TransactionState = "tx", "producer", int64(epoch), 1, types.TransactionStateOpen
		r.messages[0] = append(r.messages[0], m)
		key, value, err := types.TransactionMarkerControlBytes(markerType, 7)
		require.NoError(t, err)
		state := types.TransactionStateCommitted
		if markerType == types.TransactionMarkerAbort {
			state = types.TransactionStateAborted
		}
		r.messages[0] = append(r.messages[0], types.Message{Offset: uint64(len(r.messages[0])), Payload: types.TransactionControlMarkerPayload,
			TransactionalID: "tx", ProducerID: fmt.Sprintf("txn-history:tx:%s", markerType), Epoch: int64(epoch), SeqNum: 7,
			TransactionState: state, TransactionMarker: markerType, ControlBatchType: types.ControlBatchTransaction,
			ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 7, ControlBatchKey: key, ControlBatchValue: value})
	}
	return c, r
}

func TestConsumerHistoryRetiredTransactionHistoryBound(t *testing.T) {
	for _, state := range []string{types.TransactionMarkerCommit, types.TransactionMarkerAbort} {
		for _, epochs := range []int{100, 1000, 10000} {
			t.Run(fmt.Sprintf("%s/%d", state, epochs), func(t *testing.T) {
				c, r := historyTransactionFixture(t, epochs, state)
				end := uint64(len(r.messages[0]))
				p, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, end)
				require.NoError(t, err)
				require.Len(t, p.RemovedRecords(), 2*(epochs-1), "keep the latest raw producer and control witnesses")
				require.NoError(t, c.ValidateConsumerHistoryCompaction(r, p))
				applyHistoryPlanToFixture(r.replayIndexTestReader, p)
				r.end = map[int]uint64{0: end}
				require.Len(t, r.messages[0], 4)
				recovered, err := c.PrepareFullConsumerMetadataReplay(r)
				require.NoError(t, err)
				require.Equal(t, uint64(1), recovered.scratch.groups["g"].Offsets["events"][0])
			})
		}
	}
}

func TestConsumerHistoryTransactionMarkerWaitsForOtherSegments(t *testing.T) {
	c, r := historyTransactionFixture(t, 3, types.TransactionMarkerCommit)
	end := uint64(len(r.messages[0]))
	r.end = map[int]uint64{0: end}
	// Marker-only segment cannot drop the decision while its intent survives.
	p, err := c.PrepareConsumerHistoryCompaction(r, 0, 3, 4)
	require.NoError(t, err)
	require.Empty(t, p.RemovedRecords())
	// Removing the intent first is safe; the control still resolves raw replay.
	p, err = c.PrepareConsumerHistoryCompaction(r, 0, 2, 3)
	require.NoError(t, err)
	require.Len(t, p.RemovedRecords(), 1)
	applyHistoryPlanToFixture(r.replayIndexTestReader, p)
	p, err = c.PrepareConsumerHistoryCompaction(r, 0, 3, 4)
	require.NoError(t, err)
	require.Len(t, p.RemovedRecords(), 1)
	require.Equal(t, types.TransactionMarkerCommit, p.RemovedRecords()[0].TransactionMarker)
}

func TestConsumerHistoryTransactionFenceAndMarkerFailures(t *testing.T) {
	for _, kind := range []string{"producer_changed", "missing_marker", "conflicting_markers", "future_coordinator", "raw_producer_witness"} {
		t.Run(kind, func(t *testing.T) {
			c, r := historyTransactionFixture(t, 3, types.TransactionMarkerAbort)
			switch kind {
			case "producer_changed":
				for _, i := range []int{4, 6} {
					intent, _, err := decodeConsumerMetadataRecord(r.messages[0][i].Payload)
					require.NoError(t, err)
					intent.ProducerID = "other"
					r.messages[0][i].Payload = encodedMetadataMessage(t, intent, uint64(i)).Payload
					r.messages[0][i].ProducerID = "other"
				}
			case "missing_marker":
				r.messages[0] = append(r.messages[0][:3], r.messages[0][4:]...)
			case "conflicting_markers":
				duplicate := r.messages[0][3]
				duplicate.Offset = 8
				r.messages[0] = append(r.messages[0], duplicate)
			case "future_coordinator":
				intent, _, err := decodeConsumerMetadataRecord(r.messages[0][2].Payload)
				require.NoError(t, err)
				intent.CoordinatorEpoch = 8
				encoded := encodedMetadataMessage(t, intent, 2)
				r.messages[0][2].Payload = encoded.Payload
			case "raw_producer_witness":
				r.messages[0][2].SeqNum = 100
				r.messages[0][4].ProducerID, r.messages[0][6].ProducerID = "other", "other"
				// Changing only physical producer identity is rejected outright.
			}
			r.end = map[int]uint64{0: 9}
			p, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 9)
			if kind == "raw_producer_witness" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			for _, m := range p.RemovedRecords() {
				require.NotEqual(t, uint64(2), m.Offset, "unsafe old intent must survive")
				require.NotEqual(t, uint64(3), m.Offset, "unsafe control must survive")
			}
		})
	}
}

func TestConsumerHistoryRetiredTransactionCertificateRevalidates(t *testing.T) {
	c, r := historyTransactionFixture(t, 3, types.TransactionMarkerCommit)
	// The higher epochs physically exist but are outside the committed cut.
	r.end = map[int]uint64{0: 4}
	p, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 4)
	require.NoError(t, err)
	require.Empty(t, p.RemovedRecords(), "uncommitted epochs cannot authorize retirement")
	r.end[0] = 8
	p, err = c.PrepareConsumerHistoryCompaction(r, 0, 0, 8)
	require.NoError(t, err)
	require.Len(t, p.RemovedRecords(), 4)
	r.identity = "bootstrap-replacement"
	require.ErrorContains(t, c.ValidateConsumerHistoryCompaction(r, p), "source changed")
}

func TestConsumerHistoryLegacyTransactionsAreNeverRetirementAuthority(t *testing.T) {
	for _, legacy := range []int{2, 6} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			c, r := historyTransactionFixture(t, 3, types.TransactionMarkerCommit)
			record, _, err := decodeConsumerMetadataRecord(r.messages[0][legacy].Payload)
			require.NoError(t, err)
			record.Version = ConsumerMetadataRecordVersionTransactions
			r.messages[0][legacy].Payload = encodedMetadataMessage(t, record, uint64(legacy)).Payload
			p, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 8)
			require.NoError(t, err)
			for _, m := range p.RemovedRecords() {
				if legacy == 2 {
					require.NotEqual(t, uint64(2), m.Offset)
					require.NotEqual(t, uint64(3), m.Offset)
				}
				if legacy == 6 {
					require.Less(t, m.Offset, uint64(4), "legacy later intent cannot retire the preceding epoch")
				}
			}
		})
	}
}

func TestConsumerHistoryCoordinatorMovesKeepBoundedControlProducers(t *testing.T) {
	c, r := historyTransactionFixture(t, 100, types.TransactionMarkerCommit)
	for i := 2; i < len(r.messages[0]); i += 2 {
		epoch := int64(i)
		record, _, err := decodeConsumerMetadataRecord(r.messages[0][i].Payload)
		require.NoError(t, err)
		record.CoordinatorEpoch = epoch
		r.messages[0][i].Payload = encodedMetadataMessage(t, record, uint64(i)).Payload
		control := &r.messages[0][i+1]
		control.ControlBatchCoordinatorEpoch, control.SeqNum = epoch, uint64(epoch)
		control.ControlBatchKey, control.ControlBatchValue, err = types.TransactionMarkerControlBytes(control.TransactionMarker, epoch)
		require.NoError(t, err)
	}
	p, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, uint64(len(r.messages[0])))
	require.NoError(t, err)
	require.Len(t, p.RemovedRecords(), 198, "coordinator epochs do not create new producer witnesses")
}

func TestConsumerHistoryCurrentOpenEpochRemainsProtected(t *testing.T) {
	c, r := historyTransactionFixture(t, 3, types.TransactionMarkerCommit)
	r.messages[0] = r.messages[0][:7] // current epoch has no terminal marker
	r.hidePending = true
	p, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 7)
	require.NoError(t, err)
	for _, m := range p.RemovedRecords() {
		require.NotEqual(t, uint64(6), m.Offset)
	}
	applyHistoryPlanToFixture(r.replayIndexTestReader, p)
	require.Equal(t, types.TransactionStateOpen, r.messages[0][len(r.messages[0])-1].TransactionState)
}
