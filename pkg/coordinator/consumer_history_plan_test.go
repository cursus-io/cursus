package coordinator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type historyDivergentReader struct{ ConsumerMetadataIndexReader }

func (r historyDivergentReader) ReadCommittedConsumerMetadata(topic string, p int, next uint64, max int) ([]types.Message, error) {
	messages, err := r.ConsumerMetadataIndexReader.ReadCommittedConsumerMetadata(topic, p, next, max)
	var out []types.Message
	for _, m := range messages {
		if p != 0 || m.Offset != 5 {
			out = append(out, m)
		}
	}
	return out, err
}

func TestConsumerHistoryPlanRejectsDivergentVisibility(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	_, err := c.PrepareConsumerHistoryCompaction(historyDivergentReader{r}, 0, 0, 11)
	require.ErrorContains(t, err, "raw and committed records differ")
	for _, bounds := range [][3]uint64{{0, 0, 12}, {0, 5, 5}, {0, 6, 5}, {99, 0, 1}} {
		_, err := c.PrepareConsumerHistoryCompaction(r, int(bounds[0]), bounds[1], bounds[2])
		require.Error(t, err)
	}
}

func applyHistoryPlanToFixture(r *replayIndexTestReader, plan *ConsumerHistoryCompactionPlan) {
	removed := make(map[uint64]bool)
	for _, m := range plan.RemovedRecords() {
		removed[m.Offset] = true
	}
	var retained []types.Message
	for _, m := range r.messages[plan.partition] {
		if !removed[m.Offset] {
			retained = append(retained, m)
		}
	}
	r.messages[plan.partition] = retained
}

func TestConsumerHistoryPlanFixedState(t *testing.T) {
	for _, history := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			c, r := newIndexFixture(t, history)
			for i := range r.messages[0] {
				r.messages[0][i].ProducerID = "consumer-offset-coordinator"
			}
			start := time.Now()
			plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, uint64(history+1))
			require.NoError(t, err)
			t.Logf("history=%d certification=%s committed_records=%d raw_records=%d removed=%d", history, time.Since(start), r.reads, r.rawReads, len(plan.removed))
			require.Len(t, plan.removed, history-1)
			require.Equal(t, uint64(1), plan.removed[0].Offset)
			require.Equal(t, uint64(history-1), plan.removed[len(plan.removed)-1].Offset)
			require.NoFileExists(t, filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json"))
			require.Empty(t, c.groups, "certification never installs replay state")
			applyHistoryPlanToFixture(r, plan)
			c.cfg.ConsumerRecoveryIndexEnabled = false
			p, err := c.PrepareFullConsumerMetadataReplay(r)
			require.NoError(t, err)
			require.Equal(t, uint64(history), p.scratch.groups["g"].Offsets["events"][0])
			require.Equal(t, uint64(history), p.scratch.groups["g"].OffsetRevisions["events"])
			require.Equal(t, 2, p.status.ReplayedRecords)
		})
	}
}

func TestConsumerHistoryPlanProducerRecoveryOrder(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	for i := range r.messages[0] {
		r.messages[0][i].ProducerID = "consumer-offset-coordinator"
		r.messages[0][i].Epoch = 2
		r.messages[0][i].SeqNum = uint64(i + 1)
	}
	// A physically later lower epoch must not displace the recovery anchor.
	r.messages[0][4].Epoch = 3
	r.messages[0][4].SeqNum = 1
	plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 11)
	require.NoError(t, err)
	applyHistoryPlanToFixture(r, plan)
	var offsets []uint64
	for _, m := range r.messages[0] {
		offsets = append(offsets, m.Offset)
	}
	require.Equal(t, []uint64{0, 4, 10}, offsets)
}

func TestConsumerHistoryPlanPreservesPendingIntentAndDecisionControl(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	intent := replayOffset("g", 1, 11, 11)
	intent.Version, intent.Type = 3, ConsumerMetadataRecordTransactionalOffsetSnapshot
	intent.TransactionalID, intent.ProducerID, intent.ProducerEpoch = "pending", "transaction-producer", 2
	message := encodedMetadataMessage(t, intent, 11)
	message.TransactionalID, message.ProducerID, message.Epoch, message.SeqNum, message.TransactionState = "pending", "transaction-producer", 2, 1, types.TransactionStateOpen
	r.messages[0] = append(r.messages[0], message)
	r.hidePending = true
	reservation := reservationFixture()
	reservation.Group, reservation.Epoch = "g", 1
	reservation.Reservations[0].Offsets = []ReservedTransactionOffset{{Topic: "events", Partition: 0, Offset: 11}}
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, reservation, 12))
	key, value, err := types.TransactionMarkerControlBytes(types.TransactionMarkerAbort, 2)
	require.NoError(t, err)
	control := types.Message{Offset: 13, Payload: types.TransactionControlMarkerPayload, TransactionalID: "previous", ProducerID: "txn-marker:previous:abort", Epoch: 2, SeqNum: 1, TransactionState: types.TransactionStateAborted, TransactionMarker: types.TransactionMarkerAbort, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 2, ControlBatchKey: key, ControlBatchValue: value}
	r.messages[0] = append(r.messages[0], control)
	plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 14)
	require.NoError(t, err)
	require.Len(t, plan.removed, 9)
	applyHistoryPlanToFixture(r, plan)
	require.Contains(t, r.messages[0], message)
	require.Contains(t, r.messages[0], control)
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.Equal(t, uint64(10), p.scratch.groups["g"].Offsets["events"][0])
	require.Equal(t, reservation.Reservations, p.scratch.groups["g"].OffsetReservations)
}

func TestConsumerHistoryPlanSegmentRangeAndCacheIndependence(t *testing.T) {
	c, r := newIndexFixture(t, 100)
	for _, cache := range []string{"missing", "valid", "corrupt"} {
		t.Run(cache, func(t *testing.T) {
			path := filepath.Join(c.cfg.LogDir, ".consumer-replay-index.json")
			switch cache {
			case "valid":
				_, err := c.PrepareFullConsumerMetadataReplay(r)
				require.NoError(t, err)
			case "corrupt":
				require.NoError(t, os.WriteFile(path, []byte("torn"), 0600))
			}
			plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 20, 40)
			require.NoError(t, err)
			require.Len(t, plan.removed, 20)
			require.Equal(t, uint64(20), plan.removed[0].Offset)
			require.Equal(t, uint64(39), plan.removed[19].Offset)
			copy := plan.RemovedRecords()
			copy[0].Offset = 999
			require.Equal(t, uint64(20), plan.removed[0].Offset)
		})
	}
}

func TestConsumerHistoryPlanPreservesEpochReservationsAndSources(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	appendRecord := func(record ConsumerMetadataRecord) {
		r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, record, uint64(len(r.messages[0]))))
	}
	appendRecord(ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: "g", Epoch: 2})
	appendRecord(replayRegistration("g", 3))
	appendRecord(replayOffset("g", 3, 1, 4))
	appendRecord(replayOffset("g", 3, 2, 5))
	reservation := reservationFixture()
	reservation.Group, reservation.Epoch = "g", 3
	reservation.Reservations[0].Offsets = []ReservedTransactionOffset{{Topic: "events", Partition: 0, Offset: 20}}
	reservation.ReservationDecisions = map[string]OffsetReservationDecision{"decided": {ProducerID: "prior", ProducerEpoch: 2, Committed: true}}
	appendRecord(reservation)
	appendRecord(replayRegistration("deleted", 1))
	appendRecord(ConsumerMetadataRecord{Version: 1, Type: ConsumerMetadataRecordTombstone, Group: "deleted", Epoch: 2})
	r.messages[1] = []types.Message{encodedMetadataMessage(t, replayOffset("g", 1, 1, 1), 0)}
	end := uint64(len(r.messages[0]))
	plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, end)
	require.NoError(t, err)
	applyHistoryPlanToFixture(r, plan)
	c.cfg.ConsumerRecoveryIndexEnabled = false
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.Equal(t, uint64(3), p.scratch.groupEpochs["g"])
	require.Equal(t, uint64(2), p.scratch.groupEpochs["deleted"])
	require.Nil(t, p.scratch.groups["deleted"])
	require.Len(t, p.scratch.groups["g"].OffsetReservations, 1)
	require.Equal(t, reservation.ReservationDecisions, p.scratch.groups["g"].ReservationDecisions)
	require.Equal(t, uint64(5), p.scratch.groups["g"].Offsets["events"][0])
	require.Contains(t, p.scratch.metadataSources["g"], 1)
	// The only historical source witness on another physical partition stays.
	plan, err = c.PrepareConsumerHistoryCompaction(r, 1, 0, 1)
	require.NoError(t, err)
	require.Empty(t, plan.removed)
}

func TestConsumerHistoryPlanRejectsUnsupportedAndCorruptHistory(t *testing.T) {
	for _, kind := range []string{"legacy", "transaction", "hidden transaction", "key", "conflict", "inferred", "sync", "generation", "bounds", "authority"} {
		t.Run(kind, func(t *testing.T) {
			c, r := newIndexFixture(t, 10)
			switch kind {
			case "legacy":
				r.messages[0][1] = types.Message{Offset: 1, Payload: `{"group":"g","topic":"events","partition":0,"offset":1}`}
			case "transaction", "hidden transaction":
				r.messages[0][1].TransactionalID = "tx"
				r.messages[0][1].TransactionState = types.TransactionStateOpen
				r.hidePending = kind == "hidden transaction"
			case "key":
				r.messages[0][1].Key = "not-the-record-key"
			case "conflict":
				r.messages[0][5] = encodedMetadataMessage(t, replayOffset("g", 1, 4, 500), 5)
			case "inferred":
				r.messages[0] = r.messages[0][1:]
			case "sync":
				r.syncErr = errors.New("injected fsync")
			case "generation":
				r.onRaw = func() { r.identity = "bootstrap-replacement" }
			case "bounds":
				r.onRaw = func() { r.end = map[int]uint64{0: 10} }
			case "authority":
				r.onRaw = func() { c.authorityRevision++ }
			}
			_, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 11)
			require.Error(t, err)
			require.Empty(t, c.groups)
		})
	}
}

func TestConsumerHistoryPlanInvalidatesOnChanges(t *testing.T) {
	for _, kind := range []string{"append", "generation", "authority", "lease", "owner"} {
		t.Run(kind, func(t *testing.T) {
			c, r := newIndexFixture(t, 10)
			plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 11)
			require.NoError(t, err)
			require.NoError(t, c.ValidateConsumerHistoryCompaction(r, plan))
			switch kind {
			case "append":
				r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, replayOffset("g", 1, 11, 11), 11))
			case "generation":
				r.identity = "restore-replacement"
			case "authority":
				c.authorityRevision++
			case "lease":
				c.consumerAdmission.lease++
			case "owner":
				c = newAuthorityTestCoordinator(t)
			}
			require.Error(t, c.ValidateConsumerHistoryCompaction(r, plan))
		})
	}
}

func TestConsumerHistoryPlanPreservesProducerAndLiveClocks(t *testing.T) {
	c, r := newIndexFixture(t, 10)
	r.messages[0][2].ProducerID = "producer"
	r.messages[0][2].SeqNum = 1
	lifecycle := ConsumerMetadataRecord{Version: 4, Type: ConsumerMetadataRecordLifecycleSnapshot, Group: "g", Epoch: 1, Revision: 1, Lifecycle: &GroupLifecycleSnapshot{TopicName: "events", Generation: 1, Partitions: []int{0}, Members: []GroupLifecycleMember{{ID: "m", Assignments: []int{0}}}}}
	r.messages[0] = append(r.messages[0], encodedMetadataMessage(t, lifecycle, 11))
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplay(p))
	group := c.groups["g"]
	clock := time.Now().Add(-time.Hour)
	group.Members["m"].LastHeartbeat = clock
	group.LastActivity = clock
	c.ownershipSince["g"] = clock
	for i := 0; i < 3; i++ {
		plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, 12)
		require.NoError(t, err)
		for _, m := range plan.removed {
			require.NotEqual(t, uint64(2), m.Offset)
		}
		require.Same(t, group, c.groups["g"])
		require.Equal(t, clock, group.Members["m"].LastHeartbeat)
		require.Equal(t, clock, group.LastActivity)
		require.Equal(t, clock, c.ownershipSince["g"])
	}
}
