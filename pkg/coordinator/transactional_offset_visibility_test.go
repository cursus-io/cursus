package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func TestTransactionalOffsetVisibilityRequiresCommittedDecision(t *testing.T) {
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	require.NoError(t, c.RegisterGroup("orders", "workers", 1))
	m := transaction.NewManager()
	c.SetTransactionalOffsetResolver(m)

	producer, epoch, err := m.InitProducerWithMode("tx-offset", transaction.ModeProcessingV1)
	require.NoError(t, err)
	require.NoError(t, m.Begin("tx-offset", producer, epoch))
	require.NoError(t, m.AddOffsets("tx-offset", producer, epoch, []transaction.OffsetOperation{{
		Topic: "orders", Group: "workers", Member: "member-1", Generation: 1,
		Partition: 0, Offset: 11, RegistrationEpoch: c.GetRegistrationEpoch("workers"),
	}}))
	_, err = m.PrepareCommit("tx-offset", producer, epoch)
	require.NoError(t, err)

	before, ok := c.GetOffset("workers", "orders", 0)
	require.False(t, ok)
	require.Equal(t, uint64(0), before, "prepared offset must remain invisible")
	require.NoError(t, m.Commit("tx-offset"))
	after, ok := c.GetOffset("workers", "orders", 0)
	require.True(t, ok)
	require.Equal(t, uint64(11), after)

	abortProducer, abortEpoch, err := m.InitProducerWithMode("tx-abort", transaction.ModeProcessingV1)
	require.NoError(t, err)
	require.NoError(t, m.Begin("tx-abort", abortProducer, abortEpoch))
	require.NoError(t, m.AddOffsets("tx-abort", abortProducer, abortEpoch, []transaction.OffsetOperation{{
		Topic: "orders", Group: "workers", Member: "member-1", Generation: 1,
		Partition: 0, Offset: 15, RegistrationEpoch: c.GetRegistrationEpoch("workers"),
	}}))
	aborted, err := m.PrepareAbort("tx-abort", abortProducer, abortEpoch)
	require.NoError(t, err)
	aborted.State = transaction.StateAborted
	aborted.Revision++
	aborted.UpdatedAt = time.Now()
	require.NoError(t, m.ApplyReplicatedSnapshot(&transaction.Snapshot{
		ID: aborted.ID, Mode: aborted.Mode, Producer: aborted.Producer, Epoch: aborted.Epoch,
		CoordinatorEpoch: aborted.CoordinatorEpoch, Revision: aborted.Revision, State: aborted.State,
		Offsets: aborted.Offsets, CreatedAt: aborted.CreatedAt, UpdatedAt: aborted.UpdatedAt,
	}))
	visible, ok := c.GetOffset("workers", "orders", 0)
	require.True(t, ok)
	require.Equal(t, uint64(11), visible, "aborted offset must never become visible")
}

func TestTransactionalOffsetRecordRoundTripIsDeterministic(t *testing.T) {
	record := ConsumerMetadataRecord{
		Version: ConsumerMetadataRecordVersionTransactions, Type: ConsumerMetadataRecordTransactionalOffsetSnapshot,
		Group: "workers", Topic: "orders", Epoch: 2, Revision: 7,
		Offsets:         []OffsetItem{{Partition: 1, Offset: 20}, {Partition: 0, Offset: 10}},
		TransactionalID: "tx-1", ProducerID: "producer-1", ProducerEpoch: 3, CoordinatorEpoch: 4,
		Timestamp: time.Unix(10, 0).UTC(),
	}
	payload, key, err := encodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	decoded, err := DecodeConsumerMetadataRecord(string(payload))
	require.NoError(t, err)
	require.Equal(t, canonicalConsumerMetadataRecord(record), decoded)
	secondPayload, secondKey, err := encodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	require.Equal(t, key, secondKey)
	require.Equal(t, payload, secondPayload)
}
