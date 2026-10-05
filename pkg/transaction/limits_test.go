package transaction

import (
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestManagerBoundsTransactionStateBeforeMutation(t *testing.T) {
	limits := Limits{MaxTransactions: 2, MaxRecords: 1, MaxBytes: 64, MaxOffsets: 1}
	manager := NewManagerWithLimits(time.Hour, 1, limits)
	producer, epoch := beginInitialized(t, manager, "bounded")

	require.NoError(t, manager.AddMessage("bounded", producer, epoch, MessageOperation{Topic: "t", Message: types.Message{Payload: "one"}}))
	require.ErrorContains(t, manager.AddMessage("bounded", producer, epoch, MessageOperation{Topic: "t", Message: types.Message{Payload: "two"}}), "max_transaction_records=1")

	status, err := manager.Status("bounded")
	require.NoError(t, err)
	require.Len(t, status.Messages, 1)
}

func TestManagerBoundsTransactionPayloadAndOffsets(t *testing.T) {
	manager := NewManagerWithLimits(time.Hour, 1, Limits{MaxTransactions: 2, MaxRecords: 10, MaxBytes: 8, MaxOffsets: 1})
	producer, epoch := beginInitialized(t, manager, "bounded")
	require.ErrorContains(t, manager.AddMessage("bounded", producer, epoch, MessageOperation{Topic: "t", Message: types.Message{Payload: strings.Repeat("x", 8)}}), "max_transaction_bytes=8")
	require.NoError(t, manager.AddOffsets("bounded", producer, epoch, []OffsetOperation{{Topic: "t", Group: "g", Member: "m", Partition: 0, Offset: 1}}))
	require.ErrorContains(t, manager.AddOffsets("bounded", producer, epoch, []OffsetOperation{{Topic: "t", Group: "g", Member: "m", Partition: 1, Offset: 1}}), "max_transaction_offsets=1")
}

func TestManagerBoundsRetainedTransactionIdentities(t *testing.T) {
	manager := NewManagerWithLimits(time.Hour, 1, Limits{MaxTransactions: 1, MaxRecords: 10, MaxBytes: 1024, MaxOffsets: 10})
	_, _, err := manager.InitProducer("first")
	require.NoError(t, err)
	_, _, err = manager.InitProducer("second")
	require.ErrorContains(t, err, "max_transactions=1")
	manager.Delete("first")
	_, _, err = manager.InitProducer("second")
	require.NoError(t, err)
}

func TestRequestAssignmentLimitFailureDoesNotConsumeSequence(t *testing.T) {
	manager := NewManagerWithLimits(time.Hour, 1, Limits{MaxTransactions: 2, MaxRecords: 1, MaxBytes: 4096, MaxOffsets: 10})
	producer, epoch, err := manager.InitProducerWithMode("processing", ModeProcessingV1)
	require.NoError(t, err)
	require.NoError(t, manager.Begin("processing", producer, epoch))
	first, found, err := manager.ResolveRequestAssignment("processing", producer, epoch, "t", -1, 1, 0, "first")
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, uint64(1), first.Sequence)
	_, _, err = manager.ResolveRequestAssignment("processing", producer, epoch, "t", -1, 2, 0, "second")
	require.ErrorContains(t, err, "max_transaction_records=1")

	snapshot, ok := manager.Snapshot("processing")
	require.True(t, ok)
	require.Len(t, snapshot.RequestAssignments, 1)
	require.Equal(t, uint64(1), snapshot.SequenceByPartition["t:0"])
}
