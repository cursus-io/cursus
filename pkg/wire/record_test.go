package wire_test

import (
	"reflect"
	"testing"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/require"
)

func fullMessage() wire.Message {
	return wire.Message{
		Topic: "orders", Partition: 3, Offset: 42, Timestamp: 1_725_000_000_123,
		ProducerID: "producer-1", SeqNum: 9, Epoch: 4, Key: "order-7", Payload: `{"status":"paid"}`,
		EventType: "OrderPaid", SchemaVersion: 2, AggregateVersion: 11, Metadata: `{"trace":"abc"}`,
		TransactionalID: "txn-1", TransactionState: wire.TransactionStateCommitted,
		TransactionMarker: wire.TransactionMarkerCommit, ControlBatchType: wire.ControlBatchTransaction,
		ControlBatchVersion: wire.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 8,
		ControlBatchKey: []byte{0, 1, 2}, ControlBatchValue: []byte{3, 4, 5},
	}
}

func TestRecordRoundTripPreservesEventTransactionAndControlFields(t *testing.T) {
	want := fullMessage()
	encoded, err := wire.EncodeRecord(want)
	require.NoError(t, err)
	got, err := wire.DecodeRecord(encoded)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(want, got), "record mismatch\nwant=%+v\n got=%+v", want, got)
}

func TestRecordRoundTripPreservesNegativeSignedFields(t *testing.T) {
	want := wire.Message{
		Topic:                        "orders",
		Partition:                    -1,
		Timestamp:                    -2,
		Epoch:                        -3,
		ControlBatchVersion:          -4,
		ControlBatchCoordinatorEpoch: -5,
	}
	encoded, err := wire.EncodeRecord(want)
	require.NoError(t, err)
	got, err := wire.DecodeRecord(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestBatchRoundTripUsesWireV2AndRejectsLegacyMagic(t *testing.T) {
	want := wire.Batch{
		Topic: "orders", Partition: 3, Acks: "all", IsIdempotent: true,
		Messages: []wire.Message{fullMessage(), {ProducerID: "producer-1", SeqNum: 10, Payload: "next"}},
	}
	encoded, err := wire.EncodeBatch(want)
	require.NoError(t, err)
	require.True(t, wire.IsBatch(encoded))
	require.False(t, wire.IsBatch([]byte{0xba, 0x7c, 0, 0, 0, 0}))
	got, err := wire.DecodeBatch(encoded)
	require.NoError(t, err)
	require.Equal(t, want.Topic, got.Topic)
	require.Equal(t, want.Partition, got.Partition)
	require.Equal(t, uint64(9), got.BatchStart)
	require.Equal(t, uint64(10), got.BatchEnd)
	require.Len(t, got.Messages, 2)
	require.Equal(t, wire.TransactionStateCommitted, got.Messages[0].TransactionState)
	require.Equal(t, wire.TransactionMarkerCommit, got.Messages[0].TransactionMarker)
	require.Equal(t, []byte{3, 4, 5}, got.Messages[0].ControlBatchValue)
}

func TestRecordRejectsUnknownTransactionEnumsAndTrailingData(t *testing.T) {
	_, err := wire.EncodeRecord(wire.Message{TransactionState: "mystery"})
	require.ErrorContains(t, err, "invalid transaction state")

	encoded, err := wire.EncodeRecord(wire.Message{Topic: "t", Payload: "p"})
	require.NoError(t, err)
	_, err = wire.DecodeRecord(append(encoded, 0))
	require.ErrorContains(t, err, "trailing")
}

func FuzzDecodeRecord(f *testing.F) {
	seed, _ := wire.EncodeRecord(wire.Message{Topic: "events", Payload: "value"})
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = wire.DecodeRecord(data)
	})
}

func FuzzDecodeBatch(f *testing.F) {
	seed, _ := wire.EncodeBatch(wire.Batch{Topic: "events", Acks: "1", Messages: []wire.Message{{Payload: "value"}}})
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = wire.DecodeBatch(data)
	})
}
