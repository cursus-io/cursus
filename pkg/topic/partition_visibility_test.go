package topic

import (
	"strconv"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestTransactionVisibilityIndexPrunesResolvedEntriesAtRetentionFloor(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetLatestOffset").Return(uint64(0)).Once()
	partition := NewPartition(0, "orders", storage, nil, config.DefaultConfig())

	partition.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "resolved", TransactionState: types.TransactionStateOpen})
	partition.indexTransactionMessage(types.Message{Offset: 2, TransactionalID: "resolved", TransactionMarker: types.TransactionMarkerCommit})
	partition.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "unresolved", TransactionState: types.TransactionStateOpen})
	partition.pruneTransactionIndex(3)

	partition.txnMarkerMu.RLock()
	_, resolvedOpen := partition.txnOpenOffsets[transactionMarkerKey{transactionalID: "resolved"}]
	_, resolvedMarker := partition.txnMarkers[transactionMarkerKey{transactionalID: "resolved"}]
	unresolvedOffset := partition.txnOpenOffsets[transactionMarkerKey{transactionalID: "unresolved"}]
	floor := partition.txnRetentionFloor
	stableOffset := firstUnresolvedOpenOffset(10, floor, partition.txnOpenOffsets, partition.txnMarkers, nil)
	partition.txnMarkerMu.RUnlock()
	require.False(t, resolvedOpen)
	require.False(t, resolvedMarker)
	require.Equal(t, uint64(1), unresolvedOffset)
	require.Equal(t, uint64(3), floor)
	require.Equal(t, uint64(3), stableOffset)
}

func TestReadCommittedDoesNotUseMarkerAtHWM(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetLatestOffset").Return(uint64(0)).Once()
	storage.On("GetFlushedOffset").Return(uint64(2)).Twice()
	storage.On("GetFirstOffset").Return(uint64(0)).Twice()
	storage.On("ReadMessages", uint64(0), 2).Return([]types.Message{
		{Offset: 0, Payload: "transactional", TransactionalID: "tx", TransactionState: types.TransactionStateOpen},
		{Offset: 1, Payload: "marker", TransactionalID: "tx", TransactionMarker: types.TransactionMarkerCommit},
	}, nil).Once()
	partition := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	partition.indexTransactionMessage(types.Message{Offset: 0, TransactionalID: "tx", TransactionState: types.TransactionStateOpen})
	partition.indexTransactionMessage(types.Message{Offset: 1, TransactionalID: "tx", TransactionMarker: types.TransactionMarkerCommit})

	partition.SetHWM(1)
	// The unresolved transaction stops the cursor before any log scan.
	require.Zero(t, firstUnresolvedOpenOffset(1, 0, partition.txnOpenOffsets, partition.txnMarkers, nil))
	messages, err := partition.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Empty(t, messages)

	partition.SetHWM(2)
	messages, err = partition.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "transactional", messages[0].Payload)
	storage.AssertExpectations(t)
}

func TestTransactionVisibilityPreservesLaterOpenAcrossResolverChange(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetLatestOffset").Return(uint64(0)).Once()
	p := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	p.SetTransactionDecisionResolver(&testCoordinatorEpochDecisionResolver{state: types.TransactionStateCommitted, coordinatorEpoch: 8, known: true})
	p.indexTransactionMessage(types.Message{Offset: 0, TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen})
	p.indexTransactionMessage(types.Message{Offset: 2, TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit, ControlBatchCoordinatorEpoch: 7})
	p.indexTransactionMessage(types.Message{Offset: 5, TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen})
	require.Zero(t, firstUnresolvedOpenOffset(6, 0, p.txnOpenOffsets, p.txnMarkers, p.txnResolver))
	p.SetTransactionDecisionResolver(&testCoordinatorEpochDecisionResolver{state: types.TransactionStateCommitted, coordinatorEpoch: 7, known: true})
	require.Equal(t, uint64(5), firstUnresolvedOpenOffset(6, 0, p.txnOpenOffsets, p.txnMarkers, p.txnResolver))
	storage.AssertExpectations(t)
}

type visibilityBenchmarkStorage struct {
	messages []types.Message
}

func (s *visibilityBenchmarkStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	if offset >= uint64(len(s.messages)) || max <= 0 {
		return nil, nil
	}
	start, ok := util.SafeUint64ToInt(offset)
	if !ok {
		return nil, nil
	}
	end := start + max
	if end > len(s.messages) {
		end = len(s.messages)
	}
	return s.messages[start:end], nil
}
func (*visibilityBenchmarkStorage) GetFirstOffset() uint64       { return 0 }
func (s *visibilityBenchmarkStorage) GetAbsoluteOffset() uint64  { return uint64(len(s.messages)) }
func (s *visibilityBenchmarkStorage) GetFlushedOffset() uint64   { return uint64(len(s.messages)) }
func (s *visibilityBenchmarkStorage) GetLatestOffset() uint64    { return uint64(len(s.messages)) }
func (*visibilityBenchmarkStorage) GetSegmentPath(uint64) string { return "" }
func (*visibilityBenchmarkStorage) AppendMessage(string, int, *types.Message) (uint64, error) {
	return 0, nil
}
func (*visibilityBenchmarkStorage) AppendMessageSync(string, int, *types.Message) (uint64, error) {
	return 0, nil
}
func (*visibilityBenchmarkStorage) AppendMessageWithOffset(string, int, *types.Message) error {
	return nil
}
func (*visibilityBenchmarkStorage) WriteBatch([]types.DiskMessage) error { return nil }
func (*visibilityBenchmarkStorage) TruncateTo(uint64) error              { return nil }
func (*visibilityBenchmarkStorage) Flush()                               {}
func (*visibilityBenchmarkStorage) Close() error                         { return nil }

func BenchmarkPartitionReadCommittedLargeVisibilityIndex(b *testing.B) {
	storage := &visibilityBenchmarkStorage{messages: []types.Message{{Offset: 0, Payload: "plain"}}}
	partition := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	partition.SetHWM(1)
	for index := 0; index < 10_000; index++ {
		offset := uint64(index + 100)
		id := strconv.Itoa(index)
		partition.indexTransactionMessage(types.Message{Offset: offset, TransactionalID: id, TransactionState: types.TransactionStateOpen})
		partition.indexTransactionMessage(types.Message{Offset: offset + 1, TransactionalID: id, TransactionMarker: types.TransactionMarkerCommit})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		messages, err := partition.ReadCommitted(0, 1)
		if err != nil || len(messages) != 1 {
			b.Fatalf("ReadCommitted = %d messages, %v", len(messages), err)
		}
	}
}

func TestReadCommittedPagePreservesFilteredScanPosition(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetLatestOffset").Return(uint64(0)).Once()
	storage.On("GetFlushedOffset").Return(uint64(3))
	storage.On("GetFirstOffset").Return(uint64(0))
	records := []types.Message{
		{Offset: 0, Payload: "aborted", TransactionalID: "tx", TransactionState: types.TransactionStateAborted},
		{Offset: 1, TransactionalID: "tx", TransactionMarker: types.TransactionMarkerAbort},
		{Offset: 2, Payload: "visible"},
	}
	for i := range records {
		storage.On("ReadMessages", uint64(i), 1).Return(records[i:i+1], nil)
	}
	partition := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	partition.SetHWM(3)
	offset := uint64(0)
	for i := 0; i < 3; i++ {
		messages, _, next, err := partition.ReadCommittedPage(offset, 1, 1, true)
		require.NoError(t, err)
		require.Greater(t, next, offset)
		if i < 2 {
			require.Empty(t, messages)
		} else {
			require.Len(t, messages, 1)
			require.Equal(t, "visible", messages[0].Payload)
		}
		offset = next
	}
}

type countingVisibilityStorage struct {
	visibilityBenchmarkStorage
	reads, decoded int
}

func (s *countingVisibilityStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	records, err := s.visibilityBenchmarkStorage.ReadMessages(offset, max)
	s.reads++
	s.decoded += len(records)
	return records, err
}

func TestReadCommittedPageBoundsRepeatedDecodeWork(t *testing.T) {
	storage := &countingVisibilityStorage{}
	for i := range 1600 {
		storage.messages = append(storage.messages, types.Message{Offset: uint64(i), Payload: "visible"})
	}
	p := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	p.SetHWM(1600)
	var offset uint64
	for range 200 {
		messages, bytes, next, err := p.ReadCommittedPage(offset, 8, 1<<20, true)
		require.NoError(t, err)
		require.Len(t, messages, 8)
		require.Positive(t, bytes)
		require.Equal(t, offset+8, next)
		offset = next
	}
	require.Equal(t, 200, storage.reads)
	require.Equal(t, 1600, storage.decoded, "visible records must not be decoded repeatedly for later pages")
}

func TestReadCommittedPageGrowsAcrossFilteredPrefix(t *testing.T) {
	storage := &countingVisibilityStorage{}
	for i := range 4096 {
		storage.messages = append(storage.messages, types.Message{Offset: uint64(i), Payload: "aborted", TransactionalID: "tx", TransactionState: types.TransactionStateAborted})
	}
	storage.messages = append(storage.messages, types.Message{Offset: 4096, Payload: "visible"})
	p := NewPartition(0, "orders", storage, nil, config.DefaultConfig())
	p.SetHWM(4097)
	messages, _, next, err := p.ReadCommittedPage(0, 1, 1<<20, true)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "visible", messages[0].Payload)
	require.Equal(t, uint64(4097), next)
	require.LessOrEqual(t, storage.reads, 14, "filtered runs must grow to the existing scan cap")
	require.Equal(t, 4097, storage.decoded)
}
