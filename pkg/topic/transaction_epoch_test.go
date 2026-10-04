package topic

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRetainedTransactionProducerEpochIncludesAbortedAndCompactedState(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetFirstOffset").Return(uint64(40))
	storage.On("GetAbsoluteOffset").Return(uint64(44))
	storage.On("ReadMessages", uint64(40), 1024).Return([]types.Message{
		{Offset: 40, TransactionalID: "open", Epoch: 7, TransactionState: types.TransactionStateOpen},
	}, nil).Once()
	// Short batches are not proof of EOF; scan until the durable tail.
	storage.On("ReadMessages", uint64(41), 1024).Return([]types.Message{
		{Offset: 42, TransactionalID: "aborted", Epoch: 13, TransactionMarker: types.TransactionMarkerAbort},
		{Offset: 43, Payload: "ordinary"},
	}, nil).Once()
	p := &Partition{dh: storage, producerStateIndex: map[string]producerStateCheckpointEntry{"compacted": {Epoch: 29, Seq: 4, Offset: 41}}}
	tm := &TopicManager{topics: map[string]*Topic{"input": {Partitions: []*Partition{p}}}}
	next, err := tm.RetainedTransactionProducerEpoch()
	require.NoError(t, err)
	require.Equal(t, uint64(30), next)
	storage.AssertExpectations(t)
}

func TestRetainedTransactionProducerEpochRejectsUnreadableHistory(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetFirstOffset").Return(uint64(0))
	storage.On("GetAbsoluteOffset").Return(uint64(2))
	storage.On("ReadMessages", uint64(0), 1024).Return([]types.Message{
		{Offset: 0, TransactionalID: "known", Epoch: 7},
	}, nil).Once()
	storage.On("ReadMessages", uint64(1), 1024).Return(nil, errors.New("injected disk read failure")).Once()
	p := &Partition{dh: storage}
	tm := &TopicManager{topics: map[string]*Topic{"input": {Partitions: []*Partition{p}}}}
	next, err := tm.RetainedTransactionProducerEpoch()
	require.ErrorContains(t, err, "injected disk read failure")
	require.Zero(t, next, "partial recovery must not initialize a usable allocator")
	storage.AssertExpectations(t)
}
