package topic

import (
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestIdempotentProducerContinuesAfterInMemoryStateExpires(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.ProducerStateTTLMS = 1

	storage, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	partition := NewPartition(0, "orders", storage, nil, cfg)
	t.Cleanup(func() {
		partition.Close()
		require.NoError(t, storage.Close())
	})

	first := types.Message{Payload: "first", ProducerID: "producer-1", Epoch: 7, SeqNum: 1}
	require.NoError(t, partition.EnqueueSyncIdempotent(first))
	expireProducerState(t, partition, first.ProducerID)
	storageReads := &readCountingStorage{StorageHandler: storage}
	partition.dh = storageReads

	second := types.Message{Payload: "second", ProducerID: first.ProducerID, Epoch: first.Epoch, SeqNum: 2}
	require.NoError(t, partition.EnqueueSyncIdempotent(second))
	require.Equal(t, uint64(2), partition.NextOffset())

	expireProducerState(t, partition, first.ProducerID)
	require.NoError(t, partition.EnqueueSyncIdempotent(second), "a retry after cache expiry must remain idempotent")
	require.Equal(t, uint64(2), partition.NextOffset(), "the retried sequence was appended twice")

	expireProducerState(t, partition, first.ProducerID)
	err = partition.EnqueueSyncIdempotent(types.Message{
		Payload:    "stale",
		ProducerID: first.ProducerID,
		Epoch:      first.Epoch - 1,
		SeqNum:     1,
	})
	require.ErrorContains(t, err, "stale_producer_epoch")

	expireProducerState(t, partition, first.ProducerID)
	err = partition.EnqueueSyncIdempotent(types.Message{
		Payload:    "gap",
		ProducerID: first.ProducerID,
		Epoch:      first.Epoch,
		SeqNum:     4,
	})
	require.ErrorContains(t, err, "expected 3, got 4")
	require.Equal(t, uint64(2), partition.NextOffset(), "rejected messages changed the log")
	require.Zero(t, storageReads.readCalls, "producer validation must not scan the retained log")

	partition.persistProducerStateCheckpoint()
	data, err := os.ReadFile(partition.producerStatePath)
	require.NoError(t, err)
	checkpoint, err := decodeProducerStateCheckpoint(data)
	require.NoError(t, err)
	require.Equal(t, producerStateCheckpointEntry{Epoch: first.Epoch, Seq: second.SeqNum}, checkpoint.Producers[first.ProducerID])
}

func TestUnknownIdempotentProducerStillStartsAtSequenceOne(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()

	storage, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	partition := NewPartition(0, "orders", storage, nil, cfg)
	storageReads := &readCountingStorage{StorageHandler: storage}
	partition.dh = storageReads
	t.Cleanup(func() {
		partition.Close()
		require.NoError(t, storage.Close())
	})

	err = partition.EnqueueSyncIdempotent(types.Message{
		Payload:    "out-of-sequence",
		ProducerID: "never-seen",
		Epoch:      1,
		SeqNum:     2,
	})
	require.ErrorContains(t, err, "first message for producer never-seen must have seqNum 1")
	require.Equal(t, uint64(0), partition.NextOffset())
	require.Zero(t, storageReads.readCalls, "unknown producers must not trigger a retained-log scan")
}

type readCountingStorage struct {
	types.StorageHandler
	readCalls int
}

func (s *readCountingStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	s.readCalls++
	return s.StorageHandler.ReadMessages(offset, max)
}

func expireProducerState(t *testing.T, partition *Partition, producerID string) {
	t.Helper()
	partition.mu.Lock()
	entryValue, ok := partition.producerState.Load(producerID)
	require.True(t, ok)
	entryValue.(*producerEntry).lastSeen = time.Now().Add(-time.Hour)
	partition.mu.Unlock()

	partition.cleanStaleProducers()
	_, ok = partition.producerState.Load(producerID)
	require.False(t, ok)
}
