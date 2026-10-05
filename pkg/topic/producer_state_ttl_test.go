package topic

import (
	"os"
	"path/filepath"
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

	require.NoError(t, partition.persistProducerStateCheckpoint())
	data, err := os.ReadFile(partition.producerStatePath)
	require.NoError(t, err)
	checkpoint, err := decodeProducerStateCheckpoint(data)
	require.NoError(t, err)
	require.Equal(t, producerStateCheckpointEntry{Epoch: first.Epoch, Seq: second.SeqNum, Offset: 1}, checkpoint.Producers[first.ProducerID])
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

func TestProducerStateIndexPrunesEntriesBeforeRetentionFloor(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()

	storage, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	partition := NewPartition(0, "orders", storage, nil, cfg)
	t.Cleanup(func() {
		partition.Close()
		require.NoError(t, storage.Close())
	})

	require.NoError(t, partition.EnqueueSyncIdempotent(types.Message{
		Payload: "expired", ProducerID: "producer-expired", Epoch: 1, SeqNum: 1,
	}))
	require.NoError(t, partition.EnqueueSyncIdempotent(types.Message{
		Payload: "retained", ProducerID: "producer-retained", Epoch: 2, SeqNum: 1,
	}))
	partition.dh = &retentionBoundaryStorage{StorageHandler: storage, first: 1, tail: 2}

	partition.cleanStaleProducers()
	_, expiredPresent := partition.lookupProducerState("producer-expired")
	retained, retainedPresent := partition.lookupProducerState("producer-retained")
	require.False(t, expiredPresent)
	require.True(t, retainedPresent)
	require.Equal(t, uint64(1), retained.Offset)
	require.NoError(t, partition.persistProducerStateCheckpoint())

	data, err := os.ReadFile(partition.producerStatePath)
	require.NoError(t, err)
	checkpoint, err := decodeProducerStateCheckpoint(data)
	require.NoError(t, err)
	require.NotContains(t, checkpoint.Producers, "producer-expired")
	require.Equal(t, retained, checkpoint.Producers["producer-retained"])
}

func TestLoadProducerStateCheckpointUsesRetainedOffsetRange(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "partition_0.producers")
	require.NoError(t, os.WriteFile(checkpointPath, []byte(`{
  "version": 4,
  "covered_offset": 10,
  "producers": {
    "before-floor": {"epoch": 1, "seq": 1, "offset": 4},
    "retained": {"epoch": 2, "seq": 3, "offset": 5},
    "past-tail": {"epoch": 3, "seq": 4, "offset": 10}
  }
}`), 0o600))

	partition := &Partition{
		dh:                 &retentionBoundaryStorage{first: 5, tail: 10},
		producerStatePath:  checkpointPath,
		producerStateIndex: make(map[string]producerStateCheckpointEntry),
	}
	partition.loadProducerStateCheckpoint()

	_, beforeFloorPresent := partition.lookupProducerState("before-floor")
	retained, retainedPresent := partition.lookupProducerState("retained")
	_, pastTailPresent := partition.lookupProducerState("past-tail")
	require.False(t, beforeFloorPresent)
	require.True(t, retainedPresent)
	require.Equal(t, producerStateCheckpointEntry{Epoch: 2, Seq: 3, Offset: 5}, retained)
	require.False(t, pastTailPresent)
}

type readCountingStorage struct {
	types.StorageHandler
	readCalls int
}

type retentionBoundaryStorage struct {
	types.StorageHandler
	first uint64
	tail  uint64
}

func (s *retentionBoundaryStorage) GetFirstOffset() uint64 {
	return s.first
}

func (s *retentionBoundaryStorage) GetAbsoluteOffset() uint64 {
	return s.tail
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
