package topic

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPartition_RestoresPersistedHWMCheckpoint(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "committed"}))
	require.Equal(t, uint64(1), p.GetHWM())

	leaderBatch := []types.Message{{Payload: "uncommitted"}}
	require.NoError(t, p.EnqueueBatchLeader(leaderBatch))
	p.FlushDisk()
	require.Equal(t, uint64(1), p.GetHWM(), "leader append must not advance HWM before replication commit")
	p.Close()
	require.NoError(t, dh.Close())

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = restartedDH.Close() }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)

	require.Equal(t, uint64(1), restarted.GetHWM())
	msgs, err := restarted.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "committed", msgs[0].Payload)
}

func TestPartition_ReplacesPersistedHWMCheckpoint(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "first"}))
	p.FlushDisk()
	require.NoError(t, p.EnqueueSync(types.Message{Payload: "second"}))
	p.FlushDisk()
	p.Close()
	require.NoError(t, dh.Close())

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = restartedDH.Close() }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)
	defer restarted.Close()

	require.Equal(t, uint64(2), restarted.GetHWM())
}

func TestPartition_RestoresProducerStateCheckpoint(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "first", ProducerID: "producer-1", SeqNum: 1}))
	p.FlushDisk()
	p.Close()
	require.NoError(t, dh.Close())

	restartedDH, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = restartedDH.Close() }()
	restarted := NewPartition(0, "orders", restartedDH, nil, cfg)
	defer restarted.Close()
	restarted.isIdempotent = true

	require.NoError(t, restarted.EnqueueSync(types.Message{Payload: "duplicate", ProducerID: "producer-1", SeqNum: 1}))
	msgs, err := restarted.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "first", msgs[0].Payload)

	require.NoError(t, restarted.EnqueueSync(types.Message{Payload: "second", ProducerID: "producer-1", SeqNum: 2}))
	msgs, err = restarted.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	require.Equal(t, "second", msgs[1].Payload)
}

func TestPartition_ProducerEpochFencing(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 1

	dh, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = dh.Close() }()
	p := NewPartition(0, "orders", dh, nil, cfg)
	defer p.Close()
	p.isIdempotent = true

	require.NoError(t, p.EnqueueSync(types.Message{Payload: "epoch-10", ProducerID: "producer-1", Epoch: 10, SeqNum: 1}))
	require.NoError(t, p.EnqueueSync(types.Message{Payload: "epoch-11", ProducerID: "producer-1", Epoch: 11, SeqNum: 1}))
	require.Error(t, p.EnqueueSync(types.Message{Payload: "stale-epoch", ProducerID: "producer-1", Epoch: 10, SeqNum: 2}))

	msgs, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	require.Equal(t, "epoch-10", msgs[0].Payload)
	require.Equal(t, "epoch-11", msgs[1].Payload)
}
func TestPartition_EnqueueBatchLeaderUsesSingleBatchWrite(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("WriteBatch", mock.Anything).Return(nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	batch := []types.Message{
		{Payload: "one", ProducerID: "producer-1", SeqNum: 1},
		{Payload: "two", ProducerID: "producer-1", SeqNum: 2},
	}
	require.NoError(t, p.EnqueueBatchLeader(batch))

	require.Equal(t, uint64(1), batch[0].Offset)
	require.Equal(t, uint64(2), batch[1].Offset)
	require.Equal(t, uint64(3), p.NextOffset())
	require.Equal(t, uint64(1), p.GetHWM(), "leader append must not advance HWM before replication commit")

	diskBatch := dh.Calls[1].Arguments.Get(0).([]types.DiskMessage)
	require.Len(t, diskBatch, 2)
	require.Equal(t, uint64(1), diskBatch[0].Offset)
	require.Equal(t, uint64(2), diskBatch[1].Offset)
	dh.AssertNotCalled(t, "AppendMessage", mock.Anything, mock.Anything, mock.Anything)
}

func TestPartition_EnqueueBatchLeaderDoesNotAdvanceStateOnWriteFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	dh := new(MockStorageHandler)
	dh.On("GetLatestOffset").Return(uint64(0)).Once()
	dh.On("WriteBatch", mock.Anything).Return(errors.New("disk unavailable")).Once()
	dh.On("WriteBatch", mock.Anything).Return(nil).Once()

	p := NewPartition(0, "orders", dh, nil, cfg)
	p.isIdempotent = true

	failedBatch := []types.Message{{Payload: "one", ProducerID: "producer-1", SeqNum: 1}}
	require.Error(t, p.EnqueueBatchLeader(failedBatch))
	require.Equal(t, uint64(0), failedBatch[0].Offset)
	require.Equal(t, uint64(1), p.NextOffset())

	retryBatch := []types.Message{{Payload: "one", ProducerID: "producer-1", SeqNum: 1}}
	require.NoError(t, p.EnqueueBatchLeader(retryBatch))
	require.Equal(t, uint64(1), retryBatch[0].Offset)
	require.Equal(t, uint64(2), p.NextOffset())
}
