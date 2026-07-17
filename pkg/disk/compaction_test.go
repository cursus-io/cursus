package disk

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func compactionTestConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.CleanupPolicy = config.CleanupPolicyCompact
	cfg.SegmentSize = 1 << 20
	cfg.IndexSize = 1 << 20
	cfg.IndexIntervalBytes = 64
	cfg.DiskFlushBatchSize = 1
	cfg.DiskFlushIntervalMS = 10
	cfg.DiskWriteTimeoutMS = 100
	cfg.LingerMS = 1
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000
	cfg.MinCleanableDirtyRatio = 0.01
	return cfg
}

func appendCompactionMessage(t *testing.T, handler *DiskHandler, message types.Message) uint64 {
	t.Helper()
	offset, err := handler.AppendMessageSync("orders", 0, &message)
	require.NoError(t, err)
	return offset
}

func rollCompactionSegment(t *testing.T, handler *DiskHandler) {
	t.Helper()
	handler.mu.Lock()
	handler.ioMu.Lock()
	err := handler.rotateSegment(handler.GetAbsoluteOffset())
	handler.ioMu.Unlock()
	handler.mu.Unlock()
	require.NoError(t, err)
}

func messageOffsets(messages []types.Message) []uint64 {
	offsets := make([]uint64, len(messages))
	for i, message := range messages {
		offsets[i] = message.Offset
	}
	return offsets
}

func TestEnforceCompactionPreservesLogicalOffsetsAndProtectedRecords(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)

	require.Equal(t, uint64(0), appendCompactionMessage(t, handler, types.Message{Key: "account", Payload: "old", ProducerID: "p1", SeqNum: 1}))
	require.Equal(t, uint64(1), appendCompactionMessage(t, handler, types.Message{Key: "region", Payload: "east", ProducerID: "p1", SeqNum: 2}))
	require.Equal(t, uint64(2), appendCompactionMessage(t, handler, types.Message{Key: "account", Payload: "current", ProducerID: "p2", SeqNum: 1}))
	require.Equal(t, uint64(3), appendCompactionMessage(t, handler, types.Message{Payload: "unkeyed", ProducerID: "p3", SeqNum: 1}))
	require.Equal(t, uint64(4), appendCompactionMessage(t, handler, types.Message{
		Key:              "account",
		Payload:          "transactional",
		ProducerID:       "tx-producer",
		SeqNum:           1,
		TransactionalID:  "tx-1",
		TransactionState: types.TransactionStateCommitted,
	}))
	require.Equal(t, uint64(5), appendCompactionMessage(t, handler, types.Message{Key: "producer-anchor", Payload: "anchor", ProducerID: "p1", SeqNum: 3}))
	rollCompactionSegment(t, handler)

	require.Equal(t, uint64(6), appendCompactionMessage(t, handler, types.Message{Key: "account", Payload: "active-old", ProducerID: "p4", SeqNum: 1}))
	require.Equal(t, uint64(7), appendCompactionMessage(t, handler, types.Message{Key: "account", Payload: "active-current", ProducerID: "p4", SeqNum: 2}))
	activePath := handler.GetSegmentPath(handler.GetCurrentSegment())
	activeBefore, err := os.ReadFile(activePath)
	require.NoError(t, err)

	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.SegmentsRewritten)
	require.Equal(t, 1, result.RecordsRemoved)
	require.Empty(t, result.SkippedReason)
	require.Equal(t, uint64(8), handler.GetAbsoluteOffset())

	closedSegment := handler.segments[0]
	closedInfo, err := os.Stat(handler.GetSegmentPath(closedSegment))
	require.NoError(t, err)
	require.FileExists(t, compactionMarkerPath(handler.GetSegmentPath(closedSegment), closedInfo.Size()))

	activeAfter, err := os.ReadFile(activePath)
	require.NoError(t, err)
	require.Equal(t, activeBefore, activeAfter, "active segment must never be rewritten")

	messages, err := handler.ReadMessages(0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7}, messageOffsets(messages))
	require.Equal(t, "transactional", messages[3].Payload)
	require.Equal(t, "tx-1", messages[3].TransactionalID)
	require.Equal(t, "unkeyed", messages[2].Payload)
	require.NoError(t, handler.Close())

	reopened, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.Equal(t, uint64(8), reopened.GetAbsoluteOffset())
	reopenedInfo, err := os.Stat(reopened.GetSegmentPath(closedSegment))
	require.NoError(t, err)
	require.True(t, reopened.segmentAllowsOffsetGaps(closedSegment, reopenedInfo.Size()))
	messages, err = reopened.ReadMessages(0, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7}, messageOffsets(messages))
}

func TestEnforceCompactionWaitsForActiveUpdateToRoll(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()

	appendCompactionMessage(t, handler, types.Message{Key: "account", Payload: "old"})
	rollCompactionSegment(t, handler)
	appendCompactionMessage(t, handler, types.Message{Key: "account", Payload: "new"})

	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, "no_superseded_records", result.SkippedReason)
	require.Zero(t, result.RecordsRemoved)

	messages, err := handler.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, messageOffsets(messages))

	rollCompactionSegment(t, handler)
	result, err = handler.EnforceCompaction()
	require.NoError(t, err)
	require.Empty(t, result.SkippedReason)
	require.Equal(t, 1, result.RecordsRemoved)

	messages, err = handler.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, messageOffsets(messages))
}

func TestEnforceCompactionHonorsDirtyRatio(t *testing.T) {
	cfg := compactionTestConfig(t)
	cfg.MinCleanableDirtyRatio = 0.9
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()

	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "old"})
	for i := 0; i < 8; i++ {
		appendCompactionMessage(t, handler, types.Message{Key: string(rune('a' + i)), Payload: "retained"})
	}
	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "new"})
	rollCompactionSegment(t, handler)

	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, "dirty_ratio", result.SkippedReason)
	require.Zero(t, result.RecordsRemoved)

	messages, err := handler.ReadMessages(0, 100)
	require.NoError(t, err)
	require.Len(t, messages, 10)
}

func TestNewDiskHandlerRemovesStaleCompactionTemps(t *testing.T) {
	cfg := compactionTestConfig(t)
	dir := filepath.Join(cfg.LogDir, "orders")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	staleLog := filepath.Join(dir, "partition_0_segment_00000000000000000000.log.compacting")
	staleIndex := filepath.Join(dir, "partition_0_segment_00000000000000000000.index.compacting")
	staleMarker := filepath.Join(dir, "partition_0_segment_00000000000000000000.log.compacted-5")
	require.NoError(t, os.WriteFile(staleLog, []byte("stale"), 0o600))
	require.NoError(t, os.WriteFile(staleIndex, []byte("stale"), 0o600))
	require.NoError(t, os.WriteFile(staleMarker, []byte(compactionMarkerVersion), 0o600))

	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()
	_, err = os.Stat(staleLog)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(staleIndex)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(staleMarker)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCombinedCleanupPolicyEnablesCompaction(t *testing.T) {
	cfg := compactionTestConfig(t)
	cfg.CleanupPolicy = config.CleanupPolicyDeleteCompact
	cfg.RetentionHours = int((24 * time.Hour) / time.Hour)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()

	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "old"})
	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "new"})
	rollCompactionSegment(t, handler)

	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)
}

func TestEnforceCompactionLeavesStaleIndexCorrectnessSafe(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()

	for i := 0; i < 12; i++ {
		appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: string(rune('a' + i))})
	}
	appendCompactionMessage(t, handler, types.Message{Payload: "unkeyed"})
	rollCompactionSegment(t, handler)

	closedSegment := handler.segments[0]
	indexPath := handler.GetIndexPath(closedSegment)
	staleIndex, err := os.ReadFile(indexPath)
	require.NoError(t, err)

	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 11, result.RecordsRemoved)

	require.NoError(t, os.Chmod(indexPath, 0o600))
	require.NoError(t, os.WriteFile(indexPath, staleIndex, 0o600))
	messages, err := handler.ReadMessages(11, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{11, 12}, messageOffsets(messages))
}

func TestReadMessagesRejectsUnmarkedClosedOffsetHole(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()

	appendCompactionMessage(t, handler, types.Message{Key: "a", Payload: "zero"})
	appendCompactionMessage(t, handler, types.Message{Key: "b", Payload: "one"})
	appendCompactionMessage(t, handler, types.Message{Key: "c", Payload: "two"})
	rollCompactionSegment(t, handler)

	closedSegment := handler.segments[0]
	logPath := handler.GetSegmentPath(closedSegment)
	var retained []byte
	err = scanCompactionSegment(logPath, closedSegment, true, false, func(frame compactionFrame) error {
		if frame.message.Offset != 1 {
			retained = append(retained, frame.raw...)
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, os.Chmod(logPath, 0o600))
	require.NoError(t, os.WriteFile(logPath, retained, 0o600))

	_, err = handler.ReadMessages(0, 10)
	require.ErrorContains(t, err, "non-contiguous offset")
}

func TestRepeatedCompactionKeepsPriorMarkerUntilRestartCleanup(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)

	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "old"})
	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "current"})
	rollCompactionSegment(t, handler)
	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)

	firstSegment := handler.segments[0]
	firstLogPath := handler.GetSegmentPath(firstSegment)
	firstInfo, err := os.Stat(firstLogPath)
	require.NoError(t, err)
	firstMarker := compactionMarkerPath(firstLogPath, firstInfo.Size())
	require.FileExists(t, firstMarker)

	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "newest"})
	rollCompactionSegment(t, handler)
	result, err = handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)
	require.FileExists(t, firstMarker)

	markers, err := filepath.Glob(firstLogPath + ".compacted-*")
	require.NoError(t, err)
	require.Len(t, markers, 2)
	require.NoError(t, handler.Close())

	reopened, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	markers, err = filepath.Glob(firstLogPath + ".compacted-*")
	require.NoError(t, err)
	require.Len(t, markers, 1)

	messages, err := reopened.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, messageOffsets(messages))
}

func TestNewDiskHandlerRejectsCorruptCompactionMarker(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)

	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "old"})
	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "new"})
	rollCompactionSegment(t, handler)
	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)

	closedSegment := handler.segments[0]
	info, err := os.Stat(handler.GetSegmentPath(closedSegment))
	require.NoError(t, err)
	markerPath := compactionMarkerPath(handler.GetSegmentPath(closedSegment), info.Size())
	require.NoError(t, handler.Close())
	require.NoError(t, os.WriteFile(markerPath, []byte("corrupt"), 0o600))

	_, err = NewDiskHandler(cfg, "orders", 0)
	require.ErrorContains(t, err, "invalid compaction marker")
}

func TestRetentionRemovesCompactionMarker(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()

	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "old"})
	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "new"})
	rollCompactionSegment(t, handler)
	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)

	closedSegment := handler.segments[0]
	logPath := handler.GetSegmentPath(closedSegment)
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	markerPath := compactionMarkerPath(logPath, info.Size())
	require.FileExists(t, markerPath)

	require.NoError(t, handler.markAsDeleted(logPath))
	require.NoFileExists(t, markerPath)
	require.False(t, handler.segmentAllowsOffsetGaps(closedSegment, info.Size()))
}

func TestTruncateRejectsCompactedTargetSegment(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()

	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "old"})
	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "new"})
	rollCompactionSegment(t, handler)
	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)

	err = handler.TruncateTo(1)
	require.ErrorContains(t, err, "cannot truncate compacted segment")
}

func TestEnforceCompactionSkipsActiveReader(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()

	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "old"})
	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "new"})
	rollCompactionSegment(t, handler)

	session, err := handler.OpenForRead(0)
	require.NoError(t, err)
	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, "active_readers", result.SkippedReason)
	require.NoError(t, session.Close())
	require.NoError(t, session.Close())
	require.Zero(t, handler.GetActiveReaders())

	result, err = handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)
}

func TestCompactionCanLeaveAnEmptyClosedSegment(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)

	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "old"})
	rollCompactionSegment(t, handler)
	appendCompactionMessage(t, handler, types.Message{Key: "key", Payload: "current"})
	rollCompactionSegment(t, handler)

	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)

	emptySegment := handler.segments[0]
	info, err := os.Stat(handler.GetSegmentPath(emptySegment))
	require.NoError(t, err)
	require.Zero(t, info.Size())
	require.FileExists(t, compactionMarkerPath(handler.GetSegmentPath(emptySegment), 0))

	messages, err := handler.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, messageOffsets(messages))
	require.NoError(t, handler.Close())

	reopened, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	messages, err = reopened.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, messageOffsets(messages))
}

func TestDiskManagerPolicyCanReturnToBrokerRetentionDefaults(t *testing.T) {
	cfg := compactionTestConfig(t)
	cfg.RetentionHours = 72
	cfg.RetentionBytes = 4096
	manager := NewDiskManager(cfg)
	defer manager.CloseAllHandlers()

	value, err := manager.GetHandlerWithPolicy("state", 0, config.CleanupPolicyCompact, 24, 1024)
	require.NoError(t, err)
	handler := value.(*DiskHandler)
	hours, bytes := handler.RetentionPolicy()
	require.Equal(t, 24, hours)
	require.Equal(t, int64(1024), bytes)

	value, err = manager.GetHandlerWithPolicy("state", 0, config.CleanupPolicyDelete, 0, 0)
	require.NoError(t, err)
	handler = value.(*DiskHandler)
	hours, bytes = handler.RetentionPolicy()
	require.Equal(t, cfg.RetentionHours, hours)
	require.Equal(t, cfg.RetentionBytes, bytes)
	require.Equal(t, config.CleanupPolicyDelete, handler.CleanupPolicy())
}

func TestStoragePolicyUpdateWaitsForMaintenance(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()

	handler.maintenanceMu.Lock()
	updated := make(chan struct{})
	go func() {
		handler.SetStoragePolicy(config.CleanupPolicyDelete, 24, 1024)
		close(updated)
	}()

	select {
	case <-updated:
		handler.maintenanceMu.Unlock()
		t.Fatal("storage policy update completed during active maintenance")
	case <-time.After(50 * time.Millisecond):
	}
	handler.maintenanceMu.Unlock()

	select {
	case <-updated:
	case <-time.After(time.Second):
		t.Fatal("storage policy update did not complete after maintenance")
	}
	require.Equal(t, config.CleanupPolicyDelete, handler.CleanupPolicy())
	hours, bytes := handler.RetentionPolicy()
	require.Equal(t, 24, hours)
	require.Equal(t, int64(1024), bytes)
}
