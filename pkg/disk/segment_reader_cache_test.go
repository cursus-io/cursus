package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSegmentReaderCacheIsBoundedAndEvictsIdleLRU(t *testing.T) {
	cache := newSegmentReaderCache(2)
	directory := t.TempDir()

	for base := uint64(0); base < 3; base++ {
		path := filepath.Join(directory, fmt.Sprintf("segment-%d.log", base))
		require.NoError(t, os.WriteFile(path, []byte{byte(base)}, 0o600))
		lease, err := cache.acquire(base, path)
		require.NoError(t, err)
		require.NoError(t, lease.Close())
	}

	stats := cache.stats()
	require.Equal(t, 2, stats.Entries)
	require.Equal(t, uint64(3), stats.Misses)
	require.Equal(t, uint64(1), stats.Evictions)
	require.NoError(t, cache.close())
}

func TestSegmentReaderCacheDefersInvalidationWhileReferenced(t *testing.T) {
	cache := newSegmentReaderCache(1)
	path := filepath.Join(t.TempDir(), "segment.log")
	require.NoError(t, os.WriteFile(path, []byte("record"), 0o600))

	lease, err := cache.acquire(7, path)
	require.NoError(t, err)
	require.ErrorContains(t, cache.invalidate(7), "active cached reader")
	require.NoError(t, lease.Close())
	require.NoError(t, cache.invalidate(7))
	require.Zero(t, cache.stats().Entries)
	require.NoError(t, cache.close())
}

func TestReadMessagesReusesClosedSegmentMapping(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushBatchSize = 1
	cfg.DiskFlushIntervalMS = 10
	cfg.DiskWriteTimeoutMS = 100
	cfg.LingerMS = 1

	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })

	_, err = handler.AppendMessageSync("orders", 0, &types.Message{Payload: "closed"})
	require.NoError(t, err)
	handler.mu.Lock()
	handler.ioMu.Lock()
	err = handler.rotateSegment(handler.GetAbsoluteOffset())
	handler.ioMu.Unlock()
	handler.mu.Unlock()
	require.NoError(t, err)

	for range 2 {
		messages, readErr := handler.ReadMessages(0, 1)
		require.NoError(t, readErr)
		require.Len(t, messages, 1)
		require.Equal(t, "closed", messages[0].Payload)
	}

	stats := handler.segmentReaders.stats()
	require.Equal(t, 1, stats.Entries)
	require.Equal(t, uint64(1), stats.Misses)
	require.Equal(t, uint64(1), stats.Hits)
}

func TestRetentionRenameInvalidatesCachedSegmentMapping(t *testing.T) {
	handler := newHandlerWithCachedClosedSegment(t)
	closedBase := handler.segments[0]
	closedPath := handler.GetSegmentPath(closedBase)

	require.NoError(t, handler.markAsDeleted(closedPath))
	require.Zero(t, handler.segmentReaders.stats().Entries)
	require.NoFileExists(t, closedPath)
}

func TestTruncateInvalidatesAllCachedSegmentMappings(t *testing.T) {
	handler := newHandlerWithCachedClosedSegment(t)
	_, err := handler.AppendMessageSync("orders", 0, &types.Message{Payload: "active"})
	require.NoError(t, err)

	require.NoError(t, handler.TruncateTo(1))
	require.Zero(t, handler.segmentReaders.stats().Entries)
	require.Equal(t, uint64(1), handler.GetAbsoluteOffset())
}

func TestCompactionInvalidatesCachedSegmentMapping(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })

	appendCompactionMessage(t, handler, types.Message{Key: "account", Payload: "old", ProducerID: "p1", SeqNum: 1})
	appendCompactionMessage(t, handler, types.Message{Key: "account", Payload: "current", ProducerID: "p1", SeqNum: 2})
	rollCompactionSegment(t, handler)
	messages, err := handler.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, 1, handler.segmentReaders.stats().Entries)

	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.SegmentsRewritten)
	require.Zero(t, handler.segmentReaders.stats().Entries)
}

func newHandlerWithCachedClosedSegment(t *testing.T) *DiskHandler {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushBatchSize = 1
	cfg.DiskFlushIntervalMS = 10
	cfg.DiskWriteTimeoutMS = 100
	cfg.LingerMS = 1

	handler, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })
	_, err = handler.AppendMessageSync("orders", 0, &types.Message{Payload: "closed"})
	require.NoError(t, err)
	rollCompactionSegment(t, handler)
	messages, err := handler.ReadMessages(0, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, 1, handler.segmentReaders.stats().Entries)
	return handler
}
