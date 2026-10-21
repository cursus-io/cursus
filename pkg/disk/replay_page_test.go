package disk

import (
	"os"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestReplayPagesResumePhysicalPositionWithoutIndexLookups(t *testing.T) {
	cfg := compactionTestConfig(t)
	cfg.IndexIntervalBytes = 1 << 30
	cfg.SegmentSize = 1 << 20
	handler, err := NewDiskHandler(cfg, "metadata", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()
	for range 200 {
		_, err := handler.AppendMessageSync("metadata", 0, &types.Message{Payload: strings.Repeat("x", 1000)})
		require.NoError(t, err)
	}
	// A missing derived index must neither trigger a prefix rescan nor prevent
	// progress. Restore the mapper before Close so the original lease is freed.
	handler.indexMu.Lock()
	mapper := handler.indexMapper
	handler.indexMapper = nil
	handler.indexMu.Unlock()
	defer func() {
		handler.indexMu.Lock()
		handler.indexMapper = mapper
		handler.indexMu.Unlock()
	}()
	var cursor types.ReplayCursor
	var next uint64
	records, bytes := 0, 0
	for range 100 {
		page, err := handler.ReadReplayPage(next, 200, cursor, 7, 4000)
		require.NoError(t, err)
		require.LessOrEqual(t, page.Records, 7)
		require.LessOrEqual(t, page.Bytes, 4000)
		require.Greater(t, page.Next, next)
		next, cursor = page.Next, page.Cursor
		records += page.Records
		bytes += page.Bytes
		if next == 200 {
			break
		}
	}
	require.Equal(t, uint64(200), next)
	require.Equal(t, 200, records, "every record must be decoded exactly once")
	stat, err := os.Stat(handler.GetSegmentPath(0))
	require.NoError(t, err)
	require.Equal(t, int(stat.Size())-4*records, bytes, "account for all serialized record bytes, excluding length headers")
}

func TestReplayPagesRejectOversizedAndTruncatedCommittedRecords(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "metadata", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()
	_, err = handler.AppendMessageSync("metadata", 0, &types.Message{Payload: strings.Repeat("x", 1000)})
	require.NoError(t, err)
	page, err := handler.ReadReplayPage(0, 1, types.ReplayCursor{}, 10, 100)
	require.ErrorContains(t, err, "byte budget")
	require.Zero(t, page.Bytes)
	require.Zero(t, page.Records)
	path := handler.GetSegmentPath(handler.CurrentSegment)
	stat, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(path, stat.Size()-1))
	_, err = handler.ReadReplayPage(0, 1, types.ReplayCursor{}, 10, 4096)
	require.ErrorContains(t, err, "truncated committed")
}

func TestReplayPagesDoNotSkipNewlyCommittedRecordAfterCompactedGap(t *testing.T) {
	cfg := compactionTestConfig(t)
	handler, err := NewDiskHandler(cfg, "metadata", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, handler.Close()) }()
	require.NoError(t, handler.WriteCompactedReplicaRange(0, 10, []types.DiskMessage{
		{Topic: "metadata", Offset: 3, Payload: "before"},
		{Topic: "metadata", Offset: 8, Payload: "later"},
	}))
	page, err := handler.ReadReplayPage(0, 5, types.ReplayCursor{}, 10, 4096)
	require.NoError(t, err)
	require.Equal(t, uint64(5), page.Next)
	require.Len(t, page.Messages, 1)
	page, err = handler.ReadReplayPage(5, 10, page.Cursor, 10, 4096)
	require.NoError(t, err)
	require.Len(t, page.Messages, 1)
	require.Equal(t, uint64(8), page.Messages[0].Offset)
}
