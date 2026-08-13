package disk

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerMetadataIgnoresApplicationRetentionPolicy(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.SegmentSize = 256
	cfg.RetentionHours = 168
	cfg.RetentionBytes = -1

	application, err := NewDiskHandler(cfg, "events", 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, application.Close()) })
	internal, err := NewDiskHandler(cfg, config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, internal.Close()) })

	for index := 0; index < 8; index++ {
		message := &types.Message{Key: "key", Payload: strings.Repeat("x", 180)}
		_, err = application.AppendMessageSync("events", 0, message)
		require.NoError(t, err)
		message = &types.Message{Key: "key", Payload: strings.Repeat("x", 180)}
		_, err = internal.AppendMessageSync(config.ConsumerOffsetsTopicName, 0, message)
		require.NoError(t, err)
	}

	application.SetStoragePolicy(config.CleanupPolicyDelete, 168, -1)
	internal.SetStoragePolicy(config.CleanupPolicyDelete, 168, -1)
	require.Equal(t, config.CleanupPolicyCompact, internal.CleanupPolicy())
	hours, bytes := internal.RetentionPolicy()
	require.Zero(t, hours)
	require.Zero(t, bytes)

	ageClosedSegments(t, application)
	ageClosedSegments(t, internal)
	applicationBefore := segmentFileCount(t, application.BaseName)
	internalBefore := segmentFileCount(t, internal.BaseName)
	require.Greater(t, applicationBefore, 1)
	require.Greater(t, internalBefore, 1)

	application.EnforceRetention(cfg)
	internal.EnforceRetention(cfg)
	require.Less(t, segmentFileCount(t, application.BaseName), applicationBefore)
	require.Equal(t, internalBefore, segmentFileCount(t, internal.BaseName))
}

func TestConsumerMetadataIgnoresApplicationSizeRetentionPolicy(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.SegmentSize = 256
	cfg.RetentionHours = -1
	cfg.RetentionBytes = 1

	application, err := NewDiskHandler(cfg, "events", 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, application.Close()) })
	internal, err := NewDiskHandler(cfg, config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, internal.Close()) })
	for index := 0; index < 8; index++ {
		_, err = application.AppendMessageSync("events", 0, &types.Message{Key: "key", Payload: strings.Repeat("x", 180)})
		require.NoError(t, err)
		_, err = internal.AppendMessageSync(config.ConsumerOffsetsTopicName, 0, &types.Message{Key: "key", Payload: strings.Repeat("x", 180)})
		require.NoError(t, err)
	}

	application.SetStoragePolicy(config.CleanupPolicyDelete, -1, 1)
	internal.SetStoragePolicy(config.CleanupPolicyDelete, -1, 1)
	applicationBefore := segmentFileCount(t, application.BaseName)
	internalBefore := segmentFileCount(t, internal.BaseName)
	application.EnforceRetention(cfg)
	internal.EnforceRetention(cfg)
	require.Less(t, segmentFileCount(t, application.BaseName), applicationBefore)
	require.Equal(t, internalBefore, segmentFileCount(t, internal.BaseName))
}
func ageClosedSegments(t *testing.T, handler *DiskHandler) {
	t.Helper()
	paths, err := filepath.Glob(handler.BaseName + "_segment_*.log")
	require.NoError(t, err)
	old := time.Now().Add(-169 * time.Hour)
	for _, path := range paths {
		if path == handler.GetSegmentPath(handler.CurrentSegment) {
			continue
		}
		require.NoError(t, os.Chtimes(path, old, old))
	}
}

func segmentFileCount(t *testing.T, base string) int {
	t.Helper()
	paths, err := filepath.Glob(base + "_segment_*.log")
	require.NoError(t, err)
	return len(paths)
}
