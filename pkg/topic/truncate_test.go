package topic

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestTruncateTopicDurableResetsRecordsOffsetsAndProducerState(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := NewTopicManager(cfg, dm, nil)
	t.Cleanup(manager.Stop)

	require.NoError(t, manager.CreateTopic("orders", 1, true, false))
	require.Equal(t, topicMetadataFormatVersion, readTopicManifestVersion(t, cfg.LogDir))
	require.NoError(t, manager.PublishToPartitionWithAck("orders", 0, &types.Message{
		Payload: "old", ProducerID: "producer-1", Epoch: 1, SeqNum: 1,
	}))
	old := manager.GetTopic("orders")
	require.Equal(t, uint64(1), old.Partitions[0].NextOffset())

	result, err := manager.TruncateTopicDurable("orders", 1)
	require.NoError(t, err)
	require.True(t, result.Truncated)
	require.True(t, result.CleanupPending)
	require.True(t, manager.IsTruncationPending("orders"))
	require.Equal(t, topicMetadataFormatVersion, readTopicManifestVersion(t, cfg.LogDir))
	require.Nil(t, manager.GetTopic("orders"), "pending truncate must fence all direct topic access")
	require.ErrorContains(t, manager.MetadataReadinessError(), "topic truncation(s) pending")
	require.NoError(t, manager.CompleteTruncation("orders"))
	require.NoError(t, manager.MetadataReadinessError())

	current := manager.GetTopic("orders")
	require.NotNil(t, current)
	require.Equal(t, uint64(2), current.Revision)
	require.Equal(t, uint64(2), current.LifecycleEpoch)
	require.Zero(t, current.Partitions[0].NextOffset())
	require.Zero(t, current.Partitions[0].GetHWM())
	messages, err := manager.ReadTopicPartition("orders", 0, 0, 10)
	require.NoError(t, err)
	require.Empty(t, messages)
	require.NoError(t, manager.PublishToPartitionWithAck("orders", 0, &types.Message{
		Payload: "new", ProducerID: "producer-1", Epoch: 1, SeqNum: 1,
	}), "producer sequence state from the old lifecycle must not survive")
}

func TestApplyTruncateDefinitionFencesBeforeFallibleLocalCleanup(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := NewTopicManager(cfg, dm, nil)
	t.Cleanup(manager.Stop)

	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	require.NoError(t, manager.PublishToPartitionWithAck("orders", 0, &types.Message{Payload: "old"}))
	target := manager.GetTopic("orders").Definition()
	target.Revision++
	target.LifecycleEpoch++

	cleanupErr := errors.New("injected derived-state close failure")
	manager.SetDeleteHook(func(string) error { return cleanupErr })
	result, err := manager.ApplyTruncateDefinition(target)
	require.ErrorIs(t, err, cleanupErr)
	require.True(t, result.Truncated)
	require.True(t, result.CleanupPending)
	require.True(t, manager.IsTruncationPending("orders"))
	require.Nil(t, manager.GetTopic("orders"), "the old generation must be fenced as soon as Raft commits")
	require.ErrorContains(t, manager.MetadataReadinessError(), "topic truncation(s) pending")

	manager.SetDeleteHook(nil)
	result, err = manager.ApplyTruncateDefinition(target)
	require.NoError(t, err)
	require.True(t, result.Truncated)
	require.NoError(t, manager.CompleteTruncation("orders"))
	current := manager.GetTopic("orders")
	require.NotNil(t, current)
	require.Equal(t, target.LifecycleEpoch, current.LifecycleEpoch)
	require.Zero(t, current.Partitions[0].NextOffset())
}

func readTopicManifestVersion(t *testing.T, logDir string) int {
	t.Helper()
	// #nosec G304 -- the manifest name is fixed and logDir is the test storage root.
	data, err := os.ReadFile(filepath.Join(logDir, TopicMetadataFileName))
	require.NoError(t, err)
	var manifest topicMetadataManifest
	require.NoError(t, json.Unmarshal(data, &manifest))
	return manifest.Version
}

func TestRestoreFencesAndRecoversCommittedPendingTruncate(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000

	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	require.NoError(t, manager.PublishToPartitionWithAck("orders", 0, &types.Message{Payload: "old"}))
	result, err := manager.TruncateTopicDurable("orders", 1)
	require.NoError(t, err)
	require.True(t, result.Truncated)
	manager.Stop()
	dm.CloseAllHandlers()

	restartedDM := disk.NewDiskManager(cfg)
	t.Cleanup(restartedDM.CloseAllHandlers)
	restarted := NewTopicManager(cfg, restartedDM, nil)
	t.Cleanup(restarted.Stop)
	require.NoError(t, restarted.RestoreTopics())
	require.True(t, restarted.IsTruncationPending("orders"))
	require.Nil(t, restarted.GetTopic("orders"))
	require.NoError(t, restarted.CompleteTruncation("orders"))

	current := restarted.GetTopic("orders")
	require.NotNil(t, current)
	require.Equal(t, uint64(2), current.Revision)
	require.Equal(t, uint64(2), current.LifecycleEpoch)
	require.Zero(t, current.Partitions[0].NextOffset())
	require.Zero(t, current.Partitions[0].GetHWM())
}
