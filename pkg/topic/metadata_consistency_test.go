package topic

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestTopicMetadataMissingManifestRejectsPersistedTopicStorage(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	writePersistedTopicLog(t, cfg.LogDir, "legacy")

	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := NewTopicManager(cfg, dm, nil)

	err := manager.RestoreTopics()
	require.ErrorContains(t, err, "manifest is missing")
	require.ErrorContains(t, err, "legacy")
	require.Empty(t, manager.ListTopics())
}

func TestTopicMetadataManifestRejectsUnlistedTopicStorage(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()

	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("declared", 1, false, false))
	closeTopicManager(manager)
	dm.CloseAllHandlers()
	writePersistedTopicLog(t, cfg.LogDir, "orphaned")

	restartedDM := disk.NewDiskManager(cfg)
	t.Cleanup(restartedDM.CloseAllHandlers)
	restarted := NewTopicManager(cfg, restartedDM, nil)
	err := restarted.RestoreTopics()
	require.ErrorContains(t, err, "manifest omits persisted topic storage")
	require.ErrorContains(t, err, "orphaned")
	require.Empty(t, restarted.ListTopics())
}

func TestTopicCreateRejectsOrphanedStorage(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	writePersistedTopicLog(t, cfg.LogDir, "orders")

	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := NewTopicManager(cfg, dm, nil)

	err := manager.CreateTopic("orders", 1, false, false)
	require.ErrorContains(t, err, "persisted storage without an active durable definition")
	require.Nil(t, manager.GetTopic("orders"))
}

func TestTopicMetadataDirectorySyncFailureKeepsCreateAligned(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()

	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	originalSync := syncTopicMetadataDirectoryFn
	syncTopicMetadataDirectoryFn = func(string) error { return errors.New("directory sync failed") }
	t.Cleanup(func() { syncTopicMetadataDirectoryFn = originalSync })

	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	require.NotNil(t, manager.GetTopic("orders"))
	syncTopicMetadataDirectoryFn = originalSync
	closeTopicManager(manager)
	dm.CloseAllHandlers()

	restartedDM := disk.NewDiskManager(cfg)
	t.Cleanup(restartedDM.CloseAllHandlers)
	restarted := NewTopicManager(cfg, restartedDM, nil)
	require.NoError(t, restarted.RestoreTopics())
	defer closeTopicManager(restarted)
	require.NotNil(t, restarted.GetTopic("orders"))
}

func TestTopicMetadataDirectorySyncFailureKeepsDeletionAligned(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()

	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))
	originalSync := syncTopicMetadataDirectoryFn
	syncTopicMetadataDirectoryFn = func(string) error { return errors.New("directory sync failed") }
	t.Cleanup(func() { syncTopicMetadataDirectoryFn = originalSync })

	deleted, err := manager.DeleteTopicDurable("orders")
	require.NoError(t, err)
	require.True(t, deleted)
	require.Nil(t, manager.GetTopic("orders"))
	syncTopicMetadataDirectoryFn = originalSync
	dm.CloseAllHandlers()

	restartedDM := disk.NewDiskManager(cfg)
	t.Cleanup(restartedDM.CloseAllHandlers)
	restarted := NewTopicManager(cfg, restartedDM, nil)
	require.NoError(t, restarted.RestoreTopics())
	require.Nil(t, restarted.GetTopic("orders"))
}

func writePersistedTopicLog(t *testing.T, root, name string) {
	t.Helper()
	dir := filepath.Join(root, name)
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "partition_0_segment_00000000000000000000.log"),
		nil,
		0o600,
	))
}
