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

func TestRestoreTopicsRejectsMissingInternalMetadataStorage(t *testing.T) {
	for _, test := range []struct {
		name          string
		createPartOne bool
	}{
		{name: "missing topic directory"},
		{name: "missing declared partition", createPartOne: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			_, manifest, err := normalizeAndMarshalCurrentManifest([]Definition{{
				Name: config.ConsumerOffsetsTopicName, Partitions: 2, Policy: ConsumerMetadataPolicy(),
			}})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(root, TopicMetadataFileName), manifest, 0o600))
			if test.createPartOne {
				writePersistedTopicLog(t, root, config.ConsumerOffsetsTopicName)
			}
			cfg := config.DefaultConfig()
			cfg.LogDir = root
			dm := disk.NewDiskManager(cfg)
			manager := NewTopicManager(cfg, dm, nil)

			err = manager.RestoreTopics()
			require.Error(t, err)
			require.Error(t, manager.MetadataReadinessError())
			require.Empty(t, manager.ListTopics(), "missing internal storage must not be recreated as an empty healthy topic")
		})
	}
}
func TestRestoreTopicsRejectsLogPathThatIsNotDirectory(t *testing.T) {
	root := t.TempDir()
	logPath := filepath.Join(root, "not-a-directory")
	require.NoError(t, os.WriteFile(logPath, []byte("blocked"), 0o600))
	cfg := config.DefaultConfig()
	cfg.LogDir = logPath
	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)

	err := manager.RestoreTopics()
	require.ErrorContains(t, err, "scan topic storage root")
	require.Error(t, manager.MetadataReadinessError())
	require.Error(t, dm.Ready())
}
func TestTopicMetadataRuntimeReportsManifestFailureAndConfirmedOrphans(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	writePersistedTopicLog(t, cfg.LogDir, "legacy-a")
	writePersistedTopicLog(t, cfg.LogDir, "legacy-b")

	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := NewTopicManager(cfg, dm, nil)
	require.Error(t, manager.RestoreTopics())

	snapshot := manager.RuntimeSnapshot()
	require.NotEmpty(t, snapshot.MetadataLoadFailure)
	require.Equal(t, 2, snapshot.MetadataOrphanTopicCount)
	require.ErrorContains(t, manager.MetadataReadinessError(), "manifest load failure")
	require.ErrorContains(t, manager.MetadataReadinessError(), "2 orphan topic(s) confirmed")
}

func TestTopicMetadataRuntimeReportsCorruptManifestWithoutGuessingOrphans(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(cfg.LogDir, TopicMetadataFileName), []byte(`{"version":`), 0o600))

	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := NewTopicManager(cfg, dm, nil)
	require.Error(t, manager.RestoreTopics())

	snapshot := manager.RuntimeSnapshot()
	require.NotEmpty(t, snapshot.MetadataLoadFailure)
	require.Zero(t, snapshot.MetadataOrphanTopicCount)
}

func TestTopicMetadataDurabilityWarningTracksCurrentAndCumulative(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	t.Cleanup(func() {
		closeTopicManager(manager)
		dm.CloseAllHandlers()
	})

	originalSync := syncTopicMetadataDirectoryFn
	t.Cleanup(func() { syncTopicMetadataDirectoryFn = originalSync })
	syncTopicMetadataDirectoryFn = func(string) error { return errors.New("directory sync failed") }
	require.NoError(t, manager.CreateTopic("orders", 1, false, false))

	snapshot := manager.RuntimeSnapshot()
	require.Contains(t, snapshot.MetadataDurabilityWarning, "directory sync failed")
	require.Equal(t, uint64(1), snapshot.MetadataDurabilityWarningsTotal)
	require.ErrorContains(t, manager.MetadataReadinessError(), "metadata durability warning")

	syncTopicMetadataDirectoryFn = originalSync
	require.NoError(t, manager.CreateTopic("payments", 1, false, false))
	snapshot = manager.RuntimeSnapshot()
	require.Empty(t, snapshot.MetadataDurabilityWarning)
	require.Equal(t, uint64(1), snapshot.MetadataDurabilityWarningsTotal)
	require.NoError(t, manager.MetadataReadinessError())

	syncTopicMetadataDirectoryFn = func(string) error { return errors.New("directory sync failed again") }
	require.NoError(t, manager.CreateTopic("shipments", 1, false, false))
	snapshot = manager.RuntimeSnapshot()
	require.Equal(t, uint64(2), snapshot.MetadataDurabilityWarningsTotal)
}
