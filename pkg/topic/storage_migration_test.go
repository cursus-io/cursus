package topic

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestInspectStandaloneStorageReadsLogsWithoutOpeningHandlers(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orders", 0, 0, []types.DiskMessage{
		{Topic: "orders", Partition: 0, Offset: 0, Payload: "first"},
		{Topic: "orders", Partition: 0, Offset: 1, Payload: "second"},
	})
	require.NoError(t, os.Mkdir(filepath.Join(root, "raft"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(root, "__transaction_state.journal"), []byte("state"), 0o600))

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.False(t, inventory.ManifestPresent)
	require.Len(t, inventory.Topics, 1)
	require.Equal(t, "orders", inventory.Topics[0].Name)
	require.Equal(t, 0, inventory.Topics[0].Partitions[0].ID)
	require.Empty(t, inventory.Problems)

	// Inventory must not create runtime index or checkpoint files.
	require.NoFileExists(t, filepath.Join(root, "orders", "partition_0_segment_00000000000000000000.index"))
	require.NoFileExists(t, filepath.Join(root, "orders", "partition_0.hwm"))
}

func TestInspectStandaloneStorageReportsCorruptAndMismatchedLogs(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orders", 0, 0, []types.DiskMessage{
		{Topic: "wrong", Partition: 0, Offset: 0, Payload: "bad"},
	})
	malformed := filepath.Join(root, "orders", "partition_01_segment_0.log")
	require.NoError(t, os.WriteFile(malformed, []byte{0, 0}, 0o600))

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.Len(t, inventory.Topics, 1)
	require.Len(t, inventory.Problems, 2)
	require.Contains(t, inventory.Problems[0].Message+inventory.Problems[1].Message, "non-canonical")
	require.Contains(t, inventory.Problems[0].Message+inventory.Problems[1].Message, "does not match")
}

func TestCreateStandaloneManifestIsExplicitExclusiveAndIdempotent(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orders", 0, 0, nil)
	definition := Definition{Name: "orders", Partitions: 1, Policy: DefaultPolicy()}

	result, err := CreateStandaloneManifest(root, []Definition{definition}, false)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.True(t, result.Committed)
	manifestPath := filepath.Join(root, TopicMetadataFileName)
	before, err := os.ReadFile(manifestPath)
	require.NoError(t, err)

	result, err = CreateStandaloneManifest(root, []Definition{definition}, false)
	require.NoError(t, err)
	require.False(t, result.Changed)

	conflicting := definition
	conflicting.Idempotent = true
	_, err = CreateStandaloneManifest(root, []Definition{conflicting}, false)
	require.ErrorContains(t, err, "already exists with different definitions")
	after, readErr := os.ReadFile(manifestPath)
	require.NoError(t, readErr)
	require.Equal(t, before, after)

	cfg := config.DefaultConfig()
	cfg.LogDir = root
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.RestoreTopics())
	require.NotNil(t, manager.GetTopic("orders"))
}

func TestCreateStandaloneManifestRejectsIncompleteDefinitionsAndDryRunDoesNotWrite(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orders", 0, 0, nil)

	result, err := CreateStandaloneManifest(root, []Definition{{Name: "orders", Partitions: 2, Policy: DefaultPolicy()}}, true)
	require.ErrorContains(t, err, "storage has 1")
	require.False(t, result.Changed)
	require.NoFileExists(t, filepath.Join(root, TopicMetadataFileName))

	result, err = CreateStandaloneManifest(root, []Definition{{Name: "orders", Partitions: 1, Policy: DefaultPolicy()}}, true)
	require.NoError(t, err)
	require.False(t, result.Changed)
	require.NoFileExists(t, filepath.Join(root, TopicMetadataFileName))
}

func TestArchiveOrphanTopicMovesExactlyOneDirectory(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "logs")
	require.NoError(t, os.Mkdir(root, 0o750))
	writeMigrationSegment(t, root, "declared", 0, 0, nil)
	writeMigrationSegment(t, root, "orphan", 0, 0, []types.DiskMessage{{Topic: "orphan", Partition: 0, Offset: 0, Payload: "keep"}})
	_, err := CreateStandaloneManifest(root, []Definition{
		{Name: "declared", Partitions: 1, Policy: DefaultPolicy()},
		{Name: "orphan", Partitions: 1, Policy: DefaultPolicy()},
	}, false)
	require.NoError(t, err)

	// Simulate a valid manifest that omits the orphan without using the migration overwrite path.
	_, data, err := normalizeAndMarshalManifest([]Definition{{Name: "declared", Partitions: 1, Policy: DefaultPolicy()}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, TopicMetadataFileName), data, 0o600))

	archive := filepath.Join(parent, "archive")
	dryRun, err := ArchiveOrphanTopic(root, archive, "orphan", true)
	require.NoError(t, err)
	require.False(t, dryRun.Changed)
	require.DirExists(t, filepath.Join(root, "orphan"))
	require.NoDirExists(t, archive)

	result, err := ArchiveOrphanTopic(root, archive, "orphan", false)
	require.NoError(t, err)
	require.True(t, result.Committed)
	require.NoDirExists(t, filepath.Join(root, "orphan"))
	require.FileExists(t, filepath.Join(archive, "orphan", "partition_0_segment_00000000000000000000.log"))
	require.DirExists(t, filepath.Join(root, "declared"))

	_, err = ArchiveOrphanTopic(root, archive, "declared", false)
	require.ErrorContains(t, err, "declared in the manifest")
}

func TestArchiveOrphanTopicRejectsArchiveInsideLogRoot(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orphan", 0, 0, nil)
	_, err := ArchiveOrphanTopic(root, filepath.Join(root, "archive"), "orphan", false)
	require.ErrorContains(t, err, "outside the log directory")
	require.DirExists(t, filepath.Join(root, "orphan"))
}

func writeMigrationSegment(t *testing.T, root, topicName string, partition int, base uint64, messages []types.DiskMessage) {
	t.Helper()
	directory := filepath.Join(root, topicName)
	require.NoError(t, os.MkdirAll(directory, 0o750))
	path := filepath.Join(directory, "partition_"+strconv.Itoa(partition)+"_segment_"+fmt.Sprintf("%020d", base)+".log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	for _, message := range messages {
		payload, serializeErr := util.SerializeDiskMessage(message)
		require.NoError(t, serializeErr)
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
		_, err = file.Write(length[:])
		require.NoError(t, err)
		_, err = file.Write(payload)
		require.NoError(t, err)
	}
	require.NoError(t, file.Close())
}
