package topic

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCreateStandaloneManifestEquivalentExistingRejectsOrphanedStorage(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "declared", 0, 0, nil)
	writeMigrationSegment(t, root, "orphan", 0, 0, nil)
	definition := Definition{Name: "declared", Partitions: 1, Policy: DefaultPolicy()}
	_, manifest, err := normalizeAndMarshalManifest([]Definition{definition})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, TopicMetadataFileName), manifest, 0o600))

	result, err := CreateStandaloneManifest(root, []Definition{definition}, false)
	require.ErrorContains(t, err, "not aligned with storage")
	require.False(t, result.Changed)
}

func TestInspectStandaloneStorageReportsInvalidCurrentSizeCompactionMarker(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orders", 0, 0, []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 0}})
	logPath := filepath.Join(root, "orders", "partition_0_segment_00000000000000000000.log")
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	marker := fmt.Sprintf("%s.compacted-%d", logPath, info.Size())
	require.NoError(t, os.WriteFile(marker, []byte("invalid"), 0o600))

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.Len(t, inventory.Problems, 1)
	require.Contains(t, inventory.Problems[0].Message, "invalid content")
}

func TestInspectStandaloneStorageAllowsGapAcrossEmptyCompactedSegment(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orders", 0, 0, []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 0}})
	writeMigrationSegment(t, root, "orders", 0, 1, nil)
	writeMigrationSegment(t, root, "orders", 0, 2, []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 2}})
	emptyLog := filepath.Join(root, "orders", "partition_0_segment_00000000000000000001.log")
	require.NoError(t, os.WriteFile(emptyLog+".compacted-0", []byte(persistedCompactionMarkerVersion), 0o600))

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.Empty(t, inventory.Problems)
}

func TestArchiveTopicDirectoryExclusiveDoesNotReplaceDestination(t *testing.T) {
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	destination := filepath.Join(parent, "destination")
	require.NoError(t, os.Mkdir(source, 0o750))
	require.NoError(t, os.Mkdir(destination, 0o750))

	require.Error(t, archiveTopicDirectoryExclusive(source, destination))
	require.DirExists(t, source)
	require.DirExists(t, destination)
}
