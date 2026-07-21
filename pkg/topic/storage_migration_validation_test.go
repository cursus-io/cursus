package topic

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestInspectStandaloneStorageRequiresValidCompactionMarkerForSegmentGaps(t *testing.T) {
	tests := []struct {
		name          string
		markerContent string
		wantProblem   bool
		wantMessage   string
	}{
		{name: "missing marker", wantProblem: true, wantMessage: "not contiguous"},
		{name: "invalid marker", markerContent: "not-a-cursus-marker\n", wantProblem: true, wantMessage: "invalid content"},
		{name: "valid marker", markerContent: persistedCompactionMarkerVersion, wantProblem: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			writeMigrationSegment(t, root, "orders", 0, 0, []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 0}})
			writeMigrationSegment(t, root, "orders", 0, 2, []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 2}})
			writeMigrationSegment(t, root, "orders", 0, 3, []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 3}})
			if test.markerContent != "" {
				logPath := filepath.Join(root, "orders", "partition_0_segment_00000000000000000002.log")
				info, err := os.Stat(logPath)
				require.NoError(t, err)
				marker := fmt.Sprintf("%s.compacted-%d", logPath, info.Size())
				require.NoError(t, os.WriteFile(marker, []byte(test.markerContent), 0o600))
			}

			inventory, err := InspectStandaloneStorage(root)
			require.NoError(t, err)
			if test.wantProblem {
				require.NotEmpty(t, inventory.Problems)
				require.Contains(t, inventory.Problems[0].Message, test.wantMessage)
			} else {
				require.Empty(t, inventory.Problems)
			}
		})
	}
}

func TestInspectStandaloneStorageReportsTruncatedRecord(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "orders")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	path := filepath.Join(directory, "partition_0_segment_00000000000000000000.log")
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], 8)
	require.NoError(t, os.WriteFile(path, append(length[:], 1, 2), 0o600))

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.Len(t, inventory.Problems, 1)
	require.Contains(t, inventory.Problems[0].Message, "truncated record payload")
}

func TestCreateStandaloneManifestRejectsPartitionIDsThatDoNotStartAtZero(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orders", 1, 0, nil)

	_, err := CreateStandaloneManifest(root, []Definition{{Name: "orders", Partitions: 1, Policy: DefaultPolicy()}}, false)
	require.ErrorContains(t, err, "not contiguous from zero")
	require.NoFileExists(t, filepath.Join(root, TopicMetadataFileName))
}
