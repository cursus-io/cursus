package topic

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestInspectStandaloneStorageDoesNotAllowGapsInActiveSegment(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orders", 0, 0, []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 1}})
	logPath := filepath.Join(root, "orders", "partition_0_segment_00000000000000000000.log")
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	marker := fmt.Sprintf("%s.compacted-%d", logPath, info.Size())
	require.NoError(t, os.WriteFile(marker, []byte(persistedCompactionMarkerVersion), 0o600))

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.NotEmpty(t, inventory.Problems)
	require.Contains(t, inventory.Problems[0].Message, "does not match segment base")
}
