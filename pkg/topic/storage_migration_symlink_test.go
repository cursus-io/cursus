package topic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInspectStandaloneStorageReportsTopicSymlink(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(root, "linked-topic")); err != nil {
		t.Skipf("symlink creation is unavailable: %v", err)
	}

	inventory, err := InspectStandaloneStorage(root)
	require.NoError(t, err)
	require.Empty(t, inventory.Topics)
	require.Len(t, inventory.Problems, 1)
	require.Contains(t, inventory.Problems[0].Message, "symbolic link")
}
