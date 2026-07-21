package topic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveOrphanTopicRejectsSymlinkedParentIntoLogRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "logs")
	require.NoError(t, os.Mkdir(root, 0o750))
	writeMigrationSegment(t, root, "orphan", 0, 0, nil)
	inside := filepath.Join(root, "inside")
	require.NoError(t, os.Mkdir(inside, 0o750))
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(inside, alias); err != nil {
		t.Skipf("symlink creation is unavailable: %v", err)
	}

	_, err := ArchiveOrphanTopic(root, filepath.Join(alias, "archive"), "orphan", false)
	require.ErrorContains(t, err, "resolves inside the log directory")
	require.DirExists(t, filepath.Join(root, "orphan"))
}
