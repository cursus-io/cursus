package topic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveOrphanTopicRefusesDestinationCollision(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "logs")
	archive := filepath.Join(parent, "archive")
	require.NoError(t, os.Mkdir(root, 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(archive, "orphan"), 0o750))
	writePersistedTestSegment(t, root, "orphan", 0, 0, nil)

	_, err := ArchiveOrphanTopic(root, archive, "orphan", false)
	require.ErrorContains(t, err, "destination already exists")
	require.DirExists(t, filepath.Join(root, "orphan"))
}
