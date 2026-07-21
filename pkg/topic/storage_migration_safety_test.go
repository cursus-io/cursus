package topic

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCreateStandaloneManifestRejectsUnsafeEventSourcingPolicy(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "events", 0, 0, nil)
	policy := DefaultPolicy()
	policy.CleanupPolicy = config.CleanupPolicyCompact

	_, err := CreateStandaloneManifest(root, []Definition{{
		Name:          "events",
		Partitions:    1,
		EventSourcing: true,
		Policy:        policy,
	}}, false)
	require.ErrorContains(t, err, "not supported for event-sourcing topics")
	require.NoFileExists(t, filepath.Join(root, TopicMetadataFileName))
}

func TestCreateStandaloneManifestNeverReplacesCorruptManifest(t *testing.T) {
	root := t.TempDir()
	writeMigrationSegment(t, root, "orders", 0, 0, nil)
	manifestPath := filepath.Join(root, TopicMetadataFileName)
	original := []byte(`{"version":1,"topics":[],"unknown":true}`)
	require.NoError(t, os.WriteFile(manifestPath, original, 0o600))

	_, err := CreateStandaloneManifest(root, []Definition{{Name: "orders", Partitions: 1, Policy: DefaultPolicy()}}, false)
	require.ErrorContains(t, err, "unknown field")
	after, readErr := os.ReadFile(manifestPath)
	require.NoError(t, readErr)
	require.Equal(t, original, after)
}

func TestArchiveOrphanTopicRefusesDestinationCollision(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "logs")
	archive := filepath.Join(parent, "archive")
	require.NoError(t, os.Mkdir(root, 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(archive, "orphan"), 0o750))
	writeMigrationSegment(t, root, "orphan", 0, 0, nil)

	_, err := ArchiveOrphanTopic(root, archive, "orphan", false)
	require.ErrorContains(t, err, "destination already exists")
	require.DirExists(t, filepath.Join(root, "orphan"))
}
