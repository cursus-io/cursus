package replication

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/stretchr/testify/require"
)

func TestEnsureRaftRecoveryFormatCreatesAndValidatesVersionNineMarker(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, ensureRaftRecoveryFormat(directory))
	// #nosec G304 -- the marker path is fixed beneath t.TempDir.
	data, err := os.ReadFile(filepath.Join(directory, raftFormatMarkerName))
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(fsm.SnapshotVersionCurrent)+"\n", string(data))
	require.NoError(t, ensureRaftRecoveryFormat(directory))
}

func TestEnsureRaftRecoveryFormatRejectsUnmarkedPersistedState(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "raft.db"), []byte("legacy"), 0o600))

	err := ensureRaftRecoveryFormat(directory)
	require.ErrorIs(t, err, fsm.ErrUnsupportedRecoveryProtocol)
	require.Contains(t, err.Error(), "clean bootstrap")
	require.NoFileExists(t, filepath.Join(directory, raftFormatMarkerName))
}

func TestEnsureRaftRecoveryFormatRejectsUnknownMarker(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, raftFormatMarkerName), []byte("8\n"), 0o600))

	err := ensureRaftRecoveryFormat(directory)
	require.True(t, errors.Is(err, fsm.ErrUnsupportedRecoveryProtocol))
}

func TestEnsureRaftRecoveryFormatSyncsContainingDirectory(t *testing.T) {
	directory := t.TempDir()
	original := syncRaftDirectoryFn
	t.Cleanup(func() { syncRaftDirectoryFn = original })
	var synced string
	syncRaftDirectoryFn = func(path string) error {
		synced = path
		return nil
	}

	require.NoError(t, ensureRaftRecoveryFormat(directory))
	require.Equal(t, directory, synced)
}

func TestEnsureRaftRecoveryFormatFailsWhenDirectorySyncFails(t *testing.T) {
	directory := t.TempDir()
	original := syncRaftDirectoryFn
	t.Cleanup(func() { syncRaftDirectoryFn = original })
	want := errors.New("directory sync failed")
	syncRaftDirectoryFn = func(string) error { return want }

	err := ensureRaftRecoveryFormat(directory)
	require.ErrorIs(t, err, want)
	require.Contains(t, err.Error(), "sync Raft format marker directory")
}
