package eventsource

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotStoreSyncsFileAndPartitionDirectoryEntries(t *testing.T) {
	originalSync := syncSnapshotCreationDirectory
	t.Cleanup(func() { syncSnapshotCreationDirectory = originalSync })

	root := t.TempDir()
	dir := filepath.Join(root, "partition_0")
	var synced []string
	syncSnapshotCreationDirectory = func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return nil
	}

	store, err := NewSnapshotStore(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if len(synced) != 2 || synced[0] != dir || synced[1] != root {
		t.Fatalf("directory syncs = %v, want [%s %s]", synced, dir, root)
	}
	if err := store.Save("account:1", 1, `{"balance":100}`); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotStoreRejectsDirectorySyncFailure(t *testing.T) {
	originalSync := syncSnapshotCreationDirectory
	t.Cleanup(func() { syncSnapshotCreationDirectory = originalSync })
	syncSnapshotCreationDirectory = func(string) error {
		return errors.New("injected directory sync failure")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "partition_0_snapshots.dat")
	store, err := NewSnapshotStore(dir, 0)
	if store != nil || err == nil || !strings.Contains(err.Error(), "persist file entry") {
		t.Fatalf("NewSnapshotStore = (%v, %v), want no store and directory sync failure", store, err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("snapshot remains visible after failed creation sync: %v", statErr)
	}
}

func TestSnapshotSyncFailureDoesNotPublishInMemoryState(t *testing.T) {
	store, err := NewSnapshotStore(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.syncFileFn = func() error { return errors.New("injected snapshot fsync failure") }

	err = store.Save("account:1", 1, `{"balance":100}`)
	if err == nil || !strings.Contains(err.Error(), "snapshot store: sync") {
		t.Fatalf("Save error = %v, want file sync failure", err)
	}
	snapshot, err := store.Read("account:1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot != nil {
		t.Fatalf("failed save became visible in memory: %+v", snapshot)
	}

	store.syncFileFn = nil
	if err := store.Save("account:1", 1, `{"balance":100}`); err != nil {
		t.Fatalf("retry after restoring sync: %v", err)
	}
}
