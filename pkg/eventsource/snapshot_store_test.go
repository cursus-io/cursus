package eventsource

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotStore_SaveAndRead(t *testing.T) {
	dir := t.TempDir()
	store, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	err = store.Save("user:1", 5, `{"name":"alice"}`)
	require.NoError(t, err)

	snap, err := store.Read("user:1")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(5), snap.Version)
	assert.Equal(t, `{"name":"alice"}`, snap.Payload)
}

func TestSnapshotStore_Overwrite(t *testing.T) {
	dir := t.TempDir()
	store, err := NewSnapshotStore(dir, 1)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	err = store.Save("order:99", 1, `{"status":"pending"}`)
	require.NoError(t, err)

	err = store.Save("order:99", 3, `{"status":"shipped"}`)
	require.NoError(t, err)

	snap, err := store.Read("order:99")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(3), snap.Version)
	assert.Equal(t, `{"status":"shipped"}`, snap.Payload)
}

func TestSnapshotStore_NotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewSnapshotStore(dir, 2)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	snap, err := store.Read("nonexistent")
	require.NoError(t, err)
	assert.Nil(t, snap)
}

func TestSnapshotStore_LoadFromDisk(t *testing.T) {
	dir := t.TempDir()

	// First session: write snapshots and close.
	store, err := NewSnapshotStore(dir, 3)
	require.NoError(t, err)

	err = store.Save("account:1", 10, `{"balance":100}`)
	require.NoError(t, err)

	err = store.Save("account:2", 7, `{"balance":200}`)
	require.NoError(t, err)

	// Overwrite account:1 so the reload must pick the latest entry.
	err = store.Save("account:1", 15, `{"balance":150}`)
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	// Second session: reopen and verify persistence.
	store2, err := NewSnapshotStore(dir, 3)
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()

	snap, err := store2.Read("account:1")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(15), snap.Version)
	assert.Equal(t, `{"balance":150}`, snap.Payload)

	snap, err = store2.Read("account:2")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(7), snap.Version)
	assert.Equal(t, `{"balance":200}`, snap.Payload)
}

func TestSnapshotStore_ConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	store, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := "concurrent-snap"

	// Seed an initial snapshot so reads always find something.
	require.NoError(t, store.Save(key, 1, `{"v":1}`))

	const iterations = 100
	var wg sync.WaitGroup

	// Writer goroutine: saves snapshots at increasing versions.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(2); i <= iterations; i++ {
			payload := fmt.Sprintf(`{"v":%d}`, i)
			err := store.Save(key, i, payload)
			assert.NoError(t, err)
		}
	}()

	// Multiple reader goroutines: read concurrently while writer is active.
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				snap, err := store.Read(key)
				assert.NoError(t, err)
				if snap != nil {
					assert.True(t, snap.Version >= 1 && snap.Version <= iterations,
						"version %d out of range", snap.Version)
				}
			}
		}()
	}

	wg.Wait()

	// After all writes complete, final version should be `iterations`.
	snap, err := store.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(iterations), snap.Version)
}

func TestSnapshotStore_MultipleKeys(t *testing.T) {
	dir := t.TempDir()
	store, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	keys := []string{"alpha", "beta", "gamma"}
	for i, k := range keys {
		v := uint64(i + 1)
		payload := fmt.Sprintf(`{"key":"%s"}`, k)
		require.NoError(t, store.Save(k, v, payload))
	}

	// Each key should have its own independent snapshot.
	for i, k := range keys {
		snap, err := store.Read(k)
		require.NoError(t, err)
		require.NotNil(t, snap, "key %s should have a snapshot", k)
		assert.Equal(t, uint64(i+1), snap.Version)
		assert.Equal(t, fmt.Sprintf(`{"key":"%s"}`, k), snap.Payload)
	}

	// A key that was never saved should return nil.
	snap, err := store.Read("delta")
	require.NoError(t, err)
	assert.Nil(t, snap)
}

func TestSnapshotStore_OverwriteKey(t *testing.T) {
	dir := t.TempDir()
	store, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := "overwrite-me"

	require.NoError(t, store.Save(key, 1, `{"state":"first"}`))
	snap, err := store.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(1), snap.Version)
	assert.Equal(t, `{"state":"first"}`, snap.Payload)

	// Overwrite with a newer version.
	require.NoError(t, store.Save(key, 5, `{"state":"second"}`))
	snap, err = store.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(5), snap.Version)
	assert.Equal(t, `{"state":"second"}`, snap.Payload)

	// Overwrite again with a different payload at a higher version.
	require.NoError(t, store.Save(key, 10, `{"state":"third"}`))
	snap, err = store.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(10), snap.Version)
	assert.Equal(t, `{"state":"third"}`, snap.Payload)

	// Close and reopen to verify last-write-wins persistence.
	require.NoError(t, store.Close())

	store2, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()

	snap, err = store2.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(10), snap.Version)
	assert.Equal(t, `{"state":"third"}`, snap.Payload)
}

func TestSnapshotStore_PartialWriteRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write one good entry manually, then append truncated data to simulate
	// a partial write (e.g., crash mid-save).
	store, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	require.NoError(t, store.Save("good-key", 3, `{"ok":true}`))
	require.NoError(t, store.Close())

	path := filepath.Join(dir, fmt.Sprintf("partition_%d_snapshots.dat", 0))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)

	// Write a KeyLen that claims 200 bytes but only provide 3 bytes of key data.
	var keyLenBuf [2]byte
	binary.BigEndian.PutUint16(keyLenBuf[:], 200)
	_, err = f.Write(keyLenBuf[:])
	require.NoError(t, err)
	_, err = f.Write([]byte("abc")) // only 3 bytes, not 200
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Reopen: loadFromDisk should truncate the partial entry and recover the good one.
	store2, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()

	snap, err := store2.Read("good-key")
	require.NoError(t, err)
	require.NotNil(t, snap, "good entry should survive partial-write recovery")
	assert.Equal(t, uint64(3), snap.Version)
	assert.Equal(t, `{"ok":true}`, snap.Payload)

	// The truncated entry's key should not appear.
	snap, err = store2.Read("abc")
	require.NoError(t, err)
	assert.Nil(t, snap)

	// We should be able to save new entries after recovery.
	require.NoError(t, store2.Save("new-key", 1, `{"recovered":true}`))
	snap, err = store2.Read("new-key")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(1), snap.Version)
}
