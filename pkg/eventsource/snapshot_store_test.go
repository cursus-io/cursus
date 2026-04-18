package eventsource

import (
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
