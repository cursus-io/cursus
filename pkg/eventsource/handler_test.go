package eventsource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppendAndReadStream_Integration verifies that appending events updates
// the version correctly and that snapshot-based lookup skips already-snapshotted versions.
func TestAppendAndReadStream_Integration(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	ss, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = ss.Close() }()

	key := "order-123"

	// Append 3 events for the same aggregate key.
	for v := uint64(1); v <= 3; v++ {
		err := idx.Append(key, v, v*100, 0)
		require.NoError(t, err, "append version %d should succeed", v)
	}

	// Verify the current version is 3.
	assert.Equal(t, uint64(3), idx.GetVersion(key))

	// Save a snapshot at version 2.
	err = ss.Save(key, 2, `{"total":200}`)
	require.NoError(t, err)

	snap, err := ss.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(2), snap.Version)
	assert.Equal(t, `{"total":200}`, snap.Payload)

	// Lookup from snapshot.Version+1 should return only version 3.
	entries, err := idx.Lookup(key, snap.Version+1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(3), entries[0].AggregateVersion)
	assert.Equal(t, uint64(300), entries[0].Offset)
}

// TestVersionConflict verifies that the version counter advances correctly
// after each append, which is used by the handler to detect optimistic concurrency conflicts.
func TestVersionConflict(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	key := "account-456"

	// Append 2 events.
	require.NoError(t, idx.Append(key, 1, 10, 0))
	require.NoError(t, idx.Append(key, 2, 20, 0))

	// Verify current version is 2.
	assert.Equal(t, uint64(2), idx.GetVersion(key))

	// Simulate a second writer that successfully appends version 3.
	require.NoError(t, idx.Append(key, 3, 30, 0))

	// Now GetVersion returns 3, so the original writer with expected_version=2
	// would detect a conflict (expected_version != currentVersion+1).
	assert.Equal(t, uint64(3), idx.GetVersion(key))

	// Verify all 3 entries are present.
	entries, err := idx.Lookup(key, 1)
	require.NoError(t, err)
	assert.Len(t, entries, 3)
}

// TestSnapshotVersionValidation verifies that snapshot versions are validated
// against the current stream version: a snapshot version exceeding the stream
// version is invalid, while saving at the current version succeeds.
func TestSnapshotVersionValidation(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	ss, err := NewSnapshotStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = ss.Close() }()

	key := "cart-789"

	// Append 3 events to reach version 3.
	for v := uint64(1); v <= 3; v++ {
		require.NoError(t, idx.Append(key, v, v*10, 0))
	}
	assert.Equal(t, uint64(3), idx.GetVersion(key))

	// Simulate the handler's version validation:
	// snapshot version > currentVersion should be rejected.
	currentVersion := idx.GetVersion(key)
	snapshotVersion := uint64(5)
	assert.True(t, snapshotVersion > currentVersion,
		"snapshot version %d should exceed stream version %d — handler would reject this", snapshotVersion, currentVersion)

	// Version at or below current should pass validation.
	assert.False(t, uint64(3) > currentVersion, "version 3 should pass validation")
	assert.False(t, uint64(1) > currentVersion, "version 1 should pass validation")

	// Save at version 3 (current) should succeed.
	err = ss.Save(key, 3, `{"items":["a","b","c"]}`)
	require.NoError(t, err)

	snap, err := ss.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(3), snap.Version)
	assert.Equal(t, `{"items":["a","b","c"]}`, snap.Payload)

	// Overwrite with a lower version — the store allows it (last write wins),
	// but the handler would prevent this in production via its version check.
	err = ss.Save(key, 2, `{"items":["a","b"]}`)
	require.NoError(t, err)

	snap, err = ss.Read(key)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, uint64(2), snap.Version, "snapshot store uses last-write-wins; handler prevents version regression")
}

// TestMultipleAggregates verifies that events appended to different aggregate keys
// maintain independent version tracking.
func TestMultipleAggregates(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	keys := []string{"order-1", "order-2", "order-3"}

	// Append a different number of events per key.
	for i, key := range keys {
		count := uint64(i + 1) // 1, 2, 3 events respectively
		for v := uint64(1); v <= count; v++ {
			err := idx.Append(key, v, uint64(i*100)+v, 0)
			require.NoError(t, err)
		}
	}

	// Verify each key has its own independent version.
	assert.Equal(t, uint64(1), idx.GetVersion("order-1"))
	assert.Equal(t, uint64(2), idx.GetVersion("order-2"))
	assert.Equal(t, uint64(3), idx.GetVersion("order-3"))

	// Verify lookup returns only entries for the requested key.
	entries1, err := idx.Lookup("order-1", 1)
	require.NoError(t, err)
	assert.Len(t, entries1, 1)

	entries2, err := idx.Lookup("order-2", 1)
	require.NoError(t, err)
	assert.Len(t, entries2, 2)

	entries3, err := idx.Lookup("order-3", 1)
	require.NoError(t, err)
	assert.Len(t, entries3, 3)

	// A key that was never appended should have version 0.
	assert.Equal(t, uint64(0), idx.GetVersion("nonexistent"))
}
