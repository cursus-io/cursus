package eventsource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamIndex_AppendAndLookup(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	key := "order-123"

	require.NoError(t, idx.Append(key, 1, 100, 0))
	require.NoError(t, idx.Append(key, 2, 200, 1))
	require.NoError(t, idx.Append(key, 3, 300, 2))

	assert.Equal(t, uint64(3), idx.GetVersion(key))

	entries, err := idx.Lookup(key, 2)
	require.NoError(t, err)
	assert.Len(t, entries, 2)
	assert.Equal(t, uint64(2), entries[0].AggregateVersion)
	assert.Equal(t, uint64(200), entries[0].Offset)
	assert.Equal(t, uint64(3), entries[1].AggregateVersion)
	assert.Equal(t, uint64(300), entries[1].Offset)
}

func TestStreamIndex_LoadFromDisk(t *testing.T) {
	dir := t.TempDir()

	// Write entries and close.
	idx, err := NewStreamIndex(dir, 1)
	require.NoError(t, err)

	require.NoError(t, idx.Append("user-1", 1, 10, 0))
	require.NoError(t, idx.Append("user-1", 2, 20, 1))
	require.NoError(t, idx.Append("user-1", 3, 30, 2))
	require.NoError(t, idx.Close())

	// Reopen and verify cache is restored.
	idx2, err := NewStreamIndex(dir, 1)
	require.NoError(t, err)
	defer func() { _ = idx2.Close() }()

	assert.Equal(t, uint64(3), idx2.GetVersion("user-1"))

	entries, err := idx2.Lookup("user-1", 1)
	require.NoError(t, err)
	assert.Len(t, entries, 3)
	assert.Equal(t, uint64(1), entries[0].AggregateVersion)
	assert.Equal(t, uint64(2), entries[1].AggregateVersion)
	assert.Equal(t, uint64(3), entries[2].AggregateVersion)
}

func TestStreamIndex_MultipleKeys(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	require.NoError(t, idx.Append("key-a", 1, 100, 0))
	require.NoError(t, idx.Append("key-a", 2, 200, 1))
	require.NoError(t, idx.Append("key-b", 1, 300, 2))

	assert.Equal(t, uint64(2), idx.GetVersion("key-a"))
	assert.Equal(t, uint64(1), idx.GetVersion("key-b"))

	entriesA, err := idx.Lookup("key-a", 1)
	require.NoError(t, err)
	assert.Len(t, entriesA, 2)

	entriesB, err := idx.Lookup("key-b", 1)
	require.NoError(t, err)
	assert.Len(t, entriesB, 1)
	assert.Equal(t, uint64(300), entriesB[0].Offset)
}

func TestStreamIndex_UnknownKey(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	assert.Equal(t, uint64(0), idx.GetVersion("nonexistent"))

	entries, err := idx.Lookup("nonexistent", 1)
	require.NoError(t, err)
	assert.Nil(t, entries)
}

func TestStreamIndex_CheckAndAppend_VersionConflict(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	key := "order-conflict"

	// First append at version 1 should succeed.
	ok, _, err := idx.CheckAndAppend(key, 1, 100, 0)
	require.NoError(t, err)
	assert.True(t, ok, "first append should succeed")

	// Second append at version 2 should succeed.
	ok, _, err = idx.CheckAndAppend(key, 2, 200, 1)
	require.NoError(t, err)
	assert.True(t, ok, "second append should succeed")

	// Duplicate version 2 should fail (conflict).
	ok, current, err := idx.CheckAndAppend(key, 2, 300, 2)
	require.NoError(t, err)
	assert.False(t, ok, "duplicate version should conflict")
	assert.Equal(t, uint64(2), current, "current version should be 2")

	// Out-of-order version 1 should also fail.
	ok, current, err = idx.CheckAndAppend(key, 1, 400, 3)
	require.NoError(t, err)
	assert.False(t, ok, "out-of-order version should conflict")
	assert.Equal(t, uint64(2), current, "current version should still be 2")

	// Version 3 should succeed (correct next version).
	ok, _, err = idx.CheckAndAppend(key, 3, 500, 4)
	require.NoError(t, err)
	assert.True(t, ok, "version 3 should succeed")
	assert.Equal(t, uint64(3), idx.GetVersion(key))
}
