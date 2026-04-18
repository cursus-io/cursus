package eventsource

import (
	"fmt"
	"sync"
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

func TestStreamIndex_CheckEnqueueAndAppend(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		dir := t.TempDir()
		idx, err := NewStreamIndex(dir, 0)
		require.NoError(t, err)
		defer func() { _ = idx.Close() }()

		key := "enqueue-ok"
		callCount := 0
		ok, ver, err := idx.CheckEnqueueAndAppend(key, 1, func() (uint64, error) {
			callCount++
			return 42, nil
		})
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint64(1), ver)
		assert.Equal(t, 1, callCount, "callback should be invoked exactly once")
		assert.Equal(t, uint64(1), idx.GetVersion(key))

		// Verify the entry was recorded with the offset returned by the callback.
		entries, err := idx.Lookup(key, 1)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, uint64(42), entries[0].Offset)
	})

	t.Run("version_conflict", func(t *testing.T) {
		dir := t.TempDir()
		idx, err := NewStreamIndex(dir, 0)
		require.NoError(t, err)
		defer func() { _ = idx.Close() }()

		key := "enqueue-conflict"

		// Seed version 1.
		require.NoError(t, idx.Append(key, 1, 10, 0))

		callCount := 0
		ok, current, err := idx.CheckEnqueueAndAppend(key, 1, func() (uint64, error) {
			callCount++
			return 99, nil
		})
		require.NoError(t, err)
		assert.False(t, ok, "should conflict: expected=1 but current=1, need current+1")
		assert.Equal(t, uint64(1), current)
		assert.Equal(t, 0, callCount, "callback must not be invoked on version conflict")
	})

	t.Run("callback_error", func(t *testing.T) {
		dir := t.TempDir()
		idx, err := NewStreamIndex(dir, 0)
		require.NoError(t, err)
		defer func() { _ = idx.Close() }()

		key := "enqueue-err"
		ok, _, err := idx.CheckEnqueueAndAppend(key, 1, func() (uint64, error) {
			return 0, fmt.Errorf("enqueue failed")
		})
		assert.Error(t, err)
		assert.False(t, ok)
		assert.Contains(t, err.Error(), "enqueue failed")

		// Version should remain 0 because the append never completed.
		assert.Equal(t, uint64(0), idx.GetVersion(key))
	})
}

func TestStreamIndex_ConcurrentCheckAndAppend(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	key := "concurrent-key"
	const goroutines = 20

	var wg sync.WaitGroup
	successes := make(chan uint64, goroutines)

	// All goroutines try to append version 1 concurrently; exactly one should win.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(offset uint64) {
			defer wg.Done()
			ok, _, err := idx.CheckAndAppend(key, 1, offset, 0)
			if err == nil && ok {
				successes <- offset
			}
		}(uint64(i))
	}
	wg.Wait()
	close(successes)

	// Exactly one goroutine should have succeeded.
	var wins []uint64
	for off := range successes {
		wins = append(wins, off)
	}
	assert.Len(t, wins, 1, "exactly one concurrent append should succeed for version 1")
	assert.Equal(t, uint64(1), idx.GetVersion(key))
}

func TestStreamIndex_LargeBatchAppend(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	key := "large-batch"
	const count = 500

	for v := uint64(1); v <= count; v++ {
		require.NoError(t, idx.Append(key, v, v*10, v))
	}

	assert.Equal(t, uint64(count), idx.GetVersion(key))

	// Lookup all entries.
	entries, err := idx.Lookup(key, 1)
	require.NoError(t, err)
	assert.Len(t, entries, count)

	// Spot-check first and last.
	assert.Equal(t, uint64(1), entries[0].AggregateVersion)
	assert.Equal(t, uint64(10), entries[0].Offset)
	assert.Equal(t, uint64(count), entries[count-1].AggregateVersion)
	assert.Equal(t, uint64(count*10), entries[count-1].Offset)

	// Close and reopen to verify persistence of a large index.
	require.NoError(t, idx.Close())

	idx2, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx2.Close() }()

	assert.Equal(t, uint64(count), idx2.GetVersion(key))
	entries2, err := idx2.Lookup(key, 1)
	require.NoError(t, err)
	assert.Len(t, entries2, count)
}

func TestStreamIndex_EmptyLookup(t *testing.T) {
	dir := t.TempDir()
	idx, err := NewStreamIndex(dir, 0)
	require.NoError(t, err)
	defer func() { _ = idx.Close() }()

	key := "sparse-key"

	// Append versions 1 through 5.
	for v := uint64(1); v <= 5; v++ {
		require.NoError(t, idx.Append(key, v, v*100, 0))
	}

	// Lookup with fromVersion greater than all existing versions should return empty.
	entries, err := idx.Lookup(key, 100)
	require.NoError(t, err)
	assert.Empty(t, entries, "fromVersion beyond all entries should return empty slice")

	// Lookup at exactly the max version should return one entry.
	entries, err = idx.Lookup(key, 5)
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, uint64(5), entries[0].AggregateVersion)

	// Lookup for a key that does not exist at all.
	entries, err = idx.Lookup("no-such-key", 1)
	require.NoError(t, err)
	assert.Nil(t, entries)
}
