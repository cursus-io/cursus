package eventsource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionBeforeUsesOffsetOrderAndSeparateSeed(t *testing.T) {
	idx, err := NewStreamIndex(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, idx.Close()) })
	require.NoError(t, idx.SeedVersion("a", 3))
	revision, err := idx.ResetRevision()
	require.NoError(t, err)
	require.NoError(t, idx.Append("a", 5, 12, 0))
	require.NoError(t, idx.Append("a", 4, 10, 0))
	for end, expected := range map[uint64]uint64{0: 3, 10: 3, 11: 4, 12: 4, 13: 5} {
		got, err := idx.VersionBefore("a", end)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	}
	require.NoError(t, idx.SeedVersion("a", 3))
	after, err := idx.ResetRevision()
	require.NoError(t, err)
	require.Equal(t, revision, after)
	require.NoError(t, idx.Reset())
	got, err := idx.VersionBefore("a", 99)
	require.NoError(t, err)
	require.Zero(t, got)
}

func TestSnapshotCertificationRejectsAheadCache(t *testing.T) {
	h, _ := newBootstrapHandler(t)
	result, response := h.AppendStream("APPEND_STREAM topic=orders key=a version=1 event_type=created message=one", AppendOptions{})
	require.Empty(t, response)
	idx, err := h.getIndex("orders", 0)
	require.NoError(t, err)
	require.NoError(t, idx.Append("a", 2, result.Offset+1, 0))
	_, response = h.SaveSnapshot("SAVE_SNAPSHOT topic=orders key=a version=2 message=bad", nil)
	require.Contains(t, response, "snapshot_version_exceeds_stream")
	require.Contains(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 2, Payload: "bad"}), "snapshot_version_exceeds_stream")
	require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "good"}))
}

func TestSnapshotCertificationAllowsCommittedGrowthBetweenPasses(t *testing.T) {
	h, _ := newBootstrapHandler(t)
	_, response := h.AppendStream("APPEND_STREAM topic=orders key=a version=1 event_type=created message=one", AppendOptions{})
	require.Empty(t, response)
	_, response = h.SaveSnapshot("SAVE_SNAPSHOT topic=orders key=a version=1 message=snapshot", func(SnapshotResult) error {
		_, response := h.AppendStream("APPEND_STREAM topic=orders key=a version=2 event_type=changed message=two", AppendOptions{})
		require.Empty(t, response)
		return nil
	})
	require.Empty(t, response)
}
