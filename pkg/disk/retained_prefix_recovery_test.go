package disk

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestRetainedPrefixLegacyCanonicalAndExistingDigests(t *testing.T) {
	legacy, cfg := bootstrapTestHandler(t, 1)
	current, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, legacy.Close())
	records := prefixRecords(3)
	var data []byte
	for _, record := range records {
		encoded, err := util.SerializeDiskMessage(record)
		require.NoError(t, err)
		epoch := 4 + 2 + len("orders") + 4 + 8 + 2 + 8 + 8
		old := append([]byte("CDM2"), encoded[4:epoch]...)
		old = append(old, encoded[epoch+8:len(encoded)-8]...)
		data = binary.BigEndian.AppendUint32(data, uint32(len(old)))
		data = append(data, old...)
	}
	require.NoError(t, os.WriteFile(legacy.GetSegmentPath(0), data, 0o600))
	reopened, err := NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.NoError(t, current.WriteBatchSync(records))
	manifestBefore, err := current.RetainedRangeDigest(0, 3)
	require.NoError(t, err)
	lineageBefore, err := current.LineageCandidates(3)
	require.NoError(t, err)
	old, err := reopened.RetainedPrefixContext(context.Background(), 0, 3)
	require.NoError(t, err)
	proof, err := current.RetainedPrefixContext(context.Background(), 0, 3)
	require.NoError(t, err)
	require.Equal(t, old.Proof, proof.Proof)
	manifestAfter, err := current.RetainedRangeDigest(0, 3)
	require.NoError(t, err)
	lineageAfter, err := current.LineageCandidates(3)
	require.NoError(t, err)
	require.Equal(t, manifestBefore, manifestAfter)
	require.Equal(t, lineageBefore, lineageAfter)
	require.NotEqual(t, manifestAfter, proof.Proof.Digest)
}

func TestRetainedPrefixRetentionAndHistoricalCompaction(t *testing.T) {
	d, cfg := bootstrapTestHandler(t, 1)
	d.SetCleanupPolicy(config.CleanupPolicyCompact)
	for _, payload := range []string{"old", "new"} {
		_, err := d.AppendMessageSync("orders", 1, &types.Message{Key: "same", Payload: payload})
		require.NoError(t, err)
	}
	require.NoError(t, d.RollSegmentAt(2))
	before, err := d.RetainedPrefixContext(context.Background(), 0, 2)
	require.NoError(t, err)
	result, err := d.EnforceCompaction()
	require.NoError(t, err)
	require.EqualValues(t, 1, result.RecordsRemoved)
	d.SetCleanupPolicy(config.CleanupPolicyDelete)
	compacted, err := d.RetainedPrefixContext(context.Background(), 0, 2)
	require.NoError(t, err)
	require.True(t, compacted.UsedMarkedAuthority, "authority belongs to the segment, not current cleanup policy")
	require.NotEqual(t, before.ContentGeneration, compacted.ContentGeneration)
	require.NotEqual(t, before.Proof, compacted.Proof)
	d.SetRetentionPolicy(0, 1)
	d.EnforceRetention(cfg)
	require.Equal(t, uint64(2), d.GetFirstOffset())
	_, err = d.RetainedPrefixContext(context.Background(), 0, 2)
	require.Error(t, err)
	reanchored, err := d.RetainedPrefixContext(context.Background(), 2, 2)
	require.NoError(t, err)
	require.NotEqual(t, compacted.ContentGeneration, reanchored.ContentGeneration)
	require.Equal(t, uint64(2), reanchored.Proof.Start)
}

func TestRetainedPrefixByteBudgetAndContinuousAppend(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	d.SegmentSize, d.IndexSize = 1<<30, 1<<30
	records := prefixRecords(5)
	for i := range records {
		records[i].Payload = strings.Repeat("x", 600<<10)
	}
	records[2].Payload = strings.Repeat("y", 2<<20) // permitted first oversized frame
	require.NoError(t, d.WriteBatchSync(records))
	info, err := os.Stat(d.GetSegmentPath(0))
	require.NoError(t, err)
	_, err = d.RetainedPrefixContext(context.Background(), 0, 5)
	require.NoError(t, err)
	work := d.RetainedPrefixWorkSnapshot()
	require.Equal(t, uint64(5), work.DecodedFrames)
	require.Greater(t, work.BytesRead, uint64(info.Size()), "charge omitted payload header lookahead")
	require.LessOrEqual(t, work.BytesRead, uint64(info.Size())+4*5)

	// Multiple fixed-cut readers share a builder while a real append owner
	// advances and rotates the tail. Every reader must retain the same proof.
	steady, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, steady.WriteBatchSync(prefixRecords(300)))
	want, err := steady.RetainedPrefixContext(context.Background(), 0, 300)
	require.NoError(t, err)
	steady.SegmentSize = 1
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := steady.AppendMessageSync("orders", 1, &types.Message{Payload: "new"}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for reader := 0; reader < 2; reader++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				got, err := steady.RetainedPrefixContext(context.Background(), 0, 300)
				if err != nil || got != want {
					t.Errorf("fixed proof changed: %+v %v", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestRetainedPrefixIncompleteTailAndMixedAuthority(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	d.SegmentSize, d.IndexSize = 1<<30, 1<<30
	require.NoError(t, d.WriteBatchSync(prefixRecords(256)))
	f, err := os.OpenFile(d.GetSegmentPath(0), os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = f.Write([]byte{0})
	require.NoError(t, err)
	require.NoError(t, f.Close())
	for i := 0; i < 2; i++ {
		proof, err := d.RetainedPrefixContext(context.Background(), 0, 256)
		require.Error(t, err, "a chunk ending at tail cannot cache completion before EOF")
		require.Empty(t, proof.Proof.Digest)
	}
	mixed, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, mixed.WriteCompactedReplicaRange(0, 4, nil))
	_, err = mixed.AppendMessageSync("orders", 1, &types.Message{Payload: "unmarked"})
	require.NoError(t, err)
	require.NoError(t, os.Truncate(mixed.GetSegmentPath(4), 0))
	proof, err := mixed.RetainedPrefixContext(context.Background(), 0, 5)
	require.Error(t, err, "marked authority cannot authorize a hole in another segment")
	require.Empty(t, proof.Proof.Digest)
}

func TestRetainedPrefixAvailabilityAtPublication(t *testing.T) {
	for _, mode := range []string{"failure", "generation", "pending"} {
		d, _ := bootstrapTestHandler(t, 1)
		require.NoError(t, d.WriteBatchSync(prefixRecords(1)))
		injected := errors.New("sync failed")
		d.mu.Lock()
		d.syncFileFn = func(f *os.File) error {
			switch mode {
			case "failure":
				return injected
			case "generation":
				d.advanceContentGenerationLocked()
			case "pending":
				d.bootstrapPending.Store(true)
			}
			return f.Sync()
		}
		d.mu.Unlock()
		proof, err := d.RetainedPrefixContext(context.Background(), 0, 1)
		require.Error(t, err)
		require.Empty(t, proof.Proof.Digest)
		d.mu.Lock()
		d.syncFileFn = nil
		d.bootstrapPending.Store(false)
		d.mu.Unlock()
	}
}

func TestRetainedPrefixBoundedEmptySegmentTransitions(t *testing.T) {
	d, _ := bootstrapTestHandler(t, 1)
	d.mu.Lock()
	d.compactionMu.Lock()
	for i := uint64(0); i < 130; i++ {
		if i != 0 {
			require.NoError(t, os.WriteFile(d.GetSegmentPath(i), nil, 0o600))
			d.segments = append(d.segments, i)
		}
		d.compactedSegments[i] = 0
	}
	atomic.StoreUint64(&d.FlushedOffset, 130)
	d.compactionMu.Unlock()
	d.mu.Unlock()
	proof, err := d.RetainedPrefixContext(context.Background(), 0, 130)
	require.NoError(t, err)
	require.True(t, proof.UsedMarkedAuthority)
	work := d.RetainedPrefixWorkSnapshot()
	require.Equal(t, uint64(130), work.SegmentTransitions)
	require.Zero(t, work.BytesRead)
	require.Zero(t, work.DecodedFrames)
}
