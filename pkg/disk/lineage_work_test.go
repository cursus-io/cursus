package disk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

// TestLineageEndpointWork verifies actual read/hash work against the union of
// required records, independently of RPC counts or the endpoint cache layout.
func TestLineageEndpointWork(t *testing.T) {
	d, _ := newLineageHandler(t)
	const count = 4097
	batch := make([]types.DiskMessage, count)
	frames := make([]uint64, count)
	digests := make([]string, count+1)
	chain := lineageSeed(0)
	digests[0] = hex.EncodeToString(chain[:])
	var totalBytes uint64
	for i := range batch {
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), LeaderEpoch: 7, Payload: strings.Repeat("x", 8192)}
		encoded, err := util.SerializeDiskMessage(batch[i])
		require.NoError(t, err)
		frames[i] = uint64(len(encoded) + 4)
		totalBytes += frames[i]
		// Reference chain does not call lineageHash or consult cached cursors.
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(encoded)))
		input := append(append(append([]byte{}, chain[:]...), size[:]...), encoded...)
		chain = sha256.Sum256(input)
		digests[i+1] = hex.EncodeToString(chain[:])
	}
	require.NoError(t, d.WriteBatchSync(batch))
	_, err := d.MatchLineageCandidatesContext(context.Background(), nil, count)
	require.NoError(t, err)
	read, hashed, _ := lineageWorkSnapshot(d)
	t.Logf("cold records=%d payload_frame_bytes=%d read_bytes=%d hashed_records=%d", count, totalBytes, read, hashed)
	require.Equal(t, uint64(count), hashed, "cold reconstruction hashes the retained log once")

	t.Run("increasing-prefixes", func(t *testing.T) {
		beforeRead, beforeHash, _ := lineageWorkSnapshot(d)
		var requiredBytes uint64
		for end := uint64(1); end <= 32; end++ {
			proof, err := d.LineagePrefixContext(context.Background(), end)
			require.NoError(t, err)
			require.Equal(t, digests[end], proof.Digest)
			requiredBytes += frames[end-1]
		}
		read, hashed, _ := lineageWorkSnapshot(d)
		t.Logf("required_bytes=%d read_bytes=%d hashed_records=%d", requiredBytes, read-beforeRead, hashed-beforeHash)
		require.Equal(t, uint64(32), hashed-beforeHash, "validated increasing prefixes must not rehash overlap")
		require.LessOrEqual(t, read-beforeRead, requiredBytes+4*32, "endpoint capture must not read unused later records")
	})

	t.Run("append-preserves-certified-prefix", func(t *testing.T) {
		appendLineageRecords(t, d, "new suffix")
		_, err := d.MatchLineageCandidatesContext(context.Background(), nil, count+1)
		require.NoError(t, err)
		beforeRead, beforeHash, _ := lineageWorkSnapshot(d)
		var requiredBytes uint64
		for end := uint64(33); end <= 64; end++ {
			proof, err := d.LineagePrefixContext(context.Background(), end)
			require.NoError(t, err)
			require.Equal(t, digests[end], proof.Digest)
			requiredBytes += frames[end-1]
		}
		read, hashed, _ := lineageWorkSnapshot(d)
		require.Equal(t, uint64(32), hashed-beforeHash)
		require.LessOrEqual(t, read-beforeRead, requiredBytes+4*32)
	})

	t.Run("sparse-candidates", func(t *testing.T) {
		d.lineageMu.Lock()
		clear(d.lineageProgress.endpoints)
		checkpoints := append([]lineageCursor(nil), d.lineageProgress.index.checkpoints...)
		d.lineageMu.Unlock()
		const tail = uint64(count - 7)
		ends := lineageCandidateEnds(0, tail)
		needed := make(map[uint64]bool)
		for _, end := range ends {
			var start uint64
			for _, checkpoint := range checkpoints {
				if checkpoint.EndOffset > end {
					break
				}
				start = checkpoint.EndOffset
			}
			for offset := start; offset < end; offset++ {
				needed[offset] = true
			}
		}
		var requiredBytes uint64
		for offset := range needed {
			requiredBytes += frames[offset]
		}
		beforeRead, beforeHash, _ := lineageWorkSnapshot(d)
		proofs, err := d.LineageCandidates(tail)
		require.NoError(t, err)
		require.Len(t, proofs, len(ends))
		for i, proof := range proofs {
			require.Equal(t, ends[i], proof.EndOffset, "wire order must remain descending")
			require.Equal(t, digests[ends[i]], proof.Digest)
		}
		read, hashed, _ := lineageWorkSnapshot(d)
		t.Logf("required_records=%d required_bytes=%d read_bytes=%d hashed_records=%d", len(needed), requiredBytes, read-beforeRead, hashed-beforeHash)
		require.Equal(t, uint64(len(needed)), hashed-beforeHash, "candidate scans must reuse certified overlap")
		require.LessOrEqual(t, read-beforeRead, requiredBytes+4*uint64(len(ends)))
		_, err = d.LineageCandidates(tail)
		require.NoError(t, err)
		repeatedRead, repeatedHash, _ := lineageWorkSnapshot(d)
		require.Equal(t, read, repeatedRead)
		require.Equal(t, hashed, repeatedHash)
	})

	t.Run("bounded-cache-eviction", func(t *testing.T) {
		for end := uint64(1); end <= lineageEndpointCacheLimit+32; end++ {
			proof, err := d.LineagePrefixContext(context.Background(), end)
			require.NoError(t, err)
			require.Equal(t, digests[end], proof.Digest)
			_, _, entries := lineageWorkSnapshot(d)
			require.LessOrEqual(t, entries, lineageEndpointCacheLimit)
		}
		proof, err := d.LineagePrefixContext(context.Background(), 1)
		require.NoError(t, err)
		require.Equal(t, digests[1], proof.Digest, "evicted endpoints must reconstruct correctly")
	})
}

func lineageWorkSnapshot(d *DiskHandler) (read, hashed uint64, entries int) {
	d.lineageMu.Lock()
	defer d.lineageMu.Unlock()
	p := d.lineageProgress
	return p.bytesRead, p.recordsHashed, len(p.endpoints)
}
