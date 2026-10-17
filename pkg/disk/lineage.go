package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const lineageCheckpointInterval = 256
const lineageBackgroundRecordLimit = 1024
const lineageMagic = "CLIN0001"

// ErrLineageUnsupported routes compacted/gapped histories to compacted recovery.
var ErrLineageUnsupported = errors.New("lineage requires a contiguous retained log")

var errLineageRebuildRequired = errors.New("lineage requires an explicit proof request to rebuild")

// LineagePrefix proves the entire retained interval [StartOffset, EndOffset).
// Different retained starts are incomparable, including empty prefixes.
type LineagePrefix struct {
	StartOffset uint64 `json:"start_offset"`
	EndOffset   uint64 `json:"end_offset"`
	Digest      string `json:"digest"`
}

// LineageEpochSpan describes decoded records, not proof of common ancestry.
// LeaderEpoch zero is unknown history and must never establish a shared epoch.
type LineageEpochSpan struct {
	StartOffset uint64
	EndOffset   uint64
	LeaderEpoch int64
}

type lineageCursor struct {
	EndOffset uint64
	Segment   uint64
	Position  int64
	Digest    [sha256.Size]byte
}

type lineageIndex struct {
	start       uint64
	tail        lineageCursor
	checkpoints []lineageCursor
	epochs      []LineageEpochSpan
	info        os.FileInfo
}

// The fixed tail slot is overwritten after appending sparse checkpoints and
// completed epoch spans. Every record is independently checksummed. A crash can
// tear either write: the first proof request after restart rebuilds from the
// authoritative retained log, never trusting an old version or stale cursor.
// Checkpoints are spaced by 256 records OR the byte budget, with a constant-size
// tail slot. A large record can therefore create a checkpoint by itself.
type lineageJournalRecord struct {
	Kind        uint64 // 1: tail, 2: checkpoint, 3: completed epoch span
	StartOffset uint64
	EndOffset   uint64
	LeaderEpoch int64
	EpochStart  uint64
	Segment     uint64
	Position    int64
	Digest      [sha256.Size]byte
}

// lineageRecordBytes binds each journal record checksum to the format version.
func lineageRecordBytes(record lineageJournalRecord) []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, record)
	sum := sha256.Sum256(append([]byte(lineageMagic), b.Bytes()...))
	return append(b.Bytes(), sum[:]...)
}

// lineageSeed anchors even an empty proof to its explicit retained start.
func lineageSeed(start uint64) [sha256.Size]byte {
	var offset [8]byte
	binary.BigEndian.PutUint64(offset[:], start)
	return sha256.Sum256(append([]byte("cursus-retained-lineage-v1"), offset[:]...))
}

// lineageHash extends the chain with a length-delimited canonical full record.
func lineageHash(previous [sha256.Size]byte, message types.DiskMessage) ([sha256.Size]byte, error) {
	// Decode first, then encode every persisted field in one canonical version.
	// CDM2/CDM3/CDM4 and re-encoded CDM5 records therefore compare identically. If the
	// canonical encoding changes, bump the lineage format/domain version too.
	canonical, err := util.SerializeDiskMessage(message)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	h := sha256.New()
	_, _ = h.Write(previous[:])
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(canonical)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(canonical)
	metrics.RecordRecoveryBytes(metrics.RecoveryLineage, metrics.RecoveryCanonicalHashBytes, int64(len(previous)+len(length)+len(canonical)))
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

// LineageCandidates returns at most 64 proofs, including the exact requested
// tail and retained-start anchor. Historical scans are bounded by bytes too.
func (d *DiskHandler) LineageCandidates(tail uint64) ([]LineagePrefix, error) {
	return d.LineageCandidatesContext(context.Background(), tail)
}

// LineageCandidatesContext permits cancellation while queued or between bounded
// chunks. Successfully committed chunks remain available to the next request.
func (d *DiskHandler) LineageCandidatesContext(ctx context.Context, tail uint64) ([]LineagePrefix, error) {
	unlock, err := d.lockLineageBuilder(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := d.prepareLineage(ctx, false); err != nil {
		return nil, err
	}
	index := d.lineageProgress.index
	if tail < index.start || tail > index.tail.EndOffset {
		return nil, fmt.Errorf("lineage tail %d outside retained range [%d,%d]", tail, index.start, index.tail.EndOffset)
	}
	ends := lineageCandidateEnds(index.start, tail)
	proofs := make([]LineagePrefix, 0, len(ends))
	for _, end := range ends {
		proof, err := d.lineagePrefix(ctx, end)
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, proof)
	}
	if err := d.checkLineageGeneration(ctx); err != nil {
		return nil, err
	}
	return proofs, nil
}

// lineageCandidateEnds selects the exact tail, tail-relative rollback distances
// 1, 3, 7, ... and the retained-start anchor for start <= tail. Reserve the last
// slot for the anchor, keeping all uint64 spans within the 64-proof wire limit.
func lineageCandidateEnds(start, tail uint64) []uint64 {
	ends := []uint64{tail}
	span := tail - start
	for distance := uint64(1); distance < span && len(ends) < 63; distance = 2*distance + 1 {
		ends = append(ends, tail-distance)
	}
	if tail != start {
		ends = append(ends, start)
	}
	return ends
}

// VerifyLineagePrefix compares an explicit retained prefix only below the
// caller's authoritative, exclusive committed HWM. Epoch matches alone are
// never used as evidence; a matching final record cannot hide earlier divergence.
func (d *DiskHandler) VerifyLineagePrefix(prefix LineagePrefix, committedHWM uint64) (bool, error) {
	return d.VerifyLineagePrefixContext(context.Background(), prefix, committedHWM)
}

func (d *DiskHandler) VerifyLineagePrefixContext(ctx context.Context, prefix LineagePrefix, committedHWM uint64) (bool, error) {
	if ctx == nil {
		return false, errors.New("lineage requires context")
	}
	if prefix.EndOffset < prefix.StartOffset || prefix.EndOffset > committedHWM {
		return false, nil
	}
	unlock, err := d.lockLineageBuilder(ctx)
	if err != nil {
		return false, err
	}
	defer unlock()
	if err := d.prepareLineage(ctx, false); err != nil {
		return false, err
	}
	index := d.lineageProgress.index
	if prefix.StartOffset != index.start || prefix.EndOffset > index.tail.EndOffset {
		return false, nil
	}
	want, err := d.lineagePrefix(ctx, prefix.EndOffset)
	if err != nil {
		return false, err
	}
	if err := d.checkLineageGeneration(ctx); err != nil {
		return false, err
	}
	return want == prefix, nil
}

// MatchLineageCandidates returns the longest comparable proof below committedHWM.
func (d *DiskHandler) MatchLineageCandidates(candidates []LineagePrefix, committedHWM uint64) (*LineagePrefix, error) {
	return d.MatchLineageCandidatesContext(context.Background(), candidates, committedHWM)
}

func (d *DiskHandler) MatchLineageCandidatesContext(ctx context.Context, candidates []LineagePrefix, committedHWM uint64) (*LineagePrefix, error) {
	if len(candidates) > 64 {
		return nil, errors.New("too many lineage candidates")
	}
	unlock, err := d.lockLineageBuilder(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := d.prepareLineage(ctx, false); err != nil {
		return nil, err
	}
	index := d.lineageProgress.index
	ordered := append([]LineagePrefix(nil), candidates...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].EndOffset > ordered[j].EndOffset })
	for _, candidate := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if candidate.StartOffset != index.start || candidate.EndOffset < candidate.StartOffset || candidate.EndOffset > committedHWM || candidate.EndOffset > index.tail.EndOffset {
			continue
		}
		proof, err := d.lineagePrefix(ctx, candidate.EndOffset)
		if err != nil {
			return nil, err
		}
		if proof == candidate {
			if err := d.checkLineageGeneration(ctx); err != nil {
				return nil, err
			}
			return &proof, nil
		}
	}
	if err := d.checkLineageGeneration(ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

// LineageEpochs returns a detached copy of the complete durable epoch spans.
func (d *DiskHandler) LineageEpochs() ([]LineageEpochSpan, error) {
	return d.LineageEpochsContext(context.Background())
}

// LineageEpochsContext builds the durable index with the same cancellable gate
// as proofs, then returns spans detached from all mutable builder state.
func (d *DiskHandler) LineageEpochsContext(ctx context.Context) ([]LineageEpochSpan, error) {
	unlock, err := d.lockLineageBuilder(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := d.prepareLineage(ctx, false); err != nil {
		return nil, err
	}
	result := append([]LineageEpochSpan(nil), d.lineageProgress.index.epochs...)
	if err := d.checkLineageGeneration(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

// lineageTailRecord builds the exact tail state shared by publication and
// validation. The caller owns the builder gate and its mutable derived index.
func lineageTailRecord(index *lineageIndex) lineageJournalRecord {
	tail := lineageJournalRecord{Kind: 1, StartOffset: index.start, EndOffset: index.tail.EndOffset, Segment: index.tail.Segment, Position: index.tail.Position, Digest: index.tail.Digest}
	if len(index.epochs) != 0 {
		span := index.epochs[len(index.epochs)-1]
		tail.LeaderEpoch, tail.EpochStart = span.LeaderEpoch, span.StartOffset
	}
	return tail
}

// lineageHeaderMatches reads only the 128-byte magic and checksummed tail slot.
// Comparing the complete expected encoding detects stale valid tails as well as
// corrupt magic/checksums even when size, identity and modification time match.
func lineageHeaderMatches(path string, index *lineageIndex) bool {
	// #nosec G304 -- path is the partition-owned derived index.
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	expected := append([]byte(lineageMagic), lineageRecordBytes(lineageTailRecord(index))...)
	actual := make([]byte, len(expected))
	n, err := io.ReadFull(f, actual)
	metrics.RecordRecoveryBytes(metrics.RecoveryLineage, metrics.RecoveryReadBytes, int64(n))
	if err != nil {
		return false
	}
	return bytes.Equal(actual, expected)
}

// publishLineage appends sparse events and replaces the constant-size tail slot,
// syncing before updating the cache's file identity. The caller owns lineageMu;
// storage locks are released throughout journal writes and filesystem syncs.
func (d *DiskHandler) publishLineage(ctx context.Context, path string, index *lineageIndex, fresh bool, events []lineageJournalRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	flags := os.O_CREATE | os.O_RDWR
	if fresh {
		flags |= os.O_TRUNC
	}
	// #nosec G304 -- path is the partition-owned derived index.
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	encodedTail := lineageRecordBytes(lineageTailRecord(index))
	write := func(data []byte) error {
		n, err := f.Write(data)
		metrics.RecordRecoveryBytes(metrics.RecoveryLineage, metrics.RecoveryWriteBytes, int64(n))
		return err
	}
	if fresh {
		if err := write(append([]byte(lineageMagic), make([]byte, len(encodedTail))...)); err != nil {
			return err
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := write(lineageRecordBytes(event)); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := f.WriteAt(encodedTail, int64(len(lineageMagic)))
	metrics.RecordRecoveryBytes(metrics.RecoveryLineage, metrics.RecoveryWriteBytes, int64(n))
	if err != nil {
		return err
	}
	syncFn := d.lineageSyncFn
	if syncFn == nil {
		syncFn = (*os.File).Sync
	}
	metrics.RecordRecoveryWork(metrics.RecoveryLineage, metrics.RecoverySyncCalls, 1)
	if err := syncFn(f); err != nil {
		return err
	}
	if fresh {
		metrics.RecordRecoveryWork(metrics.RecoveryLineage, metrics.RecoverySyncCalls, 1)
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return err
		}
	}
	// Windows may finalize last-write metadata only when the write handle closes.
	// Capture it afterwards so normal appends do not look like sidecar damage.
	if err := f.Close(); err != nil {
		return err
	}
	index.info, err = os.Stat(path)
	return err
}
