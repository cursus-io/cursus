package disk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// RetainedPrefix binds canonical records and marked holes to exact retained
// bounds. Its version is independent of bootstrap manifest and lineage formats.
type RetainedPrefix struct {
	Version uint32 `json:"version"`
	Start   uint64 `json:"start"`
	End     uint64 `json:"end"`
	Digest  string `json:"digest"`
}

// RetainedPrefixResult keeps local publication authority separate from the
// transferable proof. Callers must fence publication with ContentGeneration.
type RetainedPrefixResult struct {
	Proof               RetainedPrefix
	ContentGeneration   ContentGeneration
	UsedMarkedAuthority bool
}

type retainedPrefixCheckpoint struct {
	cursor rawFrameCursor
	chain  [sha256.Size]byte
	marked bool
	// A chunk ending at the durable tail still needs EOF validation before it
	// can answer a request at that boundary without further reads.
	complete bool
}

func retainedPrefixSeed(start uint64) [sha256.Size]byte {
	data := binary.BigEndian.AppendUint64([]byte("cursus-retained-prefix-seed-v1\x00"), start)
	return sha256.Sum256(data)
}

func retainedPrefixHash(previous [sha256.Size]byte, message types.DiskMessage) ([sha256.Size]byte, uint64, error) {
	// Canonical encoding includes every persisted field, independent of the
	// original on-disk version. Changing it requires a new proof version.
	canonical, err := util.SerializeDiskMessage(message)
	if err != nil {
		return [sha256.Size]byte{}, 0, err
	}
	h := sha256.New()
	_, _ = h.Write([]byte("cursus-retained-prefix-record-v1\x00"))
	_, _ = h.Write(previous[:])
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(canonical)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(canonical)
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest, uint64(len(canonical)), nil
}

func (c retainedPrefixCheckpoint) result() RetainedPrefixResult {
	data := binary.BigEndian.AppendUint64([]byte("cursus-retained-prefix-final-v1\x00"), c.cursor.Anchor)
	data = binary.BigEndian.AppendUint64(data, c.cursor.End)
	sum := sha256.Sum256(append(data, c.chain[:]...))
	return RetainedPrefixResult{
		Proof:             RetainedPrefix{Version: 1, Start: c.cursor.Anchor, End: c.cursor.End, Digest: hex.EncodeToString(sum[:])},
		ContentGeneration: c.cursor.Generation, UsedMarkedAuthority: c.marked,
	}
}

// RetainedPrefixContext validates [start,end) from the exact durable retained
// anchor. It hashes outside storage locks and caches only bounded, process-local
// checkpoints. Completed chunks survive cancellation; unfinished chunks do not.
// Callers must not hold partition locks while building a proof.
func (d *DiskHandler) RetainedPrefixContext(ctx context.Context, start, end uint64) (RetainedPrefixResult, error) {
	if ctx == nil {
		return RetainedPrefixResult{}, fmt.Errorf("retained prefix requires context")
	}
	unlock, err := lockProofContext(ctx, &d.retainedPrefixMu)
	if err != nil {
		return RetainedPrefixResult{}, err
	}
	defer unlock()
	generation, err := d.CaptureContentGenerationContext(ctx)
	if err != nil {
		return RetainedPrefixResult{}, err
	}
	checkpoint, tail, err := d.retainedPrefixSnapshot(ctx, generation, start, end)
	if err != nil {
		return RetainedPrefixResult{}, err
	}
	finish := func() (RetainedPrefixResult, error) {
		if err := d.commitRetainedPrefix(ctx, checkpoint); err != nil {
			return RetainedPrefixResult{}, err
		}
		return checkpoint.result(), nil
	}
	if checkpoint.cursor.End == end && checkpoint.complete {
		return finish()
	}
	for {
		records := uint64(retainedPageChunkRecords)
		if end < tail && checkpoint.cursor.End < end {
			records = min(records, end-checkpoint.cursor.End)
		}
		batch, err := d.captureRetainedPrefix(ctx, checkpoint.cursor, rawFrameBudget{
			Bytes: retainedPageChunkBytes, Records: records, SegmentTransitions: retainedPageChunkTransitions,
		})
		if err != nil {
			return RetainedPrefixResult{}, err
		}
		for _, frame := range batch.Frames {
			if err := ctx.Err(); err != nil {
				return RetainedPrefixResult{}, err
			}
			if !frame.EOF {
				d.retainedPrefixWork.DecodedFrames++
			}
			next, message, included, done, err := d.validateRawFrame(checkpoint.cursor, frame, rawFramesMarkedGaps, end, batch.Tail)
			if err != nil {
				return RetainedPrefixResult{}, err
			}
			if included {
				var bytes uint64
				checkpoint.chain, bytes, err = retainedPrefixHash(checkpoint.chain, message)
				d.retainedPrefixWork.CanonicalBytes += bytes
				if err != nil {
					return RetainedPrefixResult{}, err
				}
			}
			checkpoint.cursor = next
			checkpoint.marked = checkpoint.marked || frame.Marked
			checkpoint.complete = done || next.End < batch.Tail
			if done {
				return finish()
			}
		}
		if batch.Err != nil {
			return RetainedPrefixResult{}, batch.Err
		}
		if len(batch.Frames) == 0 {
			return RetainedPrefixResult{}, fmt.Errorf("retained prefix scan made no progress")
		}
		if err := d.commitRetainedPrefix(ctx, checkpoint); err != nil {
			return RetainedPrefixResult{}, err
		}
	}
}
