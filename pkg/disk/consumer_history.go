package disk

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

// ConsumerHistorySegment is a local, nonserializable closed-segment witness.
// It is captured BEFORE certification; a later generation can never authorize
// installation of a plan prepared against an earlier prefix.
type ConsumerHistorySegment struct {
	owner      *DiskHandler
	generation ContentGeneration
	first, end uint64
	empty      bool
}

func (s ConsumerHistorySegment) Range() (uint64, uint64) { return s.first, s.end }

func (s ConsumerHistorySegment) Empty() bool { return s.empty }

func (d *DiskHandler) ConsumerHistorySegments(ctx context.Context, committed uint64) ([]ConsumerHistorySegment, error) {
	if !d.internalMetadata || d.topicName != config.ConsumerOffsetsTopicName {
		return nil, fmt.Errorf("consumer history requires offsets storage")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Internal generic compaction returns before invoking a partition gate;
	// unlike application compaction it cannot invert visibility/maintenance.
	unlock, err := lockProofContext(ctx, &d.maintenanceMu)
	if err != nil {
		return nil, err
	}
	defer unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.writeAvailabilityError(); err != nil {
		return nil, err
	}
	segments := append([]uint64(nil), d.segments...)
	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })
	var result []ConsumerHistorySegment
	for i := 0; i+1 < len(segments); i++ {
		if segments[i] != d.CurrentSegment && segments[i+1] <= committed {
			info, err := os.Stat(d.GetSegmentPath(segments[i]))
			if err != nil {
				return nil, err
			}
			// The first empty segment is a retained-start anchor, not work.
			if i == 0 && info.Size() == 0 {
				continue
			}
			result = append(result, ConsumerHistorySegment{d, d.contentGenerationLocked(), segments[i], segments[i+1], info.Size() == 0})
		}
	}
	return result, nil
}

// RewriteConsumerHistory accepts only exact ordinary records from a semantic
// certificate. The controller owns cluster activation, compatibility and commit
// fences; the partition owns visibility exclusion. The ordinary key compactor
// remains forbidden on distributed internal metadata.
func (d *DiskHandler) RewriteConsumerHistory(ctx context.Context, segment ConsumerHistorySegment, removed []types.Message) (result CompactionResult, err error) {
	if segment.owner != d || segment.first >= segment.end {
		return result, ErrStaleContentGeneration
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	unlock, err := lockProofContext(ctx, &d.maintenanceMu)
	if err != nil {
		return result, err
	}
	defer unlock()
	if err = d.CheckContentGenerationContext(ctx, segment.generation); err != nil {
		return result, err
	}
	if atomic.LoadInt32(&d.activeReaders) > 0 {
		return result, errCompactionReadersActive
	}
	remove := make(map[uint64]types.Message, len(removed))
	for _, m := range removed {
		if m.Offset < segment.first || m.Offset >= segment.end || m.Key == "" {
			return result, fmt.Errorf("invalid history removal range")
		}
		if _, exists := remove[m.Offset]; exists {
			return result, fmt.Errorf("duplicate history removal")
		}
		remove[m.Offset] = m
	}
	info, err := os.Stat(d.GetSegmentPath(segment.first))
	if err != nil {
		return result, err
	}
	if len(remove) == 0 {
		result.SkippedReason = "no_superseded_records"
		if info.Size() == 0 {
			err = d.pruneEmptyConsumerHistorySegment(segment.first, nil)
		}
		return result, err
	}
	gaps := d.segmentAllowsOffsetGaps(segment.first, info.Size())
	matched := 0
	err = scanCompactionSegmentContext(ctx, d.GetSegmentPath(segment.first), segment.first, false, gaps, func(frame compactionFrame) error {
		result.RecordsScanned++
		if m, ok := remove[frame.message.Offset]; ok {
			original := retainedMessage(frame.message)
			// Ordinary ReadMessages omits transport topic/partition fields. The
			// unforgeable segment witness already binds the physical handler.
			original.Topic, original.Partition = "", 0
			m.Topic, m.Partition = "", 0
			if hasTransactionMetadata(frame.message) || !reflect.DeepEqual(original, m) {
				return fmt.Errorf("history removal does not match original ordinary frame")
			}
			matched++
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	if matched != len(remove) {
		return result, fmt.Errorf("history removal references missing records")
	}
	result.SegmentsScanned = 1
	result.RecordsRemoved, result.BytesBefore, result.BytesAfter, err = d.rewriteClosedSegment(segment.first, gaps, func(m types.DiskMessage) bool { _, ok := remove[m.Offset]; return ok }, func(stage string) error {
		return consumerHistoryRewriteStage(ctx, stage)
	})
	if result.RecordsRemoved > 0 {
		result.SegmentsRewritten = 1
	}
	if err == nil && result.BytesAfter == 0 {
		err = d.pruneEmptyConsumerHistorySegment(segment.first, nil)
	}
	return result, err
}

// Cancellation is safe only while replacement files are still private. Once
// generation invalidation starts authoritative publication, finish all durable
// stages. Real storage failures still fence writes in rewriteClosedSegment.
// A maintenance deadline must not leave a healthy handler unavailable until
// restart, or publish a log without its matching index.
func consumerHistoryRewriteStage(ctx context.Context, stage string) error {
	switch stage {
	case "written", "log_synced", "index_synced", "marker_synced":
		return ctx.Err()
	default:
		return nil
	}
}

// Keep the first segment as the logical start anchor. Empty interior segments
// carry no state; deleting them bounds file count as well as retained bytes.
// The existing durable deletion protocol is restartable at every rename.
func (d *DiskHandler) pruneEmptyConsumerHistorySegment(segment uint64, afterStage func(string) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.segments) == 0 || segment == d.segments[0] || segment == d.CurrentSegment {
		return nil
	}
	info, err := os.Stat(d.GetSegmentPath(segment))
	if err != nil {
		return err
	}
	if info.Size() != 0 {
		return fmt.Errorf("refusing to prune nonempty history segment")
	}
	// Removing an interior base extends the preceding segment's logical end.
	// Publish its gap evidence before deleting this empty segment, otherwise
	// strict startup/bootstrap scanning would see an unmarked trailing gap.
	index := sort.Search(len(d.segments), func(i int) bool { return d.segments[i] >= segment })
	if index == 0 || index >= len(d.segments) || d.segments[index] != segment {
		return ErrStaleContentGeneration
	}
	previous := d.segments[index-1]
	previousPath := d.GetSegmentPath(previous)
	previousInfo, err := os.Stat(previousPath)
	if err != nil {
		return err
	}
	if !d.segmentAllowsOffsetGaps(previous, previousInfo.Size()) {
		next := previous
		if err := scanCompactionSegment(previousPath, previous, false, false, func(frame compactionFrame) error {
			next = frame.message.Offset + 1
			return nil
		}); err != nil {
			return err
		}
		if next != segment {
			return fmt.Errorf("refusing to certify preexisting trailing gap")
		}
		if err := d.advanceContentGenerationLocked(); err != nil {
			return err
		}
		if err := writeCompactionMarker(previousPath, previousInfo.Size()); err != nil {
			return d.markWriteUnavailable(err)
		}
		d.recordCompactedSegment(previous, previousInfo.Size())
	}
	if afterStage != nil {
		if err := afterStage("predecessor_gap_synced"); err != nil {
			return err
		}
	}
	if err := d.markAsDeletedLocked(d.GetSegmentPath(segment)); err != nil {
		return err
	}
	if afterStage != nil {
		return afterStage("empty_segment_deleted")
	}
	return nil
}
