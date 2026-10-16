package disk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/types"
)

// ScanRetainedRange certifies a durable range, including explicitly marked
// compacted holes. It intentionally permits pending bootstrap storage. Visitors
// must only accumulate detached state: do not reenter storage, take foreign
// locks, or perform network/Raft work. Memory is bounded by one decoded record.
func (d *DiskHandler) ScanRetainedRange(ctx context.Context, start, end uint64, visit func(types.Message) error) error {
	if ctx == nil || visit == nil {
		return fmt.Errorf("retained scan requires context and visitor")
	}
	unlock, err := lockProofContext(ctx, &d.maintenanceMu, &d.appendMu, &d.mu, &d.ioMu)
	if err != nil {
		return err
	}
	defer unlock()
	_, err = d.scanRetainedRangeLocked(ctx, start, end, func(m types.DiskMessage) error { return visit(retainedMessage(m)) })
	return err
}

// Caller owns maintenanceMu, appendMu, mu and ioMu. Validate every physical
// frame of each intersecting segment, even outside the requested subrange.
func (d *DiskHandler) scanRetainedRangeLocked(ctx context.Context, start, end uint64, visit func(types.DiskMessage) error) (bool, error) {
	result, err := d.scanRetainedFramesLocked(ctx, start, end, false, func(frame compactionFrame) (bool, error) {
		return true, visit(frame.message)
	})
	return result.compacted, err
}

// A private successful stop is distinct from all visitor errors. Only the
// bounded scanner creates it, after validating the first omitted frame and the
// gap preceding it. Full retained scans always validate intersecting files to EOF.
var errRetainedPageStop = errors.New("certified retained page stop")

type retainedScanResult struct {
	end                              uint64
	exhausted                        bool
	compacted                        bool
	decodedFrames, decodedFrameBytes uint64
}

func (d *DiskHandler) scanRetainedFramesLocked(ctx context.Context, start, end uint64, bounded bool, visit func(compactionFrame) (bool, error)) (retainedScanResult, error) {
	result := retainedScanResult{end: start}
	tail := atomic.LoadUint64(&d.FlushedOffset)
	if start > end || end > tail || len(d.segments) == 0 || start < d.segments[0] {
		return result, fmt.Errorf("retained scan outside durable bounds")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if bounded && start == end {
		result.exhausted = true
		return result, nil
	}
	for i, base := range d.segments {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		logicalEnd := tail
		if i+1 < len(d.segments) {
			logicalEnd = d.segments[i+1]
		}
		if base >= end {
			break
		}
		if logicalEnd <= start {
			continue
		}
		path := d.GetSegmentPath(base)
		info, err := os.Stat(path)
		if err != nil {
			return result, err
		}
		compacted := d.segmentAllowsOffsetGaps(base, info.Size())
		result.compacted = result.compacted || compacted
		next := base
		err = scanCompactionSegment(path, base, false, compacted, func(frame compactionFrame) error {
			result.decodedFrames++
			result.decodedFrameBytes += uint64(frame.size)
			if err := ctx.Err(); err != nil {
				return err
			}
			m := frame.message
			if m.Topic != d.topicName || int(m.Partition) != d.partitionID {
				return fmt.Errorf("retained record identity mismatch")
			}
			if m.Offset < base || m.Offset >= logicalEnd {
				return fmt.Errorf("record outside physical segment bounds")
			}
			if !compacted && m.Offset != next {
				return fmt.Errorf("unmarked retained offset gap")
			}
			next = m.Offset + 1
			if bounded && m.Offset >= end {
				result.end, result.exhausted = end, true
				return errRetainedPageStop
			}
			if m.Offset >= start && m.Offset < end {
				included, err := visit(frame)
				if err != nil {
					return err
				}
				if bounded && !included {
					result.end = m.Offset
					return errRetainedPageStop
				}
				// A fixed prefix is certified by its included end-1 record;
				// do not decode an unrelated future frame. At the durable
				// tail, still require clean physical EOF (including headers).
				if bounded && end < tail && next == end {
					result.end, result.exhausted = end, true
					return errRetainedPageStop
				}
			}
			return nil
		})
		if bounded && errors.Is(err, errRetainedPageStop) {
			return result, nil
		}
		if err != nil {
			return result, err
		}
		if !compacted && next != logicalEnd {
			return result, fmt.Errorf("unmarked retained trailing offset gap")
		}
	}
	result.end, result.exhausted = end, true
	return result, nil
}

func retainedMessage(m types.DiskMessage) types.Message {
	return types.Message{
		Topic: m.Topic, Partition: int(m.Partition),
		Offset:                       m.Offset,
		Payload:                      m.Payload,
		ProducerID:                   m.ProducerID,
		SeqNum:                       m.SeqNum,
		Epoch:                        m.Epoch,
		LeaderEpoch:                  m.LeaderEpoch,
		Key:                          m.Key,
		EventType:                    m.EventType,
		SchemaVersion:                m.SchemaVersion,
		AggregateVersion:             m.AggregateVersion,
		Metadata:                     m.Metadata,
		EventID:                      m.EventID,
		PayloadDigest:                m.PayloadDigest,
		TransactionalID:              m.TransactionalID,
		TransactionState:             m.TransactionState,
		TransactionMarker:            m.TransactionMarker,
		ControlBatchType:             m.ControlBatchType,
		ControlBatchVersion:          m.ControlBatchVersion,
		ControlBatchCoordinatorEpoch: m.ControlBatchCoordinatorEpoch,
		ControlBatchKey:              m.ControlBatchKey,
		ControlBatchValue:            m.ControlBatchValue,
	}
}
