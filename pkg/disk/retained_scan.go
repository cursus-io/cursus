package disk

import (
	"context"
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
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	d.appendMu.Lock()
	defer d.appendMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	_, err := d.scanRetainedRangeLocked(ctx, start, end, func(m types.DiskMessage) error { return visit(retainedMessage(m)) })
	return err
}

// Caller owns maintenanceMu, appendMu, mu and ioMu. Validate every physical
// frame of each intersecting segment, even outside the requested subrange.
func (d *DiskHandler) scanRetainedRangeLocked(ctx context.Context, start, end uint64, visit func(types.DiskMessage) error) (bool, error) {
	tail := atomic.LoadUint64(&d.FlushedOffset)
	if start > end || end > tail || len(d.segments) == 0 || start < d.segments[0] {
		return false, fmt.Errorf("retained scan outside durable bounds")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	compactedRange := false
	for i, base := range d.segments {
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
			return false, err
		}
		compacted := d.segmentAllowsOffsetGaps(base, info.Size())
		compactedRange = compactedRange || compacted
		next := base
		err = scanCompactionSegment(path, base, false, compacted, func(frame compactionFrame) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			m := frame.message
			if m.Offset < base || m.Offset >= logicalEnd {
				return fmt.Errorf("record outside physical segment bounds")
			}
			if !compacted && m.Offset != next {
				return fmt.Errorf("unmarked retained offset gap")
			}
			next = m.Offset + 1
			if m.Offset >= start && m.Offset < end {
				return visit(m)
			}
			return nil
		})
		if err != nil {
			return false, err
		}
		if !compacted && next != logicalEnd {
			return false, fmt.Errorf("unmarked retained trailing offset gap")
		}
	}
	return compactedRange, nil
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
