package topic

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
)

// PartitionReadView is valid only within its callback. The callback must finish
// local work and detach results before network, Raft or append callbacks.
// Acquire this view BEFORE event-source cache/index locks. Never call public
// Partition read wrappers from inside a view; they acquire a new read lock.
type PartitionReadView struct {
	p          *Partition
	active     bool
	generation uint64
	offline    bool
	end        uint64
}

func (p *Partition) WithReadView(fn func(*PartitionReadView) error) error {
	p.visibilityMu.RLock()
	defer p.visibilityMu.RUnlock()
	if p.BootstrapPending() {
		return ErrBootstrapPending
	}
	return p.withReadViewLocked(false, 0, fn)
}
func (p *Partition) withReadViewLocked(offline bool, end uint64, fn func(*PartitionReadView) error) error {
	v := &PartitionReadView{p: p, active: true, generation: p.visibilityGeneration, offline: offline, end: end}
	defer func() { v.active = false }()
	return fn(v)
}
func (v *PartitionReadView) check() error {
	if v == nil || !v.active {
		return fmt.Errorf("partition read view has expired")
	}
	return nil
}
func (v *PartitionReadView) Generation() uint64 {
	if v.check() != nil {
		return 0
	}
	return v.generation
}
func (v *PartitionReadView) FirstOffset() uint64 {
	if v.check() != nil {
		return 0
	}
	return v.p.firstOffset()
}
func (v *PartitionReadView) CommittedEnd() uint64 {
	if v.check() != nil {
		return 0
	}
	if v.offline {
		return v.end
	}
	return min(v.p.GetHWM(), v.p.dh.GetFlushedOffset())
}
func (v *PartitionReadView) LastStableOffset() uint64 {
	if v.check() != nil {
		return 0
	}
	if !v.offline {
		return v.p.lastStableOffset()
	}
	p := v.p
	p.txnMarkerMu.Lock()
	defer p.txnMarkerMu.Unlock()
	return firstUnresolvedOpenOffset(v.end, p.txnRetentionFloor, p.txnOpenOffsets, p.txnMarkers, p.txnResolver)
}
func (v *PartitionReadView) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	if err := v.check(); err != nil {
		return nil, err
	}
	records, err := v.p.readMessages(offset, max)
	if v.offline {
		for i, m := range records {
			if m.Offset >= v.end {
				return records[:i], err
			}
		}
	}
	return records, err
}
func (v *PartitionReadView) ReadMessagesBounded(offset uint64, max, maxBytes int, oversized bool) ([]types.Message, int, error) {
	if err := v.check(); err != nil {
		return nil, 0, err
	}
	records, n, err := v.p.readMessagesBounded(offset, max, maxBytes, oversized)
	if v.offline {
		for i, m := range records {
			if m.Offset >= v.end {
				return records[:i], n, err
			}
		}
	}
	return records, n, err
}
func (v *PartitionReadView) ReadCommittedPage(offset uint64, max, maxBytes int, oversized bool) ([]types.Message, int, uint64, error) {
	if err := v.check(); err != nil {
		return nil, 0, offset, err
	}
	if !v.offline {
		return v.p.readCommittedPage(offset, max, maxBytes, oversized)
	}
	if offset < v.FirstOffset() {
		return nil, 0, offset, &types.OffsetOutOfRangeError{Requested: offset, Earliest: v.FirstOffset(), Latest: v.end}
	}
	return v.p.readVisibleCommittedBounded(offset, max, maxBytes, oversized, v.end, v.end)
}
func (v *PartitionReadView) ReadCommittedRange(offset, end uint64, max int) ([]types.Message, error) {
	if err := v.check(); err != nil {
		return nil, err
	}
	if !v.offline {
		return v.p.readCommittedRange(offset, end, max)
	}
	return v.p.readVisibleCommitted(offset, max, v.end, min(end, v.end))
}
func (v *PartitionReadView) OffsetRange() (PartitionOffsetRange, error) {
	if err := v.check(); err != nil {
		return PartitionOffsetRange{}, err
	}
	if v.offline {
		return PartitionOffsetRange{Earliest: v.FirstOffset(), Latest: v.end, LEO: v.end, HWM: v.end}, nil
	}
	if err := v.p.RecoveryError(); err != nil {
		return PartitionOffsetRange{}, err
	}
	if err := v.p.hwmReadinessError(); err != nil {
		return PartitionOffsetRange{}, err
	}
	return v.p.offsetRange(), nil
}
func (p *Partition) ReadMessages(offset uint64, max int) (out []types.Message, err error) {
	err = p.WithReadView(func(v *PartitionReadView) error { out, err = v.ReadMessages(offset, max); return err })
	return
}
func (p *Partition) ReadMessagesBounded(offset uint64, max, maxBytes int, oversized bool) (out []types.Message, n int, err error) {
	err = p.WithReadView(func(v *PartitionReadView) error {
		out, n, err = v.ReadMessagesBounded(offset, max, maxBytes, oversized)
		return err
	})
	return
}
func (p *Partition) ReadCommittedPage(offset uint64, max, maxBytes int, oversized bool) (out []types.Message, n int, next uint64, err error) {
	next = offset
	err = p.WithReadView(func(v *PartitionReadView) error {
		out, n, next, err = v.ReadCommittedPage(offset, max, maxBytes, oversized)
		return err
	})
	return
}
func (p *Partition) ReadCommittedRange(offset, end uint64, max int) (out []types.Message, err error) {
	err = p.WithReadView(func(v *PartitionReadView) error { out, err = v.ReadCommittedRange(offset, end, max); return err })
	return
}
func (p *Partition) readCommittedMetadata(offset uint64, max int) (out []types.Message, err error) {
	err = p.WithReadView(func(v *PartitionReadView) error { out, err = p.readCommittedMetadataUngated(offset, max); return err })
	return
}
func (p *Partition) ProducerSequenceOffset(id string, epoch int64, seq uint64) (offset uint64, found bool, err error) {
	err = p.WithReadView(func(v *PartitionReadView) error {
		offset, found, err = p.producerSequenceOffset(id, epoch, seq)
		return err
	})
	return
}
func (p *Partition) LastStableOffset() (offset uint64) {
	_ = p.WithReadView(func(v *PartitionReadView) error { offset = v.LastStableOffset(); return nil })
	return
}
func (p *Partition) ReadOffsetRange() (out PartitionOffsetRange, err error) {
	err = p.WithReadView(func(v *PartitionReadView) error { out, err = v.OffsetRange(); return err })
	return
}

// FirstOffset and OffsetRange retain the legacy non-error signatures. New
// callers should use ReadOffsetRange or a view to distinguish unavailable state.
func (p *Partition) FirstOffset() (offset uint64) {
	_ = p.WithReadView(func(v *PartitionReadView) error { offset = v.FirstOffset(); return nil })
	return
}
func (p *Partition) OffsetRange() PartitionOffsetRange {
	out, _ := p.ReadOffsetRange()
	return out
}
