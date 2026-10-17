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

// ValidateIdentity checks callback lifetime and the partition whose exclusion
// protects a caller's derived caches, without reading or advancing any tail.
func (v *PartitionReadView) ValidateIdentity(name string, partition int) error {
	if err := v.check(); err != nil {
		return err
	}
	if v.p.topic != name || v.p.id != partition {
		return fmt.Errorf("partition read view identity mismatch")
	}
	return nil
}

// ValidateBootstrapSource requires the exclusive source callback and its lifecycle.
func (v *PartitionReadView) ValidateBootstrapSource(name string, partition int, lifecycle uint64) error {
	return v.validateBootstrap(name, partition, lifecycle, false)
}

// ValidateBootstrapRecovery requires the exclusive pending recovery callback.
func (v *PartitionReadView) ValidateBootstrapRecovery(name string, partition int, lifecycle uint64) error {
	return v.validateBootstrap(name, partition, lifecycle, true)
}

func (v *PartitionReadView) validateBootstrap(name string, partition int, lifecycle uint64, pending bool) error {
	if err := v.ValidateIdentity(name, partition); err != nil {
		return err
	}
	if !v.offline || v.p.BootstrapPending() != pending {
		return fmt.Errorf("bootstrap operation requires its exclusive callback view")
	}
	v.p.recoveryMu.RLock()
	defer v.p.recoveryMu.RUnlock()
	if v.p.bootstrapDefinitionKnown && v.p.bootstrapLifecycle != lifecycle {
		return fmt.Errorf("bootstrap view lifecycle mismatch")
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

// ReadCommittedMetadata scans the full committed boundary, including metadata
// after an independent unresolved transaction. Unlike ReadCommittedPage it is
// not capped at LSO. Transaction records still require committed visibility.
func (v *PartitionReadView) ReadCommittedMetadata(offset uint64, max int) ([]types.Message, error) {
	if err := v.check(); err != nil {
		return nil, err
	}
	return v.ReadCommittedMetadataThrough(offset, v.CommittedEnd(), max)
}

// ReadCommittedMetadataThrough freezes BOTH the scan and transaction-marker
// visibility boundary. Repeated pages can use one captured end even if normal
// HWM advances; a marker beyond that end cannot make an earlier record visible.
func (v *PartitionReadView) ReadCommittedMetadataThrough(offset, end uint64, max int) ([]types.Message, error) {
	if err := v.check(); err != nil {
		return nil, err
	}
	if !v.offline {
		if err := v.p.RecoveryError(); err != nil {
			return nil, err
		}
		if err := v.p.hwmReadinessError(); err != nil {
			return nil, err
		}
	}
	if end > v.CommittedEnd() {
		return nil, fmt.Errorf("metadata end exceeds committed read view")
	}
	if offset < v.FirstOffset() {
		return nil, &types.OffsetOutOfRangeError{Requested: offset, Earliest: v.FirstOffset(), Latest: end}
	}
	if max <= 0 {
		return nil, nil
	}
	v.p.txnMarkerMu.RLock()
	defer v.p.txnMarkerMu.RUnlock()
	messages, _, _, err := v.p.readCommittedScanRangeBounded(offset, end, end, max, 0, true, v.p.txnMarkers, v.p.txnResolver)
	return messages, err
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
	err = p.WithReadView(func(v *PartitionReadView) error { out, err = v.ReadCommittedMetadata(offset, max); return err })
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
