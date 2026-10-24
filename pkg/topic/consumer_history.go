package topic

import (
	"context"
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
)

type consumerHistoryStorage interface {
	ConsumerHistorySegments(context.Context, uint64) ([]disk.ConsumerHistorySegment, error)
	RefreshConsumerHistorySegment(context.Context, disk.ConsumerHistorySegment) (disk.ConsumerHistorySegment, error)
	RewriteCertifiedConsumerHistory(context.Context, disk.ConsumerHistorySegment, disk.ConsumerHistoryRemovalCertificate) (disk.CompactionResult, error)
}

func (v *PartitionReadView) RefreshConsumerHistorySegment(ctx context.Context, old disk.ConsumerHistorySegment) (disk.ConsumerHistorySegment, error) {
	if err := v.check(); err != nil {
		return disk.ConsumerHistorySegment{}, err
	}
	s, ok := v.p.dh.(consumerHistoryStorage)
	if !ok {
		return disk.ConsumerHistorySegment{}, fmt.Errorf("storage lacks checked consumer history rewrite")
	}
	return s.RefreshConsumerHistorySegment(ctx, old)
}

func (v *PartitionReadView) ConsumerHistorySegments(ctx context.Context, end uint64) ([]disk.ConsumerHistorySegment, error) {
	if err := v.check(); err != nil {
		return nil, err
	}
	if v.offline || v.snapshotCut != nil || end > v.CommittedEnd() || v.p.topic != config.ConsumerOffsetsTopicName {
		return nil, fmt.Errorf("consumer history requires ordinary committed offsets view")
	}
	s, ok := v.p.dh.(consumerHistoryStorage)
	if !ok {
		return nil, fmt.Errorf("storage lacks checked consumer history rewrite")
	}
	return s.ConsumerHistorySegments(ctx, end)
}

// WithConsumerHistoryInstallation excludes readers/bootstrap/rollback across
// validation and publication. It removes certified retired transaction evidence
// without changing HWM, LEO, producer fences or consumer membership. Caller holds
// lifecycle/FSM and consumer fences.
func (p *Partition) WithConsumerHistoryInstallation(ctx context.Context, fn func(*PartitionReadView, func(disk.ConsumerHistorySegment, disk.ConsumerHistoryRemovalCertificate) (disk.CompactionResult, error)) error) error {
	if p.topic != config.ConsumerOffsetsTopicName {
		return fmt.Errorf("not consumer metadata")
	}
	if err := p.reconcileMu.LockContext(ctx); err != nil {
		return err
	}
	defer p.reconcileMu.Unlock()
	if err := p.visibilityMu.LockContext(ctx); err != nil {
		return err
	}
	defer p.visibilityMu.Unlock()
	if p.BootstrapPending() || p.SnapshotRecoveryPending() || !p.HWMKnown() {
		return fmt.Errorf("consumer history partition recovery pending")
	}
	if err := p.RecoveryError(); err != nil {
		return err
	}
	s, ok := p.dh.(consumerHistoryStorage)
	if !ok {
		return fmt.Errorf("storage lacks checked consumer history rewrite")
	}
	return p.withReadViewLocked(false, 0, func(v *PartitionReadView) error {
		used := false
		return fn(v, func(segment disk.ConsumerHistorySegment, certificate disk.ConsumerHistoryRemovalCertificate) (disk.CompactionResult, error) {
			if used || !v.active {
				return disk.CompactionResult{}, fmt.Errorf("history installer is single use")
			}
			used = true
			_, end := segment.Range()
			if end > v.CommittedEnd() {
				return disk.CompactionResult{}, fmt.Errorf("history exceeds committed cut")
			}
			result, err := s.RewriteCertifiedConsumerHistory(ctx, segment, certificate)
			// Even a failed publication may invalidate the durable generation.
			p.visibilityGeneration++
			if err == nil {
				p.txnMarkerMu.Lock()
				for _, m := range certificate.RemovedRecords() {
					key := messageTransactionMarkerKey(m)
					if marker, ok := p.txnMarkers[key]; ok && m.TransactionMarker != "" && marker.offset == m.Offset {
						delete(p.txnMarkers, key)
						delete(p.txnOpenOffsets, key)
					}
				}
				p.advanceTransactionEvidenceRevisionLocked()
				p.txnMarkerMu.Unlock()
			}
			return result, err
		})
	})
}
