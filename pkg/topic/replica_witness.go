package topic

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/cursus-io/cursus/pkg/disk"
)

// ReplicaRequestWitness binds a local challenge to the exact receiver and its
// pre-challenge content generation. It is local authority, never wire data.
type ReplicaRequestWitness struct {
	owner                *Partition
	generation           disk.ContentGeneration
	attempt, leo, marker uint64
}

// ReplicaCatchupWitness is produced only after the actual receiver has applied
// and verified a terminal response. Fields are intentionally opaque.
type ReplicaCatchupWitness struct {
	leaderID               string
	request                ReplicaRequestWitness
	leader                 int
	lifecycle, hwm, repair uint64
}

type replicaPublicationStorage interface {
	replicaGenerationStorage
	PublishIfContentGeneration(disk.ContentGeneration, func()) error
}

// CaptureReplicaRequestWitness requires the caller's reconciliation read fence
// to span capture, challenge reads, and ValidateReplicaRequestWitness.
func (p *Partition) CaptureReplicaRequestWitness(ctx context.Context, leo uint64) (*ReplicaRequestWitness, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.snapshotRecovery || p.LEO.Load() != leo || p.replicaAttempt == math.MaxUint64 {
		return nil, fmt.Errorf("replica request state changed")
	}
	if _, ok := p.dh.(replicaPublicationStorage); !ok {
		return nil, ErrReplicaSourceUnsupported
	}
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	if err := p.replicaRecoveryErrorLocked(); err != nil {
		return nil, err
	}
	generation, err := p.CaptureReplicaContentGeneration(ctx)
	if err != nil {
		return nil, err
	}
	p.replicaAttempt++
	return &ReplicaRequestWitness{owner: p, generation: generation, attempt: p.replicaAttempt, leo: leo, marker: p.reconciliationRevision}, nil
}

func (p *Partition) replicaRecoveryErrorLocked() error {
	return errors.Join(p.recoveryErr, p.txnRecoveryErr, p.producerRecoveryErr, p.bootstrapErrorLocked(), p.rollbackErrorLocked())
}

func (p *Partition) ValidateReplicaRequestWitness(ctx context.Context, w *ReplicaRequestWitness, leo uint64) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if w == nil || w.owner != p || w.attempt != p.replicaAttempt || w.leo != leo || p.LEO.Load() != leo || p.closed {
		return fmt.Errorf("replica request witness is stale")
	}
	return p.CheckReplicaContentGeneration(ctx, w.generation)
}

// CheckReplicaRequestGeneration checks the original challenge even after the
// receiver has appended records, or entered checked rollback exclusion.
func (p *Partition) CheckReplicaRequestGeneration(ctx context.Context, w *ReplicaRequestWitness) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if w == nil || w.owner != p || w.attempt != p.replicaAttempt || p.closed {
		return fmt.Errorf("replica request witness is stale")
	}
	return p.CheckReplicaContentGeneration(ctx, w.generation)
}

// ApplyReplicaHWMChecked publishes only scalar state while the storage
// generation is locked. Notifications and checkpoint scheduling happen later.
func (p *Partition) ApplyReplicaHWMChecked(ctx context.Context, w *ReplicaRequestWitness, hwm uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	if w == nil || w.owner != p || w.attempt != p.replicaAttempt || p.closed || p.LEO.Load() != hwm {
		return fmt.Errorf("replica completion state changed")
	}
	if err := p.replicaRecoveryErrorLocked(); err != nil {
		return err
	}
	storage, ok := p.dh.(replicaPublicationStorage)
	if !ok {
		return ErrReplicaSourceUnsupported
	}
	if err := storage.PublishIfContentGeneration(w.generation, func() { p.HWM = hwm; p.hwmAuthoritative = true }); err != nil {
		return err
	}
	p.signalHWMCheckpointLocked()
	p.NotifyNewMessage()
	return nil
}

// CompleteReplicaCatchupVerification is called after terminal data and local ES
// indexing have succeeded, while the caller still holds reconciliation R.
func (p *Partition) CompleteReplicaCatchupVerification(ctx context.Context, w *ReplicaRequestWitness, leaderID string, leader int, lifecycle, hwm, repair uint64) (*ReplicaCatchupWitness, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	if w == nil || w.owner != p || w.attempt != p.replicaAttempt || p.closed || p.snapshotRecovery || p.LEO.Load() != hwm || p.HWM != hwm || p.reconciliationRevision != w.marker {
		return nil, fmt.Errorf("replica completion witness is stale")
	}
	if err := p.replicaRecoveryErrorLocked(); err != nil {
		return nil, err
	}
	if err := p.CheckReplicaContentGeneration(ctx, w.generation); err != nil {
		return nil, err
	}
	return &ReplicaCatchupWitness{request: *w, leaderID: leaderID, leader: leader, lifecycle: lifecycle, hwm: hwm, repair: repair}, nil
}

// AcceptReplicaCatchupWitness consumes the original verification authority after
// snapshot network work. It never upgrades a held reconciliation read lock.
func (p *Partition) AcceptReplicaCatchupWitness(w *ReplicaCatchupWitness, leaderID string, leader int, lifecycle, hwm uint64) error {
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()
	if w == nil || w.leaderID != leaderID {
		return fmt.Errorf("replica leader identity changed")
	}
	if err := p.validateWitnessLocked(w, leader, lifecycle, hwm, 0); err != nil {
		return err
	}
	storage, ok := p.dh.(replicaPublicationStorage)
	if !ok {
		return ErrReplicaSourceUnsupported
	}
	return storage.PublishIfContentGeneration(w.request.generation, func() {
		p.reconciliationErr = nil
		p.replicaWitness = w
		p.replicaVerified = true
	})
}

func (p *Partition) validateWitnessLocked(w *ReplicaCatchupWitness, leader int, lifecycle, hwm, repair uint64) error {
	if w == nil || w.request.owner != p || w.request.attempt != p.replicaAttempt || w.request.marker != p.reconciliationRevision || w.leader != leader || w.lifecycle != lifecycle || w.hwm != hwm || (repair != 0 && w.repair != repair) || p.LEO.Load() != hwm || p.HWM != hwm || p.closed || p.snapshotRecovery {
		return fmt.Errorf("replica verification witness is stale")
	}
	return p.replicaRecoveryErrorLocked()
}

// ValidateReplicaCatchupEligibility is shared by heartbeat and direct proof
// production. A persisted receipt or equal offset tuple is not proof authority.
func (p *Partition) ValidateReplicaCatchupEligibility(leader int, lifecycle, hwm, repair uint64) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	if !p.replicaVerified || p.reconciliationErr != nil {
		return fmt.Errorf("replica has no current verified repair")
	}
	if err := p.validateWitnessLocked(p.replicaWitness, leader, lifecycle, hwm, repair); err != nil {
		return err
	}
	return p.CheckReplicaContentGeneration(context.Background(), p.replicaWitness.request.generation)
}

// ReplicaRepairGeneration returns the verified disk identity under the existing
// read-view/FSM scope. It does not itself make a later replay publication atomic.
func (v *PartitionReadView) ReplicaRepairGeneration(leader int, lifecycle, hwm, repair uint64) (disk.ContentGeneration, error) {
	if err := v.check(); err != nil {
		return disk.ContentGeneration{}, err
	}
	p := v.p
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	if !p.replicaVerified || p.reconciliationErr != nil {
		return disk.ContentGeneration{}, fmt.Errorf("consumer log has no verified repair")
	}
	if err := p.validateWitnessLocked(p.replicaWitness, leader, lifecycle, hwm, repair); err != nil {
		return disk.ContentGeneration{}, err
	}
	generation := p.replicaWitness.request.generation
	return generation, p.CheckReplicaContentGeneration(context.Background(), generation)
}

// AcquireReplicaRepairPublication excludes verification changes through final
// publication. The caller owns visibilityR, fully drained admission, and FSM
// exclusion. Never acquire reconcileR here: rollback may hold reconcileW while
// waiting for this read view. No storage maintenance runs under these guards.
func (v *PartitionReadView) AcquireReplicaRepairPublication(leader int, lifecycle, hwm, repair uint64, generation disk.ContentGeneration) (func(), error) {
	if err := v.check(); err != nil {
		return nil, err
	}
	p := v.p
	p.mu.RLock()
	p.recoveryMu.RLock()
	release := func() { p.recoveryMu.RUnlock(); p.mu.RUnlock() }
	if !p.replicaVerified || p.reconciliationErr != nil || p.replicaWitness == nil || p.replicaWitness.request.generation != generation {
		release()
		return nil, fmt.Errorf("consumer repair publication witness changed")
	}
	if err := p.validateWitnessLocked(p.replicaWitness, leader, lifecycle, hwm, repair); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// WithReplicaCatchupIndexView exposes the fixed committed range to derived ES
// indexing while its unchanged reconciliation marker still fences ordinary
// readers. Callers own reconciliation R, lifecycle/write and FSM exclusion.
// Every other recovery error remains fatal, and transaction visibility is kept.
func (p *Partition) WithReplicaCatchupIndexView(ctx context.Context, w *ReplicaRequestWitness, hwm uint64, fn func(*PartitionReadView) error) error {
	p.visibilityMu.RLock()
	defer p.visibilityMu.RUnlock()
	validate := func() error {
		p.mu.RLock()
		defer p.mu.RUnlock()
		p.recoveryMu.RLock()
		defer p.recoveryMu.RUnlock()
		if w == nil || w.owner != p || w.attempt != p.replicaAttempt || w.marker != p.reconciliationRevision || p.LEO.Load() != hwm || p.HWM != hwm || p.closed || p.snapshotRecovery {
			return fmt.Errorf("replica index view is stale")
		}
		if err := p.replicaRecoveryErrorLocked(); err != nil {
			return err
		}
		return p.CheckReplicaContentGeneration(ctx, w.generation)
	}
	if err := validate(); err != nil {
		return err
	}
	if err := p.withReadViewLocked(true, hwm, func(view *PartitionReadView) error {
		view.replicaIndexWitness = w
		return fn(view)
	}); err != nil {
		return err
	}
	return validate()
}

// ReplicaCatchupIndexGeneration is available only within the purpose-specific
// recovery callback. It cannot authorize source export or bootstrap installation.
func (v *PartitionReadView) ReplicaCatchupIndexGeneration(name string, partition int, end uint64) (disk.ContentGeneration, error) {
	if err := v.ValidateIdentity(name, partition); err != nil {
		return disk.ContentGeneration{}, err
	}
	if !v.offline || v.replicaIndexWitness == nil || v.end != end {
		return disk.ContentGeneration{}, fmt.Errorf("replica index recovery view required")
	}
	return v.replicaIndexWitness.generation, nil
}
