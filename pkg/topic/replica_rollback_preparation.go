package topic

import (
	"context"
	"fmt"

	"github.com/cursus-io/cursus/pkg/disk"
)

// ReplicaRollbackPreparation is local, single-use reconstruction evidence. It
// neither grants metadata authority nor permits truncation without revalidation.
type ReplicaRollbackPreparation struct {
	owner                               *Partition
	state                               *rawRecoveryState
	generation                          disk.ContentGeneration
	offset, leo, hwm, attempt, proofHWM uint64
	prefix                              *disk.LineagePrefix
	used                                bool // accessed only while owner's reconciliation write gate is held
}

// PrepareReplicaRollbackContext reads the retained prefix before callers acquire
// broker-wide metadata gates. Cancellation or stale content makes no mutation.
func (p *Partition) PrepareReplicaRollbackContext(ctx context.Context, offset, leo uint64, prefix *disk.LineagePrefix, hwm uint64) (*ReplicaRollbackPreparation, error) {
	release, err := p.BeginReplicationMutationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	storage, ok := p.dh.(replicaRollbackStorage)
	if !ok {
		return nil, ErrReplicaRollbackUnsupported
	}
	p.mu.RLock()
	valid := !p.closed && !p.snapshotRecovery && p.LEO.Load() == leo && offset < leo
	prepared := &ReplicaRollbackPreparation{owner: p, offset: offset, leo: leo, hwm: p.HWM, attempt: p.replicaAttempt, proofHWM: hwm}
	p.mu.RUnlock()
	if !valid {
		return nil, fmt.Errorf("replica rollback preparation state changed")
	}
	p.recoveryMu.RLock()
	err = p.replicaRecoveryErrorLocked()
	p.recoveryMu.RUnlock()
	if err != nil {
		return nil, err
	}
	prepared.generation, err = storage.CaptureContentGenerationContext(ctx)
	if err != nil {
		return nil, err
	}
	if prefix != nil {
		proof := *prefix
		prepared.prefix = &proof
		verifier, ok := p.dh.(interface {
			VerifyLineagePrefixContext(context.Context, disk.LineagePrefix, uint64) (bool, error)
		})
		if !ok {
			return nil, ErrReplicaRollbackUnsupported
		}
		matched, err := verifier.VerifyLineagePrefixContext(ctx, proof, hwm)
		if err != nil {
			return nil, err
		}
		if !matched || proof.EndOffset != offset {
			return nil, fmt.Errorf("local retained prefix changed before rollback preparation")
		}
	}
	prepared.state, err = p.scanRawRecoveryRangeContext(ctx, &offset, false)
	if err != nil {
		return nil, err
	}
	if err = storage.CheckContentGenerationContext(ctx, prepared.generation); err != nil {
		return nil, err
	}
	p.mu.RLock()
	valid = !p.closed && p.LEO.Load() == leo && p.HWM == prepared.hwm && p.replicaAttempt == prepared.attempt
	p.mu.RUnlock()
	if !valid {
		return nil, fmt.Errorf("replica rollback preparation state changed")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return prepared, nil
}

// ApplyPreparedReplicaRollbackChecked consumes reconstruction only after the
// caller has reacquired its metadata fence; disk and local witnesses are checked.
func (p *Partition) ApplyPreparedReplicaRollbackChecked(ctx context.Context, prepared *ReplicaRollbackPreparation, offset, leo uint64, prefix *disk.LineagePrefix, hwm uint64, beforeMutation func() error) error {
	if prepared == nil {
		return fmt.Errorf("replica rollback preparation is required")
	}
	return p.repairReplicaTail(ctx, offset, leo, prefix, hwm, beforeMutation, true, canonicalReplicaRollback, nil, prepared)
}

// TruncatePreparedReplicaTailContext applies a prepared speculative-tail repair
// only while the sampled tail and content generation remain unchanged.
func (p *Partition) TruncatePreparedReplicaTailContext(ctx context.Context, prepared *ReplicaRollbackPreparation, offset uint64) error {
	if prepared == nil {
		return fmt.Errorf("replica tail preparation is required")
	}
	return p.repairReplicaTail(ctx, offset, 0, nil, 0, nil, false, speculativeReplicaTail, nil, prepared)
}

func (prepared *ReplicaRollbackPreparation) matches(p *Partition, offset, leo, hwm uint64, prefix *disk.LineagePrefix) bool {
	if prepared.owner != p || prepared.used || prepared.state == nil || prepared.offset != offset || prepared.leo != leo ||
		prepared.hwm != p.HWM || prepared.attempt != p.replicaAttempt || prepared.proofHWM != hwm {
		return false
	}
	if prefix == nil || prepared.prefix == nil {
		return prefix == nil && prepared.prefix == nil
	}
	return *prefix == *prepared.prefix
}
