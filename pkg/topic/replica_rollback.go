package topic

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
)

var ErrReplicaRollbackPending = errors.New("partition replica rollback pending")
var ErrReplicaRollbackUnsupported = errors.New("storage does not support checked replica rollback")
var ErrReplicaRollbackFailed = errors.New("replica rollback requires partition recovery")

type replicaRollbackStorage interface {
	replicaGenerationStorage
	TruncateToIfGenerationContext(context.Context, uint64, disk.ContentGeneration) error
	PublishIfContentGeneration(disk.ContentGeneration, func()) error
	ScanRetainedRange(context.Context, uint64, uint64, func(types.Message) error) error
}

type replicaRepairPolicy uint8

// Only the compatibility API can resume this exact completed physical repair.
// Checked recovery still requires its original authority and witness validation.
type replicaCheckpointRetry struct {
	offset     uint64
	generation disk.ContentGeneration
}

const (
	canonicalReplicaRollback replicaRepairPolicy = iota
	speculativeReplicaTail
	committedReplicaReconcile
)

func (p *Partition) rollbackErrorLocked() error {
	if p.rollbackPending.Load() {
		return errors.Join(ErrReplicaRollbackPending, p.rollbackErr)
	}
	return p.rollbackErr
}

func (p *Partition) rollbackError() error {
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	return p.rollbackErrorLocked()
}

// ApplyReplicaRollbackChecked owns the receiver validation-to-truncate boundary.
// Callers hold lifecycle/write then FSM transition/materialization fences.
// beforeMutation runs only after local validation, outside every disk lock; it
// may invalidate derived consumers but must not reenter partition operations.
func (p *Partition) ApplyReplicaRollbackChecked(ctx context.Context, offset, expectedLEO uint64, prefix *disk.LineagePrefix, hwm uint64, beforeMutation func() error) error {
	return p.applyReplicaRollback(ctx, offset, expectedLEO, prefix, hwm, beforeMutation, true)
}

func (p *Partition) applyReplicaRollback(ctx context.Context, offset, expectedLEO uint64, prefix *disk.LineagePrefix, hwm uint64, beforeMutation func() error, checked bool) (result error) {
	return p.repairReplicaTail(ctx, offset, expectedLEO, prefix, hwm, beforeMutation, checked, canonicalReplicaRollback)
}

// repairReplicaTail shares physical repair while keeping the distinct authority
// rules of canonical divergence rollback, speculative tail repair and Raft HWM
// reconciliation. No maintenance lock or retained-log scan runs under p.mu.
func (p *Partition) repairReplicaTail(ctx context.Context, offset, expectedLEO uint64, prefix *disk.LineagePrefix, hwm uint64, beforeMutation func() error, checked bool, policy replicaRepairPolicy) (result error) {
	if ctx == nil {
		return fmt.Errorf("replica rollback requires context")
	}
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()
	p.visibilityMu.Lock()
	defer p.visibilityMu.Unlock()
	p.hwmCheckpointMu.Lock()
	defer p.hwmCheckpointMu.Unlock()
	p.producerCheckpointMu.Lock()
	defer p.producerCheckpointMu.Unlock()
	p.mu.Lock()
	p.recoveryMu.RLock()
	// A separate reconciliation marker may need canonical catch-up before its
	// normal owner can resolve it. Preserve it without blocking physical repair.
	baseErr := errors.Join(p.recoveryErr, p.txnRecoveryErr, p.producerRecoveryErr, p.bootstrapErrorLocked())
	retry := p.rollbackCheckpointRetry
	canRetry := retry != nil && p.rollbackErr != nil && !checked && policy == canonicalReplicaRollback &&
		offset == retry.offset && expectedLEO == offset && p.LEO.Load() == offset && p.HWM == offset &&
		!p.closed && !p.snapshotRecovery && baseErr == nil
	err := errors.Join(baseErr, p.rollbackErrorLocked())
	marker := p.reconciliationRevision
	p.recoveryMu.RUnlock()
	if canRetry {
		p.mu.Unlock()
		return p.retryReplicaCheckpoints(ctx, retry)
	}
	oldHWM, wasAuthoritative := p.HWM, p.hwmAuthoritative
	if policy != canonicalReplicaRollback {
		// Public repair boundaries are not tied to an earlier fetch request.
		// Sample LEO only after excluding concurrent replication and writers.
		expectedLEO = p.LEO.Load()
		if err == nil && p.snapshotRecovery {
			err = fmt.Errorf("snapshot replay is still pending: visible_hwm=%d durable_hwm=%d requested_offset=%d", p.HWM, p.recoveryCheckpointHWM, offset)
		}
		if err == nil && offset < oldHWM {
			if policy == speculativeReplicaTail {
				err = fmt.Errorf("replica tail truncation crosses committed HWM: hwm=%d requested_offset=%d", oldHWM, offset)
			} else {
				err = fmt.Errorf("committed HWM regression: current=%d requested=%d", oldHWM, offset)
			}
		}
		if err == nil && offset > expectedLEO {
			err = fmt.Errorf("replica is behind repair boundary: leo=%d offset=%d", expectedLEO, offset)
		}
	}
	if err == nil && (p.closed || p.snapshotRecovery || p.LEO.Load() != expectedLEO || offset > expectedLEO || p.replicaAttempt == math.MaxUint64) {
		err = fmt.Errorf("replica rollback local state changed")
	}
	if err == nil {
		err = ctx.Err()
	}
	storage, supported := p.dh.(replicaRollbackStorage)
	if err == nil && checked && !supported {
		err = ErrReplicaRollbackUnsupported
	}
	if err != nil {
		p.mu.Unlock()
		return err
	}
	if policy != canonicalReplicaRollback && offset == expectedLEO {
		if policy == committedReplicaReconcile {
			p.HWM = offset
			p.hwmAuthoritative = true
			if oldHWM != offset || !wasAuthoritative {
				p.signalHWMCheckpointLocked()
				p.NotifyNewMessage()
			}
			p.clearOwnedReconciliationError(marker)
		}
		p.mu.Unlock()
		return nil
	}
	p.replicaVerified = false
	p.rollbackPending.Store(true)
	p.mu.Unlock()
	uncertain := false
	defer func() {
		p.recoveryMu.Lock()
		if uncertain && result != nil {
			result = errors.Join(ErrReplicaRollbackFailed, result)
			p.rollbackErr = fmt.Errorf("replica rollback recovery failed: %w", result)
		}
		p.rollbackPending.Store(false)
		p.recoveryMu.Unlock()
	}()
	var generation disk.ContentGeneration
	if supported {
		generation, err = storage.CaptureContentGenerationContext(ctx)
		if err != nil {
			return err
		}
	}
	if prefix != nil {
		verifier, ok := p.dh.(interface {
			VerifyLineagePrefixContext(context.Context, disk.LineagePrefix, uint64) (bool, error)
		})
		if !ok || !supported {
			return ErrReplicaRollbackUnsupported
		}
		matched, err := verifier.VerifyLineagePrefixContext(ctx, *prefix, hwm)
		if err != nil {
			return err
		}
		if !matched || prefix.EndOffset != offset {
			return fmt.Errorf("local retained prefix changed before replica rollback")
		}
	}
	if supported {
		if err := storage.CheckContentGenerationContext(ctx, generation); err != nil {
			return err
		}
	}
	p.mu.RLock()
	valid := !p.closed && p.LEO.Load() == expectedLEO
	p.mu.RUnlock()
	if !valid {
		return fmt.Errorf("replica rollback local state changed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if beforeMutation != nil {
		if err := beforeMutation(); err != nil {
			return err
		}
	}
	// Even a clean disk rejection after consumer repair admission invalidates
	// older verification attempts whose content generation may remain unchanged.
	p.mu.Lock()
	p.replicaAttempt++
	p.eventIDGeneration.Add(1)
	p.mu.Unlock()
	if supported {
		err = storage.TruncateToIfGenerationContext(ctx, offset, generation)
	} else {
		// Compatibility for legacy callers only; production uses the checked API.
		err = p.dh.TruncateTo(offset)
	}
	if err != nil {
		// Disk explicitly distinguishes clean rejection from partial mutation.
		// Never wait for another disk lock to classify a canceled operation.
		uncertain = !errors.Is(err, disk.ErrTruncateNotApplied) && !errors.Is(err, disk.ErrStaleContentGeneration)
		return err
	}
	uncertain = true
	p.mu.Lock()
	p.LEO.Store(offset)
	if policy == canonicalReplicaRollback {
		p.HWM = offset
		p.hwmAuthoritative = true
	}
	p.mu.Unlock()
	p.visibilityGeneration++
	if supported {
		generation, err = storage.CaptureContentGenerationContext(ctx)
		if err != nil {
			return err
		}
	}
	state, err := p.scanRawRecoveryStateContext(ctx)
	if err != nil {
		return err
	}
	if state.end != offset {
		return fmt.Errorf("replica rollback durable tail changed")
	}
	if supported {
		if err := storage.CheckContentGenerationContext(ctx, generation); err != nil {
			return err
		}
	}
	p.installTransactionState(state)
	p.installProducerState(state)
	_, durable := p.dh.(durablePartitionStorage)
	strictCheckpoint := checked || supported || durable
	checkpointFailure := func(err error) error {
		if !checked && policy == canonicalReplicaRollback && supported {
			p.rollbackCheckpointRetry = &replicaCheckpointRetry{offset: offset, generation: generation}
		}
		return err
	}
	// Explicitly non-durable legacy adapters have no checkpoint files. Real
	// durable storage must still reject absent paths and failed persistence.
	if strictCheckpoint || p.producerStatePath != "" {
		if err := p.persistProducerStateCheckpointLocked(strictCheckpoint); err != nil {
			return checkpointFailure(err)
		}
	}
	checkpointHWM := offset
	if policy == speculativeReplicaTail {
		checkpointHWM = oldHWM
	}
	// A versioned checkpoint is authoritative on restart. Never certify an
	// unknown old watermark merely because its speculative tail was removed.
	if (policy != speculativeReplicaTail || wasAuthoritative) && (strictCheckpoint || p.hwmCheckpointPath != "") {
		if err := p.persistHWMCheckpointLocked(checkpointHWM, strictCheckpoint); err != nil {
			return checkpointFailure(err)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition closed during replica rollback")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	publish := func() {
		if policy == committedReplicaReconcile {
			p.HWM = offset
			p.hwmAuthoritative = true
		}
		p.rollbackPending.Store(false)
	}
	if supported {
		// Only an already-excluded scalar is published under the disk check.
		if err := storage.PublishIfContentGeneration(generation, publish); err != nil {
			return err
		}
	} else {
		publish()
	}
	if policy == committedReplicaReconcile {
		p.clearOwnedReconciliationError(marker)
		if oldHWM != offset || !wasAuthoritative {
			p.NotifyNewMessage()
		}
	}
	return nil
}

// Caller owns all repair gates. No truncation or raw scan is repeated here.
func (p *Partition) retryReplicaCheckpoints(ctx context.Context, retry *replicaCheckpointRetry) error {
	storage, ok := p.dh.(replicaRollbackStorage)
	if !ok || p.dh.GetAbsoluteOffset() != retry.offset {
		return ErrReplicaRollbackFailed
	}
	if err := storage.CheckContentGenerationContext(ctx, retry.generation); err != nil {
		return err
	}
	if err := p.persistProducerStateCheckpointLocked(true); err != nil {
		return err
	}
	if err := p.persistHWMCheckpointLocked(retry.offset, true); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()
	if p.closed {
		return ErrReplicaRollbackFailed
	}
	return storage.PublishIfContentGeneration(retry.generation, func() {
		p.rollbackErr = nil
		p.rollbackCheckpointRetry = nil
	})
}

func (p *Partition) clearOwnedReconciliationError(revision uint64) {
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()
	if p.reconciliationRevision == revision {
		p.reconciliationErr = nil
	}
}
