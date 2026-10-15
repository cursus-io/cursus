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
	err := errors.Join(p.recoveryErr, p.txnRecoveryErr, p.producerRecoveryErr, p.bootstrapErrorLocked(), p.rollbackErrorLocked())
	p.recoveryMu.RUnlock()
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
	p.HWM = offset
	p.hwmAuthoritative = true
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
	if err := p.persistProducerStateCheckpointLocked(checked || supported); err != nil {
		return err
	}
	if err := p.persistHWMCheckpointLocked(offset, checked || supported); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition closed during replica rollback")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if supported {
		// Only an already-excluded scalar is published under the disk check.
		if err := storage.PublishIfContentGeneration(generation, func() { p.rollbackPending.Store(false) }); err != nil {
			return err
		}
	}
	return nil
}
