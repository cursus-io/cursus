package sdk

import (
	"context"
	"fmt"
	"time"
)

const (
	EffectEnqueued  = "ENQUEUED"
	EffectSucceeded = "SUCCEEDED"
	EffectFailed    = "FAILED"
)

type EffectState struct {
	ID        string
	Step      string
	Status    string
	CommandID string
	Published bool
	Attempts  int
	LastError string
	UpdatedAt time.Time
}

type CompensationState struct {
	Step      string
	Status    string
	Attempts  int
	LastError string
	UpdatedAt time.Time
}

// RecordCommandPublished records a confirmed hand-off of an already-enqueued
// command. It deliberately does not change the business effect result: only
// RecordEffectResult can mark an effect as SUCCEEDED or FAILED.
func (m *SagaManager) RecordCommandPublished(ctx context.Context, associationKey, effectID string) error {
	if associationKey == "" || effectID == "" {
		return fmt.Errorf("association key and effect ID are required")
	}
	if m.transaction != nil {
		return m.transaction.WithinSagaTransaction(ctx, func(txCtx context.Context, stores SagaTransactionStores) error {
			if stores.State == nil || stores.History == nil {
				return fmt.Errorf("saga transaction stores are incomplete")
			}
			local := *m
			local.state, local.history, local.transaction = stores.State, stores.History, nil
			return local.recordCommandPublished(txCtx, associationKey, effectID)
		})
	}
	return m.recordCommandPublished(ctx, associationKey, effectID)
}

func (m *SagaManager) recordCommandPublished(ctx context.Context, associationKey, effectID string) error {
	state, err := m.state.Load(ctx, m.definition.Type, associationKey)
	if err != nil {
		return fmt.Errorf("load saga state: %w", err)
	}
	if state == nil {
		return fmt.Errorf("saga state not found")
	}
	effect, ok := state.Effects[effectID]
	if !ok {
		return fmt.Errorf("effect %s not found", effectID)
	}
	if effect.Published || effect.Status != EffectPending {
		return nil
	}
	command := Command{ID: effect.CommandID, EffectID: effect.ID, Type: effect.Step, SagaID: state.ID, CorrelationID: state.CorrelationID}
	if err := m.record(ctx, state, EventEnvelope{}, SagaHistoryCommandPublished, effect.Step, uint32(effect.Attempts), command, ""); err != nil {
		return err
	}
	effect.Published = true
	effect.UpdatedAt, state.Effects[effectID], state.UpdatedAt = m.now().UTC(), effect, m.now().UTC()
	if err := m.state.Save(ctx, state); err != nil {
		return fmt.Errorf("save command publication: %w", err)
	}
	return nil
}

// RecordEffectResult records an externally confirmed business result. It is
// intentionally separate from OutboxStore.Enqueue: an accepted local outbox
// row only produces command.enqueued, never command.succeeded.
func (m *SagaManager) RecordEffectResult(ctx context.Context, associationKey, effectID string, succeeded bool, cause error) error {
	if associationKey == "" || effectID == "" {
		return fmt.Errorf("association key and effect ID are required")
	}
	if succeeded && cause != nil {
		return fmt.Errorf("successful effect result cannot include an error")
	}
	if m.transaction != nil {
		return m.transaction.WithinSagaTransaction(ctx, func(txCtx context.Context, stores SagaTransactionStores) error {
			if stores.State == nil || stores.History == nil {
				return fmt.Errorf("saga transaction stores are incomplete")
			}
			local := *m
			local.state, local.history, local.transaction = stores.State, stores.History, nil
			return local.recordEffectResult(txCtx, associationKey, effectID, succeeded, cause)
		})
	}
	return m.recordEffectResult(ctx, associationKey, effectID, succeeded, cause)
}

func (m *SagaManager) recordEffectResult(ctx context.Context, associationKey, effectID string, succeeded bool, cause error) error {
	state, err := m.state.Load(ctx, m.definition.Type, associationKey)
	if err != nil {
		return fmt.Errorf("load saga state: %w", err)
	}
	if state == nil {
		return fmt.Errorf("saga state not found")
	}
	effect, ok := state.Effects[effectID]
	if !ok {
		return fmt.Errorf("effect %s not found", effectID)
	}
	if effect.Status == EffectSucceeded || effect.Status == EffectFailed {
		return nil
	}
	effect.UpdatedAt = m.now().UTC()
	command := Command{ID: effect.CommandID, EffectID: effect.ID, Type: effect.Step, SagaID: state.ID, CorrelationID: state.CorrelationID}
	if succeeded {
		effect.Status, effect.LastError = EffectSucceeded, ""
		if err := m.record(ctx, state, EventEnvelope{}, SagaHistoryCommandSucceeded, effect.Step, uint32(effect.Attempts), command, ""); err != nil {
			return err
		}
	} else {
		message := "effect failed"
		if cause != nil {
			message = cause.Error()
		}
		effect.Status, effect.LastError = EffectFailed, message
		if err := m.record(ctx, state, EventEnvelope{}, SagaHistoryCommandFailed, effect.Step, uint32(effect.Attempts), command, message); err != nil {
			return err
		}
	}
	state.Effects[effectID], state.UpdatedAt = effect, m.now().UTC()
	if err := m.state.Save(ctx, state); err != nil {
		return fmt.Errorf("save effect result: %w", err)
	}
	return nil
}

func (m *SagaManager) loadOrCreateState(ctx context.Context, associationKey string) (*SagaState, error) {
	if associationKey == "" {
		return nil, fmt.Errorf("association key is required")
	}
	return m.updateEffect(ctx, associationKey, effectID, commandID, EffectFailed, cause)
}

func (m *SagaManager) updateEffect(ctx context.Context, associationKey, effectID, commandID, status string, cause error) error {
	if effectID == "" || commandID == "" {
		return fmt.Errorf("effect and command identities are required")
	}
	return m.repository.Transact(ctx, func(tx SagaTransaction) error {
		state, expectedVersion, err := m.loadOrCreateState(tx, associationKey)
		if err != nil {
			return err
		}
		effect, ok := state.Effects[effectID]
		if !ok {
			return fmt.Errorf("effect %q does not exist", effectID)
		}
		if effect.CommandID != commandID {
			return fmt.Errorf("effect %q command fence mismatch", effectID)
		}
		if effect.Status == status {
			return nil
		}
		if effect.Status != EffectEnqueued {
			return fmt.Errorf("effect %q is not awaiting acknowledgement", effectID)
		}
		effect.Status = status
		effect.LastError = ""
		if cause != nil {
			effect.LastError = cause.Error()
		}
		effect.UpdatedAt = m.now().UTC()
		state.Effects[effectID] = effect
		state.UpdatedAt = m.now().UTC()
		if err := saveSagaState(tx, state, expectedVersion); err != nil {
			return fmt.Errorf("save acknowledged effect: %w", err)
		}
		return nil
	})
}

func (m *SagaManager) StartCompensation(ctx context.Context, associationKey, step string, cause error) (*SagaState, error) {
	if m.transaction != nil {
		var result *SagaState
		err := m.transaction.WithinSagaTransaction(ctx, func(txCtx context.Context, stores SagaTransactionStores) error {
			if stores.State == nil || stores.History == nil {
				return fmt.Errorf("saga transaction stores are incomplete")
			}
			local := *m
			local.state, local.history, local.transaction = stores.State, stores.History, nil
			var startErr error
			result, startErr = local.startCompensation(txCtx, associationKey, step, cause)
			return startErr
		})
		return result, err
	}
	return m.startCompensation(ctx, associationKey, step, cause)
}
func (m *SagaManager) startCompensation(ctx context.Context, associationKey, step string, cause error) (*SagaState, error) {
	if step == "" {
		return nil, fmt.Errorf("compensation step is required")
	}
	state, err := m.loadOrCreateState(ctx, associationKey)
	if err != nil {
		return nil, err
	}
	if state.Compensation == nil {
		state.Compensation = &CompensationState{}
	}
	state.Compensation.Step = step
	state.Compensation.Status = SagaCompensating
	state.Compensation.Attempts++
	state.Compensation.LastError = ""
	if cause != nil {
		state.Compensation.LastError = cause.Error()
	}
	state.Status = SagaCompensating
	state.Outcome = ""
	state.UpdatedAt = m.now().UTC()
	if err := m.record(ctx, state, EventEnvelope{}, SagaHistoryCompensationStarted, step, uint32(state.Compensation.Attempts), Command{EffectID: step}, state.Compensation.LastError); err != nil {
		return nil, err
	}
	if err := m.state.Save(ctx, state); err != nil {
		return nil, fmt.Errorf("save compensation state: %w", err)
	}
	return state, nil
}

func (m *SagaManager) CompleteCompensation(ctx context.Context, associationKey string) error {
	if m.transaction != nil {
		return m.transaction.WithinSagaTransaction(ctx, func(txCtx context.Context, stores SagaTransactionStores) error {
			if stores.State == nil || stores.History == nil {
				return fmt.Errorf("saga transaction stores are incomplete")
			}
			local := *m
			local.state, local.history, local.transaction = stores.State, stores.History, nil
			return local.completeCompensation(txCtx, associationKey)
		})
	}
	return m.completeCompensation(ctx, associationKey)
}
func (m *SagaManager) completeCompensation(ctx context.Context, associationKey string) error {
	state, err := m.loadOrCreateState(ctx, associationKey)
	if err != nil {
		return err
	}
	if state.Compensation == nil || state.Compensation.Step == "" {
		return fmt.Errorf("compensation is not active")
	}
	state.Compensation.Status = SagaCompleted
	state.Compensation.LastError = ""
	state.Compensation.UpdatedAt = m.now().UTC()
	state.Status = SagaCompleted
	state.Outcome = SagaOutcomeCompensated
	state.UpdatedAt = m.now().UTC()
	if err := m.record(ctx, state, EventEnvelope{}, SagaHistoryCompensationCompleted, state.Compensation.Step, uint32(state.Compensation.Attempts), Command{EffectID: state.Compensation.Step}, ""); err != nil {
		return err
	}
	if err := m.record(ctx, state, EventEnvelope{}, SagaHistoryRunCompensated, "", 0, Command{}, ""); err != nil {
		return err
	}
	if err := m.state.Save(ctx, state); err != nil {
		return fmt.Errorf("save compensation state: %w", err)
	}
	return nil
}

func (m *SagaManager) FailCompensation(ctx context.Context, associationKey string, cause error) error {
	if m.transaction != nil {
		return m.transaction.WithinSagaTransaction(ctx, func(txCtx context.Context, stores SagaTransactionStores) error {
			if stores.State == nil || stores.History == nil {
				return fmt.Errorf("saga transaction stores are incomplete")
			}
			local := *m
			local.state, local.history, local.transaction = stores.State, stores.History, nil
			return local.failCompensation(txCtx, associationKey, cause)
		})
	}
	return m.failCompensation(ctx, associationKey, cause)
}
func (m *SagaManager) failCompensation(ctx context.Context, associationKey string, cause error) error {
	if cause == nil {
		return fmt.Errorf("compensation failure is required")
	}
	if err := m.updateCompensation(ctx, associationKey, cause, SagaFailed); err != nil {
		return err
	}
	if state.Compensation == nil || state.Compensation.Step == "" {
		return fmt.Errorf("compensation is not active")
	}
	state.Compensation.Status = SagaFailed
	state.Compensation.LastError = cause.Error()
	state.Compensation.UpdatedAt = m.now().UTC()
	state.Status = SagaFailed
	state.Outcome = SagaOutcomeFailed
	state.UpdatedAt = m.now().UTC()
	if err := m.record(ctx, state, EventEnvelope{}, SagaHistoryCompensationFailed, state.Compensation.Step, uint32(state.Compensation.Attempts), Command{EffectID: state.Compensation.Step}, cause.Error()); err != nil {
		return err
	}
	if err := m.record(ctx, state, EventEnvelope{}, SagaHistoryRunFailed, "", 0, Command{}, cause.Error()); err != nil {
		return err
	}
	if err := m.state.Save(ctx, state); err != nil {
		return fmt.Errorf("save compensation state: %w", err)
	}
	return cause
}

func (m *SagaManager) updateCompensation(ctx context.Context, associationKey string, cause error, status string) error {
	return m.repository.Transact(ctx, func(tx SagaTransaction) error {
		state, expectedVersion, err := m.loadOrCreateState(tx, associationKey)
		if err != nil {
			return err
		}
		if state.Compensation == nil || state.Compensation.Step == "" {
			return fmt.Errorf("compensation is not active")
		}
		state.Compensation.Status = status
		state.Compensation.LastError = ""
		if cause != nil {
			state.Compensation.LastError = cause.Error()
		}
		state.Compensation.UpdatedAt = m.now().UTC()
		state.Status = status
		state.UpdatedAt = m.now().UTC()
		if err := saveSagaState(tx, state, expectedVersion); err != nil {
			return fmt.Errorf("save compensation state: %w", err)
		}
		return nil
	})
}
