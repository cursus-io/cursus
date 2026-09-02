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

// AcknowledgeEffect moves a durable outbox effect to SUCCEEDED. commandID fences
// stale acknowledgements from an older attempt of the same logical effect.
func (m *SagaManager) AcknowledgeEffect(ctx context.Context, associationKey, effectID, commandID string) error {
	return m.updateEffect(ctx, associationKey, effectID, commandID, EffectSucceeded, nil)
}

// FailEffect records a command delivery or side-effect failure without allowing
// a stale command attempt to overwrite the current effect.
func (m *SagaManager) FailEffect(ctx context.Context, associationKey, effectID, commandID string, cause error) error {
	if cause == nil {
		return fmt.Errorf("effect failure is required")
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
	if step == "" {
		return nil, fmt.Errorf("compensation step is required")
	}
	var result *SagaState
	err := m.repository.Transact(ctx, func(tx SagaTransaction) error {
		state, expectedVersion, err := m.loadOrCreateState(tx, associationKey)
		if err != nil {
			return err
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
		state.Compensation.UpdatedAt = m.now().UTC()
		state.Status = SagaCompensating
		state.UpdatedAt = m.now().UTC()
		if err := saveSagaState(tx, state, expectedVersion); err != nil {
			return fmt.Errorf("save compensation state: %w", err)
		}
		result = state
		return nil
	})
	return result, err
}

func (m *SagaManager) CompleteCompensation(ctx context.Context, associationKey string) error {
	return m.updateCompensation(ctx, associationKey, nil, SagaCompleted)
}

func (m *SagaManager) FailCompensation(ctx context.Context, associationKey string, cause error) error {
	if cause == nil {
		return fmt.Errorf("compensation failure is required")
	}
	if err := m.updateCompensation(ctx, associationKey, cause, SagaFailed); err != nil {
		return err
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
