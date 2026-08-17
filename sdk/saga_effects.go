package sdk

import (
	"context"
	"fmt"
	"time"
)

const (
	EffectPending   = "PENDING"
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

func (m *SagaManager) loadOrCreateState(ctx context.Context, associationKey string) (*SagaState, error) {
	if associationKey == "" {
		return nil, fmt.Errorf("association key is required")
	}
	state, err := m.state.Load(ctx, m.definition.Type, associationKey)
	if err != nil {
		return nil, fmt.Errorf("load saga state: %w", err)
	}
	if state == nil {
		state = &SagaState{ID: associationKey, Type: m.definition.Type, AssociationKey: associationKey, Status: SagaRunning}
	}
	if state.Effects == nil {
		state.Effects = make(map[string]EffectState)
	}
	return state, nil
}

func (m *SagaManager) StartCompensation(ctx context.Context, associationKey, step string, cause error) (*SagaState, error) {
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
	state.UpdatedAt = m.now().UTC()
	if err := m.state.Save(ctx, state); err != nil {
		return nil, fmt.Errorf("save compensation state: %w", err)
	}
	return state, nil
}

func (m *SagaManager) CompleteCompensation(ctx context.Context, associationKey string) error {
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
	state.UpdatedAt = m.now().UTC()
	if err := m.state.Save(ctx, state); err != nil {
		return fmt.Errorf("save compensation state: %w", err)
	}
	return nil
}

func (m *SagaManager) FailCompensation(ctx context.Context, associationKey string, cause error) error {
	if cause == nil {
		return fmt.Errorf("compensation failure is required")
	}
	state, err := m.loadOrCreateState(ctx, associationKey)
	if err != nil {
		return err
	}
	if state.Compensation == nil || state.Compensation.Step == "" {
		return fmt.Errorf("compensation is not active")
	}
	state.Compensation.Status = SagaFailed
	state.Compensation.LastError = cause.Error()
	state.Compensation.UpdatedAt = m.now().UTC()
	state.Status = SagaFailed
	state.UpdatedAt = m.now().UTC()
	if err := m.state.Save(ctx, state); err != nil {
		return fmt.Errorf("save compensation state: %w", err)
	}
	return cause
}
