package sdk

import (
	"context"
	"fmt"
	"time"
)

const (
	SagaRunning      = "RUNNING"
	SagaWaiting      = "WAITING"
	SagaCompleted    = "COMPLETED"
	SagaCompensating = "COMPENSATING"
	SagaFailed       = "FAILED"
)

// SagaState is the durable application-owned state of one saga instance.
// Version is incremented by every successful mutation and fenced by SaveCAS.
type SagaState struct {
	ID             string
	Type           string
	AssociationKey string
	CorrelationID  string
	Status         string
	Step           string
	Data           string
	RetryCount     int
	LastError      string
	UpdatedAt      time.Time
	Version        uint64
	Effects        map[string]EffectState
	Compensation   *CompensationState
}

// Command is an application command emitted by a saga.
type Command struct {
	ID            string
	EffectID      string
	Type          string
	SagaID        string
	CorrelationID string
	CausationID   string
	Payload       string
}

// SagaTransaction is the single atomic durability boundary for an inbox claim,
// saga state CAS, and outbox inserts. Implementations must roll back every
// operation when the callback returned to SagaRepository.Transact fails.
type SagaTransaction interface {
	Claim(consumer, eventID string) (bool, error)
	Load(sagaType, associationKey string) (*SagaState, error)
	SaveCAS(state *SagaState, expectedVersion uint64) error
	Enqueue(command Command) error
	Complete(consumer, eventID string) error
	Fail(consumer, eventID string, cause error) error
}

// SagaRepository runs one serializable local transaction. A callback error must
// leave the inbox, saga state, and outbox unchanged.
type SagaRepository interface {
	Transact(context.Context, func(SagaTransaction) error) error
}

// SagaHandler applies one event to a saga and returns commands to enqueue.
type SagaHandler func(context.Context, *SagaState, EventEnvelope) ([]Command, error)

// SagaDefinition describes event handlers for one saga type.
type SagaDefinition struct {
	Type     string
	Handlers map[string]SagaHandler
}

// SagaManager coordinates inbox, state, and outbox through one transaction.
type SagaManager struct {
	definition SagaDefinition
	repository SagaRepository
	now        func() time.Time
}

func NewSagaManager(definition SagaDefinition, repository SagaRepository) (*SagaManager, error) {
	if definition.Type == "" || len(definition.Handlers) == 0 {
		return nil, fmt.Errorf("saga definition requires a type and handlers")
	}
	if repository == nil {
		return nil, fmt.Errorf("saga repository is required")
	}
	return &SagaManager{definition: definition, repository: repository, now: time.Now}, nil
}

// Handle processes one event in a single atomic transaction. Duplicate claims
// are harmless, and a crash cannot expose state without its outbox commands.
func (m *SagaManager) Handle(ctx context.Context, event EventEnvelope) error {
	if event.EventID == "" || event.EventType == "" {
		return fmt.Errorf("saga event identity is incomplete")
	}
	associationKey := event.AssociationKey
	if associationKey == "" {
		associationKey = event.CorrelationID
	}
	if associationKey == "" {
		associationKey = event.AggregateID
	}
	if associationKey == "" {
		return fmt.Errorf("saga association key is required")
	}

	var handlerFailure error
	err := m.repository.Transact(ctx, func(tx SagaTransaction) error {
		claimed, err := tx.Claim(m.definition.Type, event.EventID)
		if err != nil {
			return fmt.Errorf("claim saga inbox: %w", err)
		}
		if !claimed {
			return nil
		}

		state, expectedVersion, err := m.loadOrCreateState(tx, associationKey)
		if err != nil {
			return err
		}
		if state.CorrelationID == "" {
			state.CorrelationID = event.CorrelationID
		}

		handler, ok := m.definition.Handlers[event.EventType]
		if !ok {
			return tx.Complete(m.definition.Type, event.EventID)
		}

		commands, handleErr := handler(ctx, state, event)
		if handleErr != nil {
			state.RetryCount++
			state.LastError = handleErr.Error()
			state.UpdatedAt = m.now().UTC()
			if err := saveSagaState(tx, state, expectedVersion); err != nil {
				return fmt.Errorf("save failed saga state: %w", err)
			}
			if err := tx.Fail(m.definition.Type, event.EventID, handleErr); err != nil {
				return fmt.Errorf("record saga inbox failure: %w", err)
			}
			handlerFailure = handleErr
			return nil
		}

		state.LastError = ""
		for index, command := range commands {
			if command.Type == "" {
				return fmt.Errorf("saga command type is required at index %d", index)
			}
			effectID := command.EffectID
			if effectID == "" {
				effectID = fmt.Sprintf("%s:%d", event.EventID, index)
			}
			if effect, exists := state.Effects[effectID]; exists && (effect.Status == EffectEnqueued || effect.Status == EffectSucceeded) {
				continue
			}
			command = m.prepareCommand(command, state, event.EventID, effectID)
			if err := tx.Enqueue(command); err != nil {
				return fmt.Errorf("enqueue saga command: %w", err)
			}
			effect := state.Effects[effectID]
			effect.ID = effectID
			effect.Step = command.Type
			effect.Status = EffectEnqueued
			effect.CommandID = command.ID
			effect.Attempts++
			effect.LastError = ""
			effect.UpdatedAt = m.now().UTC()
			state.Effects[effectID] = effect
		}
		state.UpdatedAt = m.now().UTC()
		if err := saveSagaState(tx, state, expectedVersion); err != nil {
			return fmt.Errorf("save saga state: %w", err)
		}
		if err := tx.Complete(m.definition.Type, event.EventID); err != nil {
			return fmt.Errorf("complete saga inbox: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return handlerFailure
}

func (m *SagaManager) prepareCommand(command Command, state *SagaState, causationID, effectID string) Command {
	command.EffectID = effectID
	if command.SagaID == "" {
		command.SagaID = state.ID
	}
	if command.CorrelationID == "" {
		command.CorrelationID = state.CorrelationID
	}
	if command.CausationID == "" {
		command.CausationID = causationID
	}
	command.ID = m.definition.Type + ":" + state.ID + ":" + effectID
	return command
}

func (m *SagaManager) loadOrCreateState(tx SagaTransaction, associationKey string) (*SagaState, uint64, error) {
	if associationKey == "" {
		return nil, 0, fmt.Errorf("association key is required")
	}
	state, err := tx.Load(m.definition.Type, associationKey)
	if err != nil {
		return nil, 0, fmt.Errorf("load saga state: %w", err)
	}
	if state == nil {
		state = &SagaState{ID: associationKey, Type: m.definition.Type, AssociationKey: associationKey, Status: SagaRunning}
	}
	if state.Effects == nil {
		state.Effects = make(map[string]EffectState)
	}
	return state, state.Version, nil
}

func saveSagaState(tx SagaTransaction, state *SagaState, expectedVersion uint64) error {
	state.Version = expectedVersion + 1
	if err := tx.SaveCAS(state, expectedVersion); err != nil {
		state.Version = expectedVersion
		return err
	}
	return nil
}
