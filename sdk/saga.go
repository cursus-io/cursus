package sdk

import (
	"context"
	"fmt"
	"time"
)

// SagaState is the durable application-owned state of one saga instance.
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
}

const (
	SagaRunning      = "RUNNING"
	SagaWaiting      = "WAITING"
	SagaCompleted    = "COMPLETED"
	SagaCompensating = "COMPENSATING"
	SagaFailed       = "FAILED"
)

// Command is an application command emitted by a saga.
type Command struct {
	ID            string
	Type          string
	SagaID        string
	CorrelationID string
	CausationID   string
	Payload       string
}

// NewCommand creates a command with a unique delivery identity.
func NewCommand(commandType, sagaID, correlationID, causationID, payload string) Command {
	event, _ := NewEventEnvelope("command", sagaID, commandType, payload)
	return Command{ID: event.EventID, Type: commandType, SagaID: sagaID, CorrelationID: correlationID, CausationID: causationID, Payload: payload}
}

// InboxStore is implemented by the service database. Claim must be atomic on
// (consumer, eventID) and return false when the event was already claimed.
type InboxStore interface {
	Claim(context.Context, string, string) (bool, error)
	Complete(context.Context, string, string) error
	Fail(context.Context, string, string, error) error
}

// OutboxStore is implemented by the service database. Enqueue should be part
// of the same local transaction as the saga state update when possible.
type OutboxStore interface {
	Enqueue(context.Context, Command) error
}

// SagaStore persists saga state in the application-owned database.
type SagaStore interface {
	Load(context.Context, string, string) (*SagaState, error)
	Save(context.Context, *SagaState) error
}

// SagaHandler applies one event to a saga and returns commands to enqueue.
type SagaHandler func(context.Context, *SagaState, EventEnvelope) ([]Command, error)

// SagaDefinition describes event handlers for one saga type.
type SagaDefinition struct {
	Type     string
	Handlers map[string]SagaHandler
}

// SagaManager coordinates inbox claim, saga state, and outbox commands.
type SagaManager struct {
	definition SagaDefinition
	inbox      InboxStore
	state      SagaStore
	outbox     OutboxStore
	now        func() time.Time
}

func NewSagaManager(definition SagaDefinition, inbox InboxStore, state SagaStore, outbox OutboxStore) (*SagaManager, error) {
	if definition.Type == "" || len(definition.Handlers) == 0 {
		return nil, fmt.Errorf("saga definition requires a type and handlers")
	}
	if inbox == nil || state == nil || outbox == nil {
		return nil, fmt.Errorf("inbox, saga state, and outbox stores are required")
	}
	return &SagaManager{definition: definition, inbox: inbox, state: state, outbox: outbox, now: time.Now}, nil
}

// Handle processes an event at least once and safely ignores a duplicate claim.
func (m *SagaManager) Handle(ctx context.Context, event EventEnvelope) error {
	if event.EventID == "" || event.EventType == "" {
		return fmt.Errorf("saga event identity is incomplete")
	}
	associationKey := event.CorrelationID
	if associationKey == "" {
		associationKey = event.AggregateID
	}
	claimed, err := m.inbox.Claim(ctx, m.definition.Type, event.EventID)
	if err != nil {
		return fmt.Errorf("claim saga inbox: %w", err)
	}
	if !claimed {
		return nil
	}

	state, err := m.state.Load(ctx, m.definition.Type, associationKey)
	if err != nil {
		return m.fail(ctx, associationKey, event.EventID, nil, fmt.Errorf("load saga state: %w", err))
	}
	if state == nil {
		state = &SagaState{ID: associationKey, Type: m.definition.Type, AssociationKey: associationKey, CorrelationID: event.CorrelationID, Status: SagaRunning}
	}
	handler, ok := m.definition.Handlers[event.EventType]
	if !ok {
		return m.complete(ctx, associationKey, event.EventID, state)
	}
	commands, err := handler(ctx, state, event)
	if err != nil {
		state.RetryCount++
		state.LastError = err.Error()
		return m.fail(ctx, associationKey, event.EventID, state, err)
	}
	state.UpdatedAt = m.now().UTC()
	if err := m.state.Save(ctx, state); err != nil {
		return m.fail(ctx, associationKey, event.EventID, state, fmt.Errorf("save saga state: %w", err))
	}
	for _, command := range commands {
		if command.SagaID == "" {
			command.SagaID = state.ID
		}
		if command.CorrelationID == "" {
			command.CorrelationID = state.CorrelationID
		}
		if command.CausationID == "" {
			command.CausationID = event.EventID
		}
		if command.ID == "" {
			command = NewCommand(command.Type, command.SagaID, command.CorrelationID, command.CausationID, command.Payload)
		}
		if err := m.outbox.Enqueue(ctx, command); err != nil {
			return m.fail(ctx, associationKey, event.EventID, state, fmt.Errorf("enqueue saga command: %w", err))
		}
	}
	return m.complete(ctx, associationKey, event.EventID, state)
}

func (m *SagaManager) complete(ctx context.Context, associationKey, eventID string, state *SagaState) error {
	if err := m.inbox.Complete(ctx, m.definition.Type, eventID); err != nil {
		return fmt.Errorf("complete saga inbox: %w", err)
	}
	return nil
}

func (m *SagaManager) fail(ctx context.Context, associationKey, eventID string, state *SagaState, cause error) error {
	if state != nil {
		state.UpdatedAt = m.now().UTC()
		_ = m.state.Save(ctx, state)
	}
	if err := m.inbox.Fail(ctx, m.definition.Type, eventID, cause); err != nil {
		return fmt.Errorf("record saga inbox failure: %w", err)
	}
	return cause
}
