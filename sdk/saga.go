//go:build legacy_sql_saga

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
	RunID          string
	NextSequence   uint64
	Outcome        string
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
	SagaType      string
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
	definition           SagaDefinition
	inbox                InboxStore
	state                SagaStore
	outbox               OutboxStore
	history              SagaHistoryStore
	historyOptions       SagaHistoryOptions
	transaction          SagaTransaction
	transactionalAttempt bool
	now                  func() time.Time
}

// NewTransactionalSagaManager creates the opt-in path for services which need
// an atomic PostgreSQL (or equivalent) boundary. Existing NewSagaManager
// callers remain supported, but must not claim cross-store atomicity.
func NewTransactionalSagaManager(definition SagaDefinition, transaction SagaTransaction, options SagaHistoryOptions) (*SagaManager, error) {
	if definition.Type == "" || len(definition.Handlers) == 0 {
		return nil, fmt.Errorf("saga definition requires a type and handlers")
	}
	if transaction == nil {
		return nil, fmt.Errorf("saga transaction is required")
	}
	if err := options.validate(); err != nil {
		return nil, err
	}
	return &SagaManager{definition: definition, transaction: transaction, historyOptions: options, now: time.Now}, nil
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
	if m.transaction != nil {
		return m.handleTransactional(ctx, event)
	}
	return m.handle(ctx, event)
}

// StartNewRun explicitly creates another run for an already terminal Saga
// instance. Handle never infers a new run from a duplicate or later event;
// callers must opt in through this method so historical runs remain distinct.
func (m *SagaManager) StartNewRun(ctx context.Context, associationKey string) (*SagaState, error) {
	if associationKey == "" {
		return nil, fmt.Errorf("association key is required")
	}
	if m.transaction != nil {
		var result *SagaState
		err := m.transaction.WithinSagaTransaction(ctx, func(txCtx context.Context, stores SagaTransactionStores) error {
			if stores.State == nil || stores.History == nil {
				return fmt.Errorf("saga transaction stores are incomplete")
			}
			local := *m
			local.state, local.history, local.transaction = stores.State, stores.History, nil
			var startErr error
			result, startErr = local.startNewRun(txCtx, associationKey)
			return startErr
		})
		return result, err
	}
	return m.startNewRun(ctx, associationKey)
}

func (m *SagaManager) startNewRun(ctx context.Context, associationKey string) (*SagaState, error) {
	state, err := m.state.Load(ctx, m.definition.Type, associationKey)
	if err != nil {
		return nil, fmt.Errorf("load saga state: %w", err)
	}
	if state == nil {
		return nil, fmt.Errorf("cannot start a new run before an initial run exists")
	}
	if state.Status != SagaCompleted && state.Status != SagaFailed {
		return nil, fmt.Errorf("cannot start a new run while saga status is %s", state.Status)
	}
	state.RunID, state.NextSequence, state.Status, state.Outcome = newSagaRunID(), 0, SagaRunning, ""
	state.Step, state.RetryCount, state.LastError, state.Compensation = "", 0, "", nil
	state.Effects, state.UpdatedAt = make(map[string]EffectState), m.now().UTC()
	if err := m.record(ctx, state, EventEnvelope{}, SagaHistoryRunStarted, "", 0, Command{}, ""); err != nil {
		return nil, err
	}
	if err := m.state.Save(ctx, state); err != nil {
		return nil, fmt.Errorf("save new saga run: %w", err)
	}
	return state, nil
}

func (m *SagaManager) handle(ctx context.Context, event EventEnvelope) error {
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
		state = &SagaState{ID: associationKey, Type: m.definition.Type, AssociationKey: associationKey, CorrelationID: event.CorrelationID, Status: SagaRunning, RunID: newSagaRunID()}
		if err := m.record(ctx, state, event, SagaHistoryRunStarted, "", 0, Command{}, ""); err != nil {
			return m.fail(ctx, associationKey, event.EventID, state, err)
		}
	}
	if state.Effects == nil {
		state.Effects = make(map[string]EffectState)
	}
	handler, ok := m.definition.Handlers[event.EventType]
	if !ok {
		return m.complete(ctx, associationKey, event.EventID, state)
	}
	attempt := uint32(state.RetryCount + 1)
	commands, err := handler(ctx, state, event)
	if err != nil {
		state.RetryCount++
		state.LastError = err.Error()
		return m.fail(ctx, associationKey, event.EventID, state, err)
	}
	stepID := state.Step
	if stepID == "" {
		stepID = event.EventType
	}
	if err := m.record(ctx, state, event, SagaHistoryStepStarted, stepID, attempt, Command{}, ""); err != nil {
		return m.fail(ctx, associationKey, event.EventID, state, err)
	}
	state.UpdatedAt = m.now().UTC()
	if err := m.state.Save(ctx, state); err != nil {
		return m.fail(ctx, associationKey, event.EventID, state, fmt.Errorf("save saga state: %w", err))
	}
	for index, command := range commands {
		effectID := command.EffectID
		if effectID == "" {
			effectID = fmt.Sprintf("%s:%d", event.EventID, index)
		}
		if effect, ok := state.Effects[effectID]; ok && effect.Status != EffectFailed {
			continue
		}
		if command.SagaID == "" {
			command.SagaID = state.ID
		}
		if command.SagaType == "" {
			command.SagaType = state.Type
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
		command.EffectID = effectID
		effect := state.Effects[effectID]
		effect.ID = effectID
		effect.Step = command.Type
		effect.Attempts++
		effect.Status = EffectPending
		effect.CommandID = command.ID
		effect.UpdatedAt = m.now().UTC()
		state.Effects[effectID] = effect
		if err := m.outbox.Enqueue(ctx, command); err != nil {
			effect.Status = EffectFailed
			effect.LastError = err.Error()
			effect.UpdatedAt = m.now().UTC()
			state.Effects[effectID] = effect
			return m.fail(ctx, associationKey, event.EventID, state, fmt.Errorf("enqueue saga command: %w", err))
		}
		if err := m.record(ctx, state, event, SagaHistoryCommandEnqueued, command.Type, uint32(effect.Attempts), command, ""); err != nil {
			return m.fail(ctx, associationKey, event.EventID, state, err)
		}
		if err := m.state.Save(ctx, state); err != nil {
			return m.fail(ctx, associationKey, event.EventID, state, fmt.Errorf("save saga effect: %w", err))
		}
	}
	if err := m.record(ctx, state, event, SagaHistoryStepCompleted, stepID, attempt, Command{}, ""); err != nil {
		return m.fail(ctx, associationKey, event.EventID, state, err)
	}
	if state.Status == SagaWaiting {
		if err := m.record(ctx, state, event, SagaHistoryRunWaiting, stepID, attempt, Command{}, ""); err != nil {
			return m.fail(ctx, associationKey, event.EventID, state, err)
		}
	}
	if state.Status == SagaCompleted {
		state.Outcome = SagaOutcomeSucceeded
		if err := m.record(ctx, state, event, SagaHistoryRunCompleted, "", 0, Command{}, ""); err != nil {
			return m.fail(ctx, associationKey, event.EventID, state, err)
		}
	}
	if state.Status == SagaFailed {
		state.Outcome = SagaOutcomeFailed
		if err := m.record(ctx, state, event, SagaHistoryRunFailed, "", 0, Command{}, state.LastError); err != nil {
			return m.fail(ctx, associationKey, event.EventID, state, err)
		}
	}
	if err := m.state.Save(ctx, state); err != nil {
		return m.fail(ctx, associationKey, event.EventID, state, fmt.Errorf("save saga history sequence: %w", err))
	}
	return m.complete(ctx, associationKey, event.EventID, state)
}

func (m *SagaManager) handleTransactional(ctx context.Context, event EventEnvelope) error {
	if event.EventID == "" || event.EventType == "" {
		return fmt.Errorf("saga event identity is incomplete")
	}
	associationKey := sagaAssociationKey(event)
	err := m.transaction.WithinSagaTransaction(ctx, func(txCtx context.Context, stores SagaTransactionStores) error {
		if stores.Inbox == nil || stores.State == nil || stores.Outbox == nil || stores.History == nil {
			return fmt.Errorf("saga transaction stores are incomplete")
		}
		local := *m
		local.inbox, local.state, local.outbox, local.history, local.transaction = stores.Inbox, stores.State, stores.Outbox, stores.History, nil
		local.transactionalAttempt = true
		return local.handle(txCtx, event)
	})
	if err == nil {
		return nil
	}
	// The successful handler transaction has rolled back. Persist only a
	// separately provable failure for an already-existing run; never invent a
	// started timestamp/run when the initial transaction did not commit.
	_ = m.transaction.WithinSagaTransaction(ctx, func(txCtx context.Context, stores SagaTransactionStores) error {
		if stores.Inbox == nil || stores.State == nil || stores.History == nil {
			return fmt.Errorf("saga transaction stores are incomplete")
		}
		state, loadErr := stores.State.Load(txCtx, m.definition.Type, associationKey)
		if loadErr == nil && state != nil {
			state.RetryCount++
			state.LastError = err.Error()
			state.UpdatedAt = m.now().UTC()
			local := *m
			local.inbox, local.state, local.outbox, local.history, local.transaction = stores.Inbox, stores.State, stores.Outbox, stores.History, nil
			_ = local.record(txCtx, state, event, SagaHistoryStepFailed, state.Step, uint32(state.RetryCount), Command{}, err.Error())
			_ = stores.State.Save(txCtx, state)
		}
		return stores.Inbox.Fail(txCtx, m.definition.Type, event.EventID, err)
	})
	return err
}

func sagaAssociationKey(event EventEnvelope) string {
	if event.AssociationKey != "" {
		return event.AssociationKey
	}
	if event.CorrelationID != "" {
		return event.CorrelationID
	}
	return event.AggregateID
}

func (m *SagaManager) record(ctx context.Context, state *SagaState, source EventEnvelope, eventType, step string, attempt uint32, command Command, failure string) error {
	if m.history == nil {
		return nil
	}
	if state.RunID == "" {
		state.RunID = newSagaRunID()
	}
	state.NextSequence++
	now := m.now().UTC()
	return m.history.AppendSagaHistory(ctx, SagaHistoryEvent{HistorySchemaVersion: SagaHistorySchemaV1, HistoryEventID: newSagaHistoryEventID(), EnvironmentID: m.historyOptions.EnvironmentID, ServiceName: m.historyOptions.ServiceName, SagaType: state.Type, SagaID: state.ID, RunID: state.RunID, Sequence: state.NextSequence, EventType: eventType, OccurredAt: now, RecordedAt: now, StepID: step, Attempt: attempt, CommandID: command.ID, EffectID: command.EffectID, SourceEventID: source.EventID, CorrelationID: source.CorrelationID, CausationID: source.CausationID, SourceTopic: source.SourceTopic, SourcePartition: source.SourcePartition, SourceOffset: source.SourceOffset, AggregateType: source.AggregateType, AggregateID: source.AggregateID, AggregateVersion: source.AggregateVersion, Payload: command.Payload, Error: failure})
}

func (m *SagaManager) complete(ctx context.Context, associationKey, eventID string, state *SagaState) error {
	if err := m.inbox.Complete(ctx, m.definition.Type, eventID); err != nil {
		return fmt.Errorf("complete saga inbox: %w", err)
	}
	return nil
}

func (m *SagaManager) fail(ctx context.Context, associationKey, eventID string, state *SagaState, cause error) error {
	// A transactional attempt is rolled back as a whole. Its failure record is
	// written by handleTransactional in a fresh transaction, after rollback.
	// Writing it here can mask the original PostgreSQL serialization error with
	// "transaction is aborted", preventing the adapter from retrying safely.
	if m.transactionalAttempt {
		return cause
	}
	if state != nil {
		state.UpdatedAt = m.now().UTC()
		_ = m.state.Save(ctx, state)
	}
	if err := m.inbox.Fail(ctx, m.definition.Type, eventID, cause); err != nil {
		return fmt.Errorf("record saga inbox failure: %w", err)
	}
	return cause
}
