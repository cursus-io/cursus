package sdk

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSagaHistoryJSONMatchesV1Fixture(t *testing.T) {
	fixture, err := os.ReadFile("../contracts/fixtures/saga-history-v1.json")
	require.NoError(t, err)
	var expected map[string]any
	require.NoError(t, json.Unmarshal(fixture, &expected))

	actual, err := json.Marshal(SagaHistoryEvent{
		HistorySchemaVersion: SagaHistorySchemaV1,
		HistoryEventID:       "a4c9f9a0-291e-41e5-babb-3c13b84c4bb4",
		EnvironmentID:        "development",
		ServiceName:          "orders",
		SagaType:             "order-fulfillment",
		SagaID:               "order-42",
		RunID:                "de94b8eb-50c4-4a35-b324-59b9318af658",
		Sequence:             3,
		EventType:            SagaHistoryCommandEnqueued,
		OccurredAt:           time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC),
		RecordedAt:           time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC),
		StepID:               "reserve-inventory",
		Attempt:              1,
		CommandID:            "reserve-42",
		EffectID:             "reserve-inventory:1",
		SourceEventID:        "event-order-42",
		CorrelationID:        "order-42",
		SourceTopic:          "orders.events",
		SourcePartition:      2,
		SourceOffset:         9007199254740993,
		AggregateType:        "order",
		AggregateID:          "order-42",
		AggregateVersion:     7,
		Payload:              `{"order_id":"order-42"}`,
	})
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(actual, &got))
	require.Equal(t, expected, got)
}

type memoryHistoryStore struct{ events []SagaHistoryEvent }

func (m *memoryHistoryStore) AppendSagaHistory(_ context.Context, event SagaHistoryEvent) error {
	m.events = append(m.events, event)
	return nil
}

type memorySagaTransaction struct{ stores SagaTransactionStores }

func (m memorySagaTransaction) WithinSagaTransaction(ctx context.Context, fn func(context.Context, SagaTransactionStores) error) error {
	return fn(ctx, m.stores)
}

func TestTransactionalSagaManagerRecordsOrderedHistoryWithoutClaimingExternalSuccess(t *testing.T) {
	inbox := &memoryInbox{claimed: map[string]bool{}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	outbox := &memoryOutbox{}
	history := &memoryHistoryStore{}
	manager, err := NewTransactionalSagaManager(SagaDefinition{Type: "orders", Handlers: map[string]SagaHandler{
		"OrderCreated": func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
			saga.Step, saga.Status = "reserve-inventory", SagaWaiting
			return []Command{{Type: "ReserveInventory", Payload: `{"order":"1"}`}}, nil
		},
	}}, memorySagaTransaction{stores: SagaTransactionStores{Inbox: inbox, State: state, Outbox: outbox, History: history}}, SagaHistoryOptions{EnvironmentID: "test", ServiceName: "orders"})
	require.NoError(t, err)
	event := sagaTestEvent()
	event.EventType = "OrderCreated"
	event.SourceTopic, event.SourcePartition, event.SourceOffset = "orders.events", 2, 9007199254740993
	require.NoError(t, manager.Handle(context.Background(), event))
	require.Len(t, history.events, 5)
	require.Equal(t, []string{SagaHistoryRunStarted, SagaHistoryStepStarted, SagaHistoryCommandEnqueued, SagaHistoryStepCompleted, SagaHistoryRunWaiting}, []string{history.events[0].EventType, history.events[1].EventType, history.events[2].EventType, history.events[3].EventType, history.events[4].EventType})
	for index, recorded := range history.events {
		require.Equal(t, uint64(index+1), recorded.Sequence)
		require.NotEmpty(t, recorded.HistoryEventID)
		require.NotEmpty(t, recorded.RunID)
		require.NotEqual(t, SagaHistoryCommandSucceeded, recorded.EventType)
	}
	require.Equal(t, "orders.events", history.events[0].SourceTopic)
	require.Equal(t, 2, history.events[0].SourcePartition)
	require.Equal(t, uint64(9007199254740993), history.events[0].SourceOffset)
	require.Equal(t, "game", history.events[0].AggregateType)
	require.Equal(t, "game-1", history.events[0].AggregateID)
	require.Len(t, outbox.commands, 1)
	require.Equal(t, uint64(5), state.states["orders:saga-1"].NextSequence)
	// A duplicate source event is stopped by the transaction-scoped inbox and
	// therefore cannot create a second run or a second history sequence.
	require.NoError(t, manager.Handle(context.Background(), event))
	require.Len(t, history.events, 5)
}

func TestTransactionalSagaManagerRecordsCompletedRun(t *testing.T) {
	inbox := &memoryInbox{claimed: map[string]bool{}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	history := &memoryHistoryStore{}
	manager, err := NewTransactionalSagaManager(SagaDefinition{Type: "orders", Handlers: map[string]SagaHandler{
		"OrderCreated": func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
			saga.Step, saga.Status = "finish", SagaCompleted
			return nil, nil
		},
	}}, memorySagaTransaction{stores: SagaTransactionStores{Inbox: inbox, State: state, Outbox: &memoryOutbox{}, History: history}}, SagaHistoryOptions{EnvironmentID: "test", ServiceName: "orders"})
	require.NoError(t, err)
	event := sagaTestEvent()
	event.EventType = "OrderCreated"
	require.NoError(t, manager.Handle(context.Background(), event))
	require.Equal(t, []string{SagaHistoryRunStarted, SagaHistoryStepStarted, SagaHistoryStepCompleted, SagaHistoryRunCompleted}, []string{history.events[0].EventType, history.events[1].EventType, history.events[2].EventType, history.events[3].EventType})
	require.Equal(t, SagaOutcomeSucceeded, state.states["orders:saga-1"].Outcome)
}

func TestTransactionalSagaManagerCreatesNewRunOnlyExplicitly(t *testing.T) {
	inbox := &memoryInbox{claimed: map[string]bool{}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	history := &memoryHistoryStore{}
	manager, err := NewTransactionalSagaManager(SagaDefinition{Type: "orders", Handlers: map[string]SagaHandler{
		"OrderCreated": func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
			saga.Status = SagaCompleted
			return nil, nil
		},
	}}, memorySagaTransaction{stores: SagaTransactionStores{Inbox: inbox, State: state, Outbox: &memoryOutbox{}, History: history}}, SagaHistoryOptions{EnvironmentID: "test", ServiceName: "orders"})
	require.NoError(t, err)
	event := sagaTestEvent()
	event.EventType = "OrderCreated"
	require.NoError(t, manager.Handle(context.Background(), event))
	firstRun := state.states["orders:saga-1"].RunID
	started, err := manager.StartNewRun(context.Background(), "saga-1")
	require.NoError(t, err)
	require.NotEqual(t, firstRun, started.RunID)
	require.Equal(t, uint64(1), started.NextSequence)
	require.Equal(t, SagaHistoryRunStarted, history.events[len(history.events)-1].EventType)
}

func TestTransactionalSagaManagerRecordsExternallyConfirmedEffectResult(t *testing.T) {
	inbox := &memoryInbox{claimed: map[string]bool{}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	history := &memoryHistoryStore{}
	manager, err := NewTransactionalSagaManager(SagaDefinition{Type: "orders", Handlers: map[string]SagaHandler{
		"OrderCreated": func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
			saga.Status = SagaWaiting
			return []Command{{EffectID: "reserve", Type: "ReserveInventory"}}, nil
		},
	}}, memorySagaTransaction{stores: SagaTransactionStores{Inbox: inbox, State: state, Outbox: &memoryOutbox{}, History: history}}, SagaHistoryOptions{EnvironmentID: "test", ServiceName: "orders"})
	require.NoError(t, err)
	event := sagaTestEvent()
	event.EventType = "OrderCreated"
	require.NoError(t, manager.Handle(context.Background(), event))
	require.NoError(t, manager.RecordEffectResult(context.Background(), "saga-1", "reserve", true, nil))
	require.Equal(t, EffectSucceeded, state.states["orders:saga-1"].Effects["reserve"].Status)
	require.Equal(t, SagaHistoryCommandSucceeded, history.events[len(history.events)-1].EventType)
}

func TestTransactionalSagaManagerRecordsPublishedCommandWithoutClaimingResult(t *testing.T) {
	inbox := &memoryInbox{claimed: map[string]bool{}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	history := &memoryHistoryStore{}
	manager, err := NewTransactionalSagaManager(SagaDefinition{Type: "orders", Handlers: map[string]SagaHandler{
		"OrderCreated": func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
			saga.Status = SagaWaiting
			return []Command{{EffectID: "reserve", Type: "ReserveInventory"}}, nil
		},
	}}, memorySagaTransaction{stores: SagaTransactionStores{Inbox: inbox, State: state, Outbox: &memoryOutbox{}, History: history}}, SagaHistoryOptions{EnvironmentID: "test", ServiceName: "orders"})
	require.NoError(t, err)
	event := sagaTestEvent()
	event.EventType = "OrderCreated"
	require.NoError(t, manager.Handle(context.Background(), event))
	require.NoError(t, manager.RecordCommandPublished(context.Background(), "saga-1", "reserve"))
	require.True(t, state.states["orders:saga-1"].Effects["reserve"].Published)
	require.Equal(t, EffectPending, state.states["orders:saga-1"].Effects["reserve"].Status)
	require.Equal(t, SagaHistoryCommandPublished, history.events[len(history.events)-1].EventType)
	before := len(history.events)
	require.NoError(t, manager.RecordCommandPublished(context.Background(), "saga-1", "reserve"))
	require.Len(t, history.events, before)
}
