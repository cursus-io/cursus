package sdk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type memoryInbox struct {
	claimed  map[string]bool
	complete []string
	fails    []string
}

func (m *memoryInbox) Claim(_ context.Context, consumer, eventID string) (bool, error) {
	key := consumer + ":" + eventID
	if m.claimed[key] {
		return false, nil
	}
	m.claimed[key] = true
	return true, nil
}
func (m *memoryInbox) Complete(_ context.Context, _, eventID string) error {
	m.complete = append(m.complete, eventID)
	return nil
}
func (m *memoryInbox) Fail(_ context.Context, _, eventID string, _ error) error {
	m.fails = append(m.fails, eventID)
	return nil
}

type memorySagaStore struct{ states map[string]*SagaState }

func (m *memorySagaStore) Load(_ context.Context, typ, id string) (*SagaState, error) {
	state := m.states[typ+":"+id]
	if state == nil {
		return nil, nil
	}
	copy := *state
	return &copy, nil
}
func (m *memorySagaStore) Save(_ context.Context, state *SagaState) error {
	copy := *state
	m.states[state.Type+":"+state.ID] = &copy
	return nil
}

type memoryOutbox struct{ commands []Command }

func (m *memoryOutbox) Enqueue(_ context.Context, command Command) error {
	m.commands = append(m.commands, command)
	return nil
}

func sagaTestEvent() EventEnvelope {
	return EventEnvelope{EventID: "event-1", EventType: "GameFinished", AggregateType: "game", AggregateID: "game-1", AggregateVersion: 1, SchemaVersion: 1, OccurredAt: time.Now().UTC(), CorrelationID: "saga-1", Payload: []byte(`{"winner":"p1"}`)}
}

func TestSagaManagerClaimsOnceAndEnqueuesCorrelatedCommand(t *testing.T) {
	inbox := &memoryInbox{claimed: map[string]bool{}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	outbox := &memoryOutbox{}
	manager, err := NewSagaManager(SagaDefinition{Type: "finish-game", Handlers: map[string]SagaHandler{
		"GameFinished": func(_ context.Context, saga *SagaState, event EventEnvelope) ([]Command, error) {
			saga.Step, saga.Status = "update-elo", SagaWaiting
			return []Command{{Type: "UpdatePlayerElo", Payload: `{"player":"p1"}`}}, nil
		},
	}}, inbox, state, outbox)
	require.NoError(t, err)

	event := sagaTestEvent()
	require.NoError(t, manager.Handle(context.Background(), event))
	require.NoError(t, manager.Handle(context.Background(), event))
	require.Len(t, outbox.commands, 1)
	require.Equal(t, "saga-1", outbox.commands[0].SagaID)
	require.Equal(t, event.EventID, outbox.commands[0].CausationID)
	require.Len(t, inbox.complete, 1)
	require.Equal(t, SagaWaiting, state.states["finish-game:saga-1"].Status)
}

func TestSagaManagerRecordsFailureAndRetryCount(t *testing.T) {
	inbox := &memoryInbox{claimed: map[string]bool{}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	manager, err := NewSagaManager(SagaDefinition{Type: "finish-game", Handlers: map[string]SagaHandler{
		"GameFinished": func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
			return nil, errors.New("elo service unavailable")
		},
	}}, inbox, state, &memoryOutbox{})
	require.NoError(t, err)
	err = manager.Handle(context.Background(), sagaTestEvent())
	require.EqualError(t, err, "elo service unavailable")
	require.Equal(t, 1, state.states["finish-game:saga-1"].RetryCount)
	require.Len(t, inbox.fails, 1)
}
