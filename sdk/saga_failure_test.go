package sdk

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingInbox struct {
	memoryInbox
	claimErr    error
	completeErr error
	failErr     error
}

func (i *failingInbox) Claim(ctx context.Context, consumer, eventID string) (bool, error) {
	if i.claimErr != nil {
		return false, i.claimErr
	}
	return i.memoryInbox.Claim(ctx, consumer, eventID)
}

func (i *failingInbox) Complete(ctx context.Context, consumer, eventID string) error {
	if i.completeErr != nil {
		return i.completeErr
	}
	return i.memoryInbox.Complete(ctx, consumer, eventID)
}

func (i *failingInbox) Fail(ctx context.Context, consumer, eventID string, cause error) error {
	if i.failErr != nil {
		return i.failErr
	}
	return i.memoryInbox.Fail(ctx, consumer, eventID, cause)
}

type failingSagaStore struct {
	memorySagaStore
	loadErr error
	saveErr error
}

func (s *failingSagaStore) Load(ctx context.Context, typ, id string) (*SagaState, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.memorySagaStore.Load(ctx, typ, id)
}

func (s *failingSagaStore) Save(ctx context.Context, state *SagaState) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	return s.memorySagaStore.Save(ctx, state)
}

type failingOutbox struct {
	memoryOutbox
	err error
}

func (o *failingOutbox) Enqueue(ctx context.Context, command Command) error {
	if o.err != nil {
		return o.err
	}
	return o.memoryOutbox.Enqueue(ctx, command)
}

func newSagaTestManager(t *testing.T, inbox InboxStore, state SagaStore, outbox OutboxStore, handler SagaHandler) *SagaManager {
	t.Helper()
	manager, err := NewSagaManager(SagaDefinition{
		Type:     "finish-game",
		Handlers: map[string]SagaHandler{"GameFinished": handler},
	}, inbox, state, outbox)
	require.NoError(t, err)
	return manager
}

func TestNewSagaManagerRejectsIncompleteDependencies(t *testing.T) {
	_, err := NewSagaManager(SagaDefinition{}, &memoryInbox{}, &memorySagaStore{}, &memoryOutbox{})
	require.EqualError(t, err, "saga definition requires a type and handlers")

	_, err = NewSagaManager(SagaDefinition{Type: "finish-game", Handlers: map[string]SagaHandler{"GameFinished": func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return nil, nil
	}}}, nil, &memorySagaStore{}, &memoryOutbox{})
	require.EqualError(t, err, "inbox, saga state, and outbox stores are required")
}

func TestSagaManagerRecordsInboxClaimFailure(t *testing.T) {
	inbox := &failingInbox{memoryInbox: memoryInbox{claimed: map[string]bool{}}, claimErr: errors.New("inbox unavailable")}
	manager := newSagaTestManager(t, inbox, &memorySagaStore{states: map[string]*SagaState{}}, &memoryOutbox{}, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return nil, nil
	})

	err := manager.Handle(context.Background(), sagaTestEvent())
	require.EqualError(t, err, "claim saga inbox: inbox unavailable")
	require.Empty(t, inbox.complete)
	require.Empty(t, inbox.fails)
}

func TestSagaManagerRecordsStateLoadFailure(t *testing.T) {
	inbox := &failingInbox{memoryInbox: memoryInbox{claimed: map[string]bool{}}}
	state := &failingSagaStore{memorySagaStore: memorySagaStore{states: map[string]*SagaState{}}, loadErr: errors.New("state unavailable")}
	manager := newSagaTestManager(t, inbox, state, &memoryOutbox{}, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return nil, nil
	})

	err := manager.Handle(context.Background(), sagaTestEvent())
	require.EqualError(t, err, "load saga state: state unavailable")
	require.Equal(t, []string{"event-1"}, inbox.fails)
}

func TestSagaManagerRecordsStateSaveFailure(t *testing.T) {
	inbox := &failingInbox{memoryInbox: memoryInbox{claimed: map[string]bool{}}}
	state := &failingSagaStore{memorySagaStore: memorySagaStore{states: map[string]*SagaState{}}, saveErr: errors.New("state write failed")}
	manager := newSagaTestManager(t, inbox, state, &memoryOutbox{}, func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
		saga.Status = SagaWaiting
		return nil, nil
	})

	err := manager.Handle(context.Background(), sagaTestEvent())
	require.EqualError(t, err, "save saga state: state write failed")
	require.Equal(t, []string{"event-1"}, inbox.fails)
}

func TestSagaManagerRecordsOutboxFailureAfterStateSave(t *testing.T) {
	inbox := &failingInbox{memoryInbox: memoryInbox{claimed: map[string]bool{}}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	outbox := &failingOutbox{err: errors.New("outbox unavailable")}
	manager := newSagaTestManager(t, inbox, state, outbox, func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
		saga.Status = SagaWaiting
		return []Command{{Type: "UpdatePlayerElo"}}, nil
	})

	err := manager.Handle(context.Background(), sagaTestEvent())
	require.EqualError(t, err, "enqueue saga command: outbox unavailable")
	require.Equal(t, SagaWaiting, state.states["finish-game:saga-1"].Status)
	require.Equal(t, []string{"event-1"}, inbox.fails)
}

func TestSagaManagerPropagatesInboxCompletionFailure(t *testing.T) {
	inbox := &failingInbox{
		memoryInbox: memoryInbox{claimed: map[string]bool{}},
		completeErr: errors.New("inbox completion failed"),
	}
	manager := newSagaTestManager(t, inbox, &memorySagaStore{states: map[string]*SagaState{}}, &memoryOutbox{}, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return nil, nil
	})

	err := manager.Handle(context.Background(), sagaTestEvent())
	require.EqualError(t, err, "complete saga inbox: inbox completion failed")
}

func TestSagaManagerCompletesUnhandledEventWithoutCreatingCommands(t *testing.T) {
	inbox := &failingInbox{memoryInbox: memoryInbox{claimed: map[string]bool{}}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	manager := newSagaTestManager(t, inbox, state, &memoryOutbox{}, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return nil, nil
	})

	event := sagaTestEvent()
	event.EventType = "GameStarted"
	require.NoError(t, manager.Handle(context.Background(), event))
	require.Equal(t, []string{"event-1"}, inbox.complete)
	require.Empty(t, state.states)
}

func TestSagaManagerUsesAggregateIDWhenCorrelationIDIsEmpty(t *testing.T) {
	inbox := &failingInbox{memoryInbox: memoryInbox{claimed: map[string]bool{}}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	outbox := &memoryOutbox{}
	manager := newSagaTestManager(t, inbox, state, outbox, func(_ context.Context, saga *SagaState, event EventEnvelope) ([]Command, error) {
		saga.Status = SagaWaiting
		return []Command{{Type: "UpdatePlayerElo"}}, nil
	})

	event := sagaTestEvent()
	event.CorrelationID = ""
	require.NoError(t, manager.Handle(context.Background(), event))
	require.Equal(t, SagaWaiting, state.states["finish-game:game-1"].Status)
	require.Len(t, outbox.commands, 1)
	require.True(t, strings.HasPrefix(outbox.commands[0].SagaID, "game-1"))
}
