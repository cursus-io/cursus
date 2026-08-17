package sdk

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSagaManagerPersistsEffectAndSkipsSucceededEffect(t *testing.T) {
	inbox := &memoryInbox{claimed: map[string]bool{}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	outbox := &memoryOutbox{}
	manager := newSagaTestManager(t, inbox, state, outbox, func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
		saga.Status = SagaWaiting
		return []Command{{EffectID: "update-elo", Type: "UpdatePlayerElo"}}, nil
	})

	first := sagaTestEvent()
	require.NoError(t, manager.Handle(context.Background(), first))
	require.Len(t, outbox.commands, 1)
	require.Equal(t, EffectSucceeded, state.states["finish-game:saga-1"].Effects["update-elo"].Status)

	second := sagaTestEvent()
	second.EventID = "event-2"
	require.NoError(t, manager.Handle(context.Background(), second))
	require.Len(t, outbox.commands, 1)
	require.Equal(t, EffectSucceeded, state.states["finish-game:saga-1"].Effects["update-elo"].Status)
}

func TestSagaManagerUsesExplicitAssociationKey(t *testing.T) {
	inbox := &memoryInbox{claimed: map[string]bool{}}
	state := &memorySagaStore{states: map[string]*SagaState{}}
	outbox := &memoryOutbox{}
	manager := newSagaTestManager(t, inbox, state, outbox, func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
		saga.Status = SagaWaiting
		return nil, nil
	})

	event := sagaTestEvent()
	event.AssociationKey = "membership:42"
	event.CorrelationID = "correlation:ignored"
	require.NoError(t, manager.Handle(context.Background(), event))
	require.NotNil(t, state.states["finish-game:membership:42"])
	require.Nil(t, state.states["finish-game:correlation:ignored"])
}

func TestSagaManagerDurablyTracksCompensationLifecycle(t *testing.T) {
	state := &memorySagaStore{states: map[string]*SagaState{}}
	manager := newSagaTestManager(t, &memoryInbox{claimed: map[string]bool{}}, state, &memoryOutbox{}, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return nil, nil
	})

	started, err := manager.StartCompensation(context.Background(), "saga-1", "rollback-elo", errors.New("elo update failed"))
	require.NoError(t, err)
	require.Equal(t, SagaCompensating, started.Status)
	require.Equal(t, SagaCompensating, started.Compensation.Status)
	require.Equal(t, 1, started.Compensation.Attempts)
	require.Equal(t, "elo update failed", started.Compensation.LastError)

	require.NoError(t, manager.CompleteCompensation(context.Background(), "saga-1"))
	persisted := state.states["finish-game:saga-1"]
	require.Equal(t, SagaCompleted, persisted.Status)
	require.Equal(t, SagaCompleted, persisted.Compensation.Status)
	require.Equal(t, "rollback-elo", persisted.Compensation.Step)
}

func TestSagaManagerRecordsCompensationFailureDurably(t *testing.T) {
	state := &memorySagaStore{states: map[string]*SagaState{}}
	manager := newSagaTestManager(t, &memoryInbox{claimed: map[string]bool{}}, state, &memoryOutbox{}, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return nil, nil
	})

	_, err := manager.StartCompensation(context.Background(), "saga-1", "rollback-elo", nil)
	require.NoError(t, err)
	err = manager.FailCompensation(context.Background(), "saga-1", errors.New("rollback failed"))
	require.EqualError(t, err, "rollback failed")
	persisted := state.states["finish-game:saga-1"]
	require.Equal(t, SagaFailed, persisted.Status)
	require.Equal(t, SagaFailed, persisted.Compensation.Status)
	require.Equal(t, "rollback failed", persisted.Compensation.LastError)
}
