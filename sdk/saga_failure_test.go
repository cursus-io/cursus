package sdk

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSagaManagerRejectsIncompleteDependencies(t *testing.T) {
	_, err := NewSagaManager(SagaDefinition{}, newMemorySagaRepository())
	require.EqualError(t, err, "saga definition requires a type and handlers")
	_, err = NewSagaManager(SagaDefinition{Type: "finish-game", Handlers: map[string]SagaHandler{
		"GameFinished": func(context.Context, *SagaState, EventEnvelope) ([]Command, error) { return nil, nil },
	}}, nil)
	require.EqualError(t, err, "saga repository is required")
}

func TestSagaManagerRollsBackEveryInfrastructureFailure(t *testing.T) {
	for _, test := range []struct {
		point   string
		message string
	}{
		{point: "claim", message: "claim saga inbox: injected claim failure"},
		{point: "load", message: "load saga state: injected load failure"},
		{point: "enqueue", message: "enqueue saga command: injected enqueue failure"},
		{point: "save", message: "save saga state: injected save failure"},
		{point: "complete", message: "complete saga inbox: injected complete failure"},
		{point: "commit", message: "injected commit failure"},
	} {
		t.Run(test.point, func(t *testing.T) {
			repository := newMemorySagaRepository()
			repository.failures[test.point] = errors.New("injected " + test.point + " failure")
			manager := newSagaTestManager(t, repository, func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
				saga.Status = SagaWaiting
				return []Command{{Type: "UpdatePlayerElo"}}, nil
			})

			err := manager.Handle(context.Background(), sagaTestEvent())
			require.EqualError(t, err, test.message)
			require.Empty(t, repository.claimed)
			require.Empty(t, repository.states)
			require.Empty(t, repository.commands)
			require.Empty(t, repository.complete)
			require.Empty(t, repository.fails)
		})
	}
}

func TestSagaManagerRollsBackWhenFailureRecordCannotCommit(t *testing.T) {
	repository := newMemorySagaRepository()
	repository.failures["fail"] = errors.New("inbox failure write failed")
	manager := newSagaTestManager(t, repository, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return nil, errors.New("handler failed")
	})

	err := manager.Handle(context.Background(), sagaTestEvent())
	require.EqualError(t, err, "record saga inbox failure: inbox failure write failed")
	require.Empty(t, repository.claimed)
	require.Empty(t, repository.states)
	require.Empty(t, repository.fails)
}

func TestSagaManagerCompletesUnhandledEventWithoutCreatingState(t *testing.T) {
	repository := newMemorySagaRepository()
	manager := newSagaTestManager(t, repository, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) { return nil, nil })
	event := sagaTestEvent()
	event.EventType = "GameStarted"

	require.NoError(t, manager.Handle(context.Background(), event))
	require.Equal(t, []string{"event-1"}, repository.complete)
	require.Empty(t, repository.states)
	require.Empty(t, repository.commands)
}

func TestSagaManagerUsesAggregateIDWhenCorrelationIDIsEmpty(t *testing.T) {
	repository := newMemorySagaRepository()
	manager := newSagaTestManager(t, repository, func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
		saga.Status = SagaWaiting
		return []Command{{Type: "UpdatePlayerElo"}}, nil
	})
	event := sagaTestEvent()
	event.CorrelationID = ""

	require.NoError(t, manager.Handle(context.Background(), event))
	require.Equal(t, SagaWaiting, repository.states["finish-game:game-1"].Status)
	require.Equal(t, "game-1", repository.commands[0].SagaID)
}

func TestSagaTransactionRejectsStaleStateVersion(t *testing.T) {
	repository := newMemorySagaRepository()
	repository.states["finish-game:saga-1"] = &SagaState{ID: "saga-1", Type: "finish-game", Version: 2}
	err := repository.Transact(context.Background(), func(tx SagaTransaction) error {
		return tx.SaveCAS(&SagaState{ID: "saga-1", Type: "finish-game", Version: 2}, 1)
	})
	require.EqualError(t, err, "saga version conflict: expected 1, have 2")
	require.Equal(t, uint64(2), repository.states["finish-game:saga-1"].Version)
}
