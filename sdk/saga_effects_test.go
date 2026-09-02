package sdk

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSagaManagerAcknowledgesEnqueuedEffectWithCommandFence(t *testing.T) {
	repository := newMemorySagaRepository()
	manager := newSagaTestManager(t, repository, func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
		saga.Status = SagaWaiting
		return []Command{{EffectID: "update-elo", Type: "UpdatePlayerElo"}}, nil
	})

	require.NoError(t, manager.Handle(context.Background(), sagaTestEvent()))
	effect := repository.states["finish-game:saga-1"].Effects["update-elo"]
	require.Equal(t, EffectEnqueued, effect.Status)
	require.Len(t, repository.commands, 1)
	require.ErrorContains(t, manager.AcknowledgeEffect(context.Background(), "saga-1", "update-elo", "stale-command"), "command fence mismatch")
	require.NoError(t, manager.AcknowledgeEffect(context.Background(), "saga-1", "update-elo", effect.CommandID))
	require.NoError(t, manager.AcknowledgeEffect(context.Background(), "saga-1", "update-elo", effect.CommandID))
	require.Equal(t, EffectSucceeded, repository.states["finish-game:saga-1"].Effects["update-elo"].Status)
	require.Equal(t, uint64(2), repository.states["finish-game:saga-1"].Version)

	second := sagaTestEvent()
	second.EventID = "event-2"
	require.NoError(t, manager.Handle(context.Background(), second))
	require.Len(t, repository.commands, 1)
}

func TestSagaManagerRecordsEffectFailureWithCommandFence(t *testing.T) {
	repository := newMemorySagaRepository()
	manager := newSagaTestManager(t, repository, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return []Command{{EffectID: "update-elo", Type: "UpdatePlayerElo"}}, nil
	})
	require.NoError(t, manager.Handle(context.Background(), sagaTestEvent()))
	effect := repository.states["finish-game:saga-1"].Effects["update-elo"]

	err := manager.FailEffect(context.Background(), "saga-1", "update-elo", effect.CommandID, errors.New("delivery failed"))
	require.NoError(t, err)
	persisted := repository.states["finish-game:saga-1"].Effects["update-elo"]
	require.Equal(t, EffectFailed, persisted.Status)
	require.Equal(t, "delivery failed", persisted.LastError)
	require.ErrorContains(t, manager.AcknowledgeEffect(context.Background(), "saga-1", "update-elo", effect.CommandID), "not awaiting acknowledgement")
}

func TestSagaManagerUsesExplicitAssociationKey(t *testing.T) {
	repository := newMemorySagaRepository()
	manager := newSagaTestManager(t, repository, func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
		saga.Status = SagaWaiting
		return nil, nil
	})

	event := sagaTestEvent()
	event.AssociationKey = "membership:42"
	event.CorrelationID = "correlation:ignored"
	require.NoError(t, manager.Handle(context.Background(), event))
	require.NotNil(t, repository.states["finish-game:membership:42"])
	require.Nil(t, repository.states["finish-game:correlation:ignored"])
}

func TestSagaManagerDurablyTracksCompensationLifecycle(t *testing.T) {
	repository := newMemorySagaRepository()
	manager := newSagaTestManager(t, repository, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) { return nil, nil })

	started, err := manager.StartCompensation(context.Background(), "saga-1", "rollback-elo", errors.New("elo update failed"))
	require.NoError(t, err)
	require.Equal(t, SagaCompensating, started.Status)
	require.Equal(t, 1, started.Compensation.Attempts)
	require.Equal(t, "elo update failed", started.Compensation.LastError)
	require.Equal(t, uint64(1), started.Version)

	require.NoError(t, manager.CompleteCompensation(context.Background(), "saga-1"))
	persisted := repository.states["finish-game:saga-1"]
	require.Equal(t, SagaCompleted, persisted.Status)
	require.Equal(t, SagaCompleted, persisted.Compensation.Status)
	require.Equal(t, uint64(2), persisted.Version)
}

func TestSagaManagerRecordsCompensationFailureDurably(t *testing.T) {
	repository := newMemorySagaRepository()
	manager := newSagaTestManager(t, repository, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) { return nil, nil })

	_, err := manager.StartCompensation(context.Background(), "saga-1", "rollback-elo", nil)
	require.NoError(t, err)
	err = manager.FailCompensation(context.Background(), "saga-1", errors.New("rollback failed"))
	require.EqualError(t, err, "rollback failed")
	persisted := repository.states["finish-game:saga-1"]
	require.Equal(t, SagaFailed, persisted.Status)
	require.Equal(t, "rollback failed", persisted.Compensation.LastError)
}
