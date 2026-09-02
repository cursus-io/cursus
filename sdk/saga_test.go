package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type memorySagaRepository struct {
	mu       sync.Mutex
	claimed  map[string]bool
	complete []string
	fails    []string
	states   map[string]*SagaState
	commands []Command
	failures map[string]error
}

type memorySagaTransaction struct {
	repository *memorySagaRepository
	claimed    map[string]bool
	complete   []string
	fails      []string
	states     map[string]*SagaState
	commands   []Command
}

func newMemorySagaRepository() *memorySagaRepository {
	return &memorySagaRepository{
		claimed:  make(map[string]bool),
		states:   make(map[string]*SagaState),
		failures: make(map[string]error),
	}
}

func (r *memorySagaRepository) Transact(ctx context.Context, apply func(SagaTransaction) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.failures["begin"]; err != nil {
		return err
	}
	tx := &memorySagaTransaction{
		repository: r,
		claimed:    cloneBoolMap(r.claimed),
		complete:   append([]string(nil), r.complete...),
		fails:      append([]string(nil), r.fails...),
		states:     cloneSagaStates(r.states),
		commands:   append([]Command(nil), r.commands...),
	}
	if err := apply(tx); err != nil {
		return err
	}
	if err := r.failures["commit"]; err != nil {
		return err
	}
	r.claimed = tx.claimed
	r.complete = tx.complete
	r.fails = tx.fails
	r.states = tx.states
	r.commands = tx.commands
	return nil
}

func (tx *memorySagaTransaction) Claim(consumer, eventID string) (bool, error) {
	if err := tx.repository.failures["claim"]; err != nil {
		return false, err
	}
	key := consumer + ":" + eventID
	if tx.claimed[key] {
		return false, nil
	}
	tx.claimed[key] = true
	return true, nil
}

func (tx *memorySagaTransaction) Load(sagaType, associationKey string) (*SagaState, error) {
	if err := tx.repository.failures["load"]; err != nil {
		return nil, err
	}
	return cloneSagaState(tx.states[sagaType+":"+associationKey]), nil
}

func (tx *memorySagaTransaction) SaveCAS(state *SagaState, expectedVersion uint64) error {
	if err := tx.repository.failures["save"]; err != nil {
		return err
	}
	key := state.Type + ":" + state.ID
	currentVersion := uint64(0)
	if current := tx.states[key]; current != nil {
		currentVersion = current.Version
	}
	if currentVersion != expectedVersion {
		return fmt.Errorf("saga version conflict: expected %d, have %d", expectedVersion, currentVersion)
	}
	if state.Version != expectedVersion+1 {
		return fmt.Errorf("invalid next saga version %d", state.Version)
	}
	tx.states[key] = cloneSagaState(state)
	return nil
}

func (tx *memorySagaTransaction) Enqueue(command Command) error {
	if err := tx.repository.failures["enqueue"]; err != nil {
		return err
	}
	for _, existing := range tx.commands {
		if existing.ID == command.ID {
			if existing == command {
				return nil
			}
			return fmt.Errorf("outbox command identity conflict: %s", command.ID)
		}
	}
	tx.commands = append(tx.commands, command)
	return nil
}

func (tx *memorySagaTransaction) Complete(_, eventID string) error {
	if err := tx.repository.failures["complete"]; err != nil {
		return err
	}
	tx.complete = append(tx.complete, eventID)
	return nil
}

func (tx *memorySagaTransaction) Fail(_, eventID string, _ error) error {
	if err := tx.repository.failures["fail"]; err != nil {
		return err
	}
	tx.fails = append(tx.fails, eventID)
	return nil
}

func cloneBoolMap(source map[string]bool) map[string]bool {
	clone := make(map[string]bool, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneSagaStates(source map[string]*SagaState) map[string]*SagaState {
	clone := make(map[string]*SagaState, len(source))
	for key, state := range source {
		clone[key] = cloneSagaState(state)
	}
	return clone
}

func cloneSagaState(state *SagaState) *SagaState {
	if state == nil {
		return nil
	}
	clone := *state
	clone.Effects = make(map[string]EffectState, len(state.Effects))
	for key, effect := range state.Effects {
		clone.Effects[key] = effect
	}
	if state.Compensation != nil {
		compensation := *state.Compensation
		clone.Compensation = &compensation
	}
	return &clone
}

func sagaTestEvent() EventEnvelope {
	return EventEnvelope{EventID: "event-1", EventType: "GameFinished", AggregateType: "game", AggregateID: "game-1", AggregateVersion: 1, SchemaVersion: 1, OccurredAt: time.Now().UTC(), CorrelationID: "saga-1", Payload: []byte(`{"winner":"p1"}`)}
}

func newSagaTestManager(t *testing.T, repository SagaRepository, handler SagaHandler) *SagaManager {
	t.Helper()
	manager, err := NewSagaManager(SagaDefinition{
		Type:     "finish-game",
		Handlers: map[string]SagaHandler{"GameFinished": handler},
	}, repository)
	require.NoError(t, err)
	return manager
}

func TestSagaManagerCommitsClaimStateAndOutboxAtomically(t *testing.T) {
	repository := newMemorySagaRepository()
	manager := newSagaTestManager(t, repository, func(_ context.Context, saga *SagaState, _ EventEnvelope) ([]Command, error) {
		saga.Step, saga.Status = "update-elo", SagaWaiting
		return []Command{{Type: "UpdatePlayerElo", Payload: `{"player":"p1"}`}}, nil
	})

	event := sagaTestEvent()
	require.NoError(t, manager.Handle(context.Background(), event))
	require.NoError(t, manager.Handle(context.Background(), event))
	require.Len(t, repository.commands, 1)
	require.Equal(t, "finish-game:saga-1:event-1:0", repository.commands[0].ID)
	require.Equal(t, "saga-1", repository.commands[0].SagaID)
	require.Equal(t, event.EventID, repository.commands[0].CausationID)
	require.Equal(t, []string{"event-1"}, repository.complete)
	persisted := repository.states["finish-game:saga-1"]
	require.Equal(t, SagaWaiting, persisted.Status)
	require.Equal(t, uint64(1), persisted.Version)
	require.Equal(t, EffectEnqueued, persisted.Effects["event-1:0"].Status)
}

func TestSagaManagerCommitsHandlerFailureAndRetryCount(t *testing.T) {
	repository := newMemorySagaRepository()
	manager := newSagaTestManager(t, repository, func(context.Context, *SagaState, EventEnvelope) ([]Command, error) {
		return nil, errors.New("elo service unavailable")
	})

	err := manager.Handle(context.Background(), sagaTestEvent())
	require.EqualError(t, err, "elo service unavailable")
	require.Equal(t, 1, repository.states["finish-game:saga-1"].RetryCount)
	require.Equal(t, []string{"event-1"}, repository.fails)
	require.True(t, repository.claimed["finish-game:event-1"])
}
