package e2e

import (
	"context"
	"fmt"
	"testing"

	sdk "github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

type e2eSagaRepository struct {
	seen     map[string]bool
	state    *sdk.SagaState
	commands []sdk.Command
}

func (r *e2eSagaRepository) Transact(_ context.Context, apply func(sdk.SagaTransaction) error) error {
	next := &e2eSagaRepository{seen: make(map[string]bool, len(r.seen)), state: cloneE2ESagaState(r.state), commands: append([]sdk.Command(nil), r.commands...)}
	for key, value := range r.seen {
		next.seen[key] = value
	}
	if err := apply(next); err != nil {
		return err
	}
	*r = *next
	return nil
}

func (r *e2eSagaRepository) Claim(consumer, eventID string) (bool, error) {
	key := consumer + ":" + eventID
	if r.seen[key] {
		return false, nil
	}
	r.seen[key] = true
	return true, nil
}
func (*e2eSagaRepository) Complete(string, string) error    { return nil }
func (*e2eSagaRepository) Fail(string, string, error) error { return nil }
func (r *e2eSagaRepository) Load(string, string) (*sdk.SagaState, error) {
	return cloneE2ESagaState(r.state), nil
}
func (r *e2eSagaRepository) SaveCAS(state *sdk.SagaState, expectedVersion uint64) error {
	if r.state != nil && r.state.Version != expectedVersion {
		return fmt.Errorf("saga version conflict")
	}
	r.state = cloneE2ESagaState(state)
	return nil
}
func (r *e2eSagaRepository) Enqueue(command sdk.Command) error {
	r.commands = append(r.commands, command)
	return nil
}

func cloneE2ESagaState(state *sdk.SagaState) *sdk.SagaState {
	if state == nil {
		return nil
	}
	copy := *state
	copy.Effects = make(map[string]sdk.EffectState, len(state.Effects))
	for key, effect := range state.Effects {
		copy.Effects[key] = effect
	}
	return &copy
}

func TestSagaFrameworkProcessesCursusEventThroughDockerBroker(t *testing.T) {
	ctx := GivenStandalone(t).WithTopic("saga-framework-e2e").WithPartitions(1)
	defer ctx.Cleanup()

	store := sdk.NewEventStore(defaultBrokerAddrs[0], ctx.topic, "saga-framework-e2e-producer")
	defer func() { _ = store.Close() }()
	require.NoError(t, store.CreateTopic(ctx.partitions))

	event, err := sdk.NewEventEnvelope("game", "game-saga-e2e", "GameFinished", map[string]string{"winner": "p1"})
	require.NoError(t, err)
	event.CorrelationID = "saga-e2e-1"
	_, err = store.AppendEnvelope(event.AggregateID, 0, event)
	require.NoError(t, err)
	events, err := store.ReadEnvelopes(event.AggregateID)
	require.NoError(t, err)
	require.Len(t, events, 1)

	repository := &e2eSagaRepository{seen: map[string]bool{}}
	manager, err := sdk.NewSagaManager(sdk.SagaDefinition{Type: "finish-game", Handlers: map[string]sdk.SagaHandler{
		"GameFinished": func(_ context.Context, state *sdk.SagaState, _ sdk.EventEnvelope) ([]sdk.Command, error) {
			state.Status = sdk.SagaWaiting
			state.Step = "update-elo"
			return []sdk.Command{{Type: "UpdatePlayerElo", Payload: `{"player":"p1"}`}}, nil
		},
	}}, repository)
	require.NoError(t, err)
	require.NoError(t, manager.Handle(context.Background(), events[0]))
	require.Len(t, repository.commands, 1)
	require.Equal(t, "UpdatePlayerElo", repository.commands[0].Type)
	require.Equal(t, "saga-e2e-1", repository.commands[0].SagaID)
}
