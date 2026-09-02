package main

import (
	"context"
	"fmt"

	sdk "github.com/cursus-io/cursus/sdk"
)

type repository struct {
	seen     map[string]bool
	state    *sdk.SagaState
	commands []sdk.Command
}

func (r *repository) Transact(_ context.Context, apply func(sdk.SagaTransaction) error) error {
	next := &repository{seen: make(map[string]bool, len(r.seen)), state: cloneState(r.state), commands: append([]sdk.Command(nil), r.commands...)}
	for key, value := range r.seen {
		next.seen[key] = value
	}
	if err := apply(next); err != nil {
		return err
	}
	*r = *next
	return nil
}

func (r *repository) Claim(consumer, eventID string) (bool, error) {
	key := consumer + ":" + eventID
	if r.seen[key] {
		return false, nil
	}
	r.seen[key] = true
	return true, nil
}
func (*repository) Complete(string, string) error    { return nil }
func (*repository) Fail(string, string, error) error { return nil }
func (r *repository) Load(string, string) (*sdk.SagaState, error) {
	return cloneState(r.state), nil
}
func (r *repository) SaveCAS(state *sdk.SagaState, expectedVersion uint64) error {
	if r.state != nil && r.state.Version != expectedVersion {
		return fmt.Errorf("saga version conflict")
	}
	r.state = cloneState(state)
	return nil
}
func (r *repository) Enqueue(command sdk.Command) error {
	r.commands = append(r.commands, command)
	return nil
}

func cloneState(state *sdk.SagaState) *sdk.SagaState {
	if state == nil {
		return nil
	}
	copy := *state
	if state.Effects != nil {
		copy.Effects = make(map[string]sdk.EffectState, len(state.Effects))
		for id, effect := range state.Effects {
			copy.Effects[id] = effect
		}
	}
	if state.Compensation != nil {
		compensation := *state.Compensation
		copy.Compensation = &compensation
	}
	return &copy
}

func main() {
	manager, err := sdk.NewSagaManager(sdk.SagaDefinition{
		Type: "finish-game",
		Handlers: map[string]sdk.SagaHandler{
			"GameFinished": func(_ context.Context, state *sdk.SagaState, _ sdk.EventEnvelope) ([]sdk.Command, error) {
				state.Status = sdk.SagaWaiting
				state.Step = "update-elo"
				return []sdk.Command{{
					EffectID: "update-player-elo",
					Type:     "UpdatePlayerElo",
					Payload:  "{\"player\":\"p1\"}",
				}}, nil
			},
		},
	}, &repository{seen: map[string]bool{}})
	if err != nil {
		panic(err)
	}

	event, err := sdk.NewEventEnvelope(
		"game", "game-1", "GameFinished",
		map[string]string{"winner": "p1"},
	)
	if err != nil {
		panic(err)
	}
	event.AssociationKey = "membership:player-1"
	event.CorrelationID = "saga-1"

	if err := manager.Handle(context.Background(), event); err != nil {
		panic(err)
	}
	fmt.Printf("Saga accepted event association=%s effect=%s\n", event.AssociationKey, "update-player-elo")
}
