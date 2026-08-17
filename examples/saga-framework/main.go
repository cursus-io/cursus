package main

import (
	"context"
	"fmt"

	sdk "github.com/cursus-io/cursus/sdk"
)

type inbox struct{ seen map[string]bool }

func (i *inbox) Claim(_ context.Context, consumer, eventID string) (bool, error) {
	key := consumer + ":" + eventID
	if i.seen[key] {
		return false, nil
	}
	i.seen[key] = true
	return true, nil
}
func (*inbox) Complete(context.Context, string, string) error    { return nil }
func (*inbox) Fail(context.Context, string, string, error) error { return nil }

type sagaStore struct{ state *sdk.SagaState }

func (s *sagaStore) Load(context.Context, string, string) (*sdk.SagaState, error) {
	return s.state, nil
}
func (s *sagaStore) Save(_ context.Context, state *sdk.SagaState) error {
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
	s.state = &copy
	return nil
}

type outbox struct{ commands []sdk.Command }

func (o *outbox) Enqueue(_ context.Context, command sdk.Command) error {
	o.commands = append(o.commands, command)
	return nil
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
	}, &inbox{seen: map[string]bool{}}, &sagaStore{}, &outbox{})
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
