package e2e

import (
	"context"
	"testing"

	sdk "github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

type e2eInbox struct{ seen map[string]bool }

func (i *e2eInbox) Claim(_ context.Context, consumer, eventID string) (bool, error) {
	key := consumer + ":" + eventID
	if i.seen[key] {
		return false, nil
	}
	i.seen[key] = true
	return true, nil
}
func (*e2eInbox) Complete(context.Context, string, string) error    { return nil }
func (*e2eInbox) Fail(context.Context, string, string, error) error { return nil }

type e2eSagaStore struct{ state *sdk.SagaState }

func (s *e2eSagaStore) Load(context.Context, string, string) (*sdk.SagaState, error) {
	return s.state, nil
}
func (s *e2eSagaStore) Save(_ context.Context, state *sdk.SagaState) error {
	copy := *state
	s.state = &copy
	return nil
}

type e2eOutbox struct{ commands []sdk.Command }

func (o *e2eOutbox) Enqueue(_ context.Context, command sdk.Command) error {
	o.commands = append(o.commands, command)
	return nil
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

	outbox := &e2eOutbox{}
	manager, err := sdk.NewSagaManager(sdk.SagaDefinition{Type: "finish-game", Handlers: map[string]sdk.SagaHandler{
		"GameFinished": func(_ context.Context, state *sdk.SagaState, _ sdk.EventEnvelope) ([]sdk.Command, error) {
			state.Status = sdk.SagaWaiting
			state.Step = "update-elo"
			return []sdk.Command{{Type: "UpdatePlayerElo", Payload: `{"player":"p1"}`}}, nil
		},
	}}, &e2eInbox{seen: map[string]bool{}}, &e2eSagaStore{}, outbox)
	require.NoError(t, err)
	require.NoError(t, manager.Handle(context.Background(), events[0]))
	require.Len(t, outbox.commands, 1)
	require.Equal(t, "UpdatePlayerElo", outbox.commands[0].Type)
	require.Equal(t, "saga-e2e-1", outbox.commands[0].SagaID)
}
