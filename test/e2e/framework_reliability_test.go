package e2e

import (
	"context"
	"testing"

	sdk "github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

type reliabilityAggregate struct {
	id      string
	version uint64
	status  string
}

func (a *reliabilityAggregate) ID() string      { return a.id }
func (a *reliabilityAggregate) Type() string    { return "game" }
func (a *reliabilityAggregate) Version() uint64 { return a.version }
func (a *reliabilityAggregate) Apply(event sdk.EventEnvelope) error {
	a.version = event.AggregateVersion
	a.status = string(event.Payload)
	return nil
}
func (a *reliabilityAggregate) RestoreSnapshot(payload string, version uint64) error {
	a.version = version
	a.status = payload
	return nil
}

func TestFrameworkAggregateRepositoryRoundTripAndSnapshotThroughDockerBroker(t *testing.T) {
	ctx := GivenStandalone(t).WithTopic("framework-reliability-e2e").WithPartitions(1)
	defer ctx.Cleanup()

	store := sdk.NewEventStore(defaultBrokerAddrs[0], ctx.topic, "framework-reliability-producer")
	defer func() { _ = store.Close() }()
	require.NoError(t, store.CreateTopic(ctx.partitions))

	repository, err := sdk.NewAggregateRepository(store, func(id string) sdk.Aggregate {
		return &reliabilityAggregate{id: id}
	})
	require.NoError(t, err)

	aggregate, err := repository.Load("game-reliability-e2e")
	require.NoError(t, err)
	first, err := sdk.NewEventEnvelope("game", aggregate.ID(), "GameCreated", map[string]string{"status": "created"})
	require.NoError(t, err)
	require.NoError(t, repository.Save(aggregate, []sdk.EventEnvelope{first}))
	second, err := sdk.NewEventEnvelope("game", aggregate.ID(), "GameFinished", map[string]string{"status": "finished"})
	require.NoError(t, err)
	require.NoError(t, repository.Save(aggregate, []sdk.EventEnvelope{second}))
	require.Equal(t, uint64(2), aggregate.Version())

	require.NoError(t, store.SaveSnapshot(aggregate.ID(), aggregate.Version(), "{\"status\":\"finished\"}"))
	loaded, err := repository.Load(aggregate.ID())
	require.NoError(t, err)
	require.Equal(t, uint64(2), loaded.Version())
	require.Equal(t, "{\"status\":\"finished\"}", loaded.(*reliabilityAggregate).status)
}

func TestSagaFrameworkRedeliveryDoesNotDuplicateCommandThroughDockerBroker(t *testing.T) {
	ctx := GivenStandalone(t).WithTopic("saga-reliability-e2e").WithPartitions(1)
	defer ctx.Cleanup()

	store := sdk.NewEventStore(defaultBrokerAddrs[0], ctx.topic, "saga-reliability-producer")
	defer func() { _ = store.Close() }()
	require.NoError(t, store.CreateTopic(ctx.partitions))

	event, err := sdk.NewEventEnvelope("game", "game-redelivery-e2e", "GameFinished", map[string]string{"winner": "p1"})
	require.NoError(t, err)
	_, err = store.AppendEnvelope(event.AggregateID, 0, event)
	require.NoError(t, err)
	events, err := store.ReadEnvelopes(event.AggregateID)
	require.NoError(t, err)
	require.Len(t, events, 1)

	outbox := &e2eOutbox{}
	manager, err := sdk.NewSagaManager(sdk.SagaDefinition{
		Type: "finish-game",
		Handlers: map[string]sdk.SagaHandler{
			"GameFinished": func(_ context.Context, state *sdk.SagaState, _ sdk.EventEnvelope) ([]sdk.Command, error) {
				state.Status = sdk.SagaWaiting
				return []sdk.Command{{Type: "UpdatePlayerElo"}}, nil
			},
		},
	}, &e2eInbox{seen: map[string]bool{}}, &e2eSagaStore{}, outbox)
	require.NoError(t, err)

	require.NoError(t, manager.Handle(context.Background(), events[0]))
	require.NoError(t, manager.Handle(context.Background(), events[0]))
	require.Len(t, outbox.commands, 1)
}
