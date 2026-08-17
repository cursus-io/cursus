package e2e

import (
	"testing"

	sdk "github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

func TestFrameworkReplayUpcastsCursusEventsThroughDockerBroker(t *testing.T) {
	ctx := GivenStandalone(t).WithTopic("framework-replay-e2e").WithPartitions(1)
	defer ctx.Cleanup()

	store := sdk.NewEventStore(defaultBrokerAddrs[0], ctx.topic, "framework-replay-e2e-producer")
	defer func() { _ = store.Close() }()
	require.NoError(t, store.CreateTopic(ctx.partitions))
	event, err := sdk.NewEventEnvelope("game", "game-replay-e2e", "GameFinished", map[string]string{"winner": "p1"})
	require.NoError(t, err)
	event.SchemaVersion = 1
	_, err = store.AppendEnvelope(event.AggregateID, 0, event)
	require.NoError(t, err)

	registry := sdk.NewUpcasterRegistry()
	require.NoError(t, registry.Register("GameFinished", 1, func(event sdk.EventEnvelope) (sdk.EventEnvelope, error) {
		event.SchemaVersion = 2
		return event, nil
	}))
	var replayed sdk.EventEnvelope
	require.NoError(t, sdk.Replay(store, event.AggregateID, 1, registry, func(got sdk.EventEnvelope) error { replayed = got; return nil }))
	require.Equal(t, uint32(2), replayed.SchemaVersion)
	require.Equal(t, event.EventID, replayed.EventID)
}
