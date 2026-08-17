package e2e

import (
	"encoding/json"
	"testing"

	sdk "github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

func TestFrameworkEventEnvelopeRoundTripThroughDockerBroker(t *testing.T) {
	ctx := GivenStandalone(t).WithTopic("framework-event-envelope-e2e").WithPartitions(1)
	defer ctx.Cleanup()

	store := sdk.NewEventStore(defaultBrokerAddrs[0], ctx.topic, "framework-e2e-producer")
	defer func() { _ = store.Close() }()
	require.NoError(t, store.CreateTopic(ctx.partitions))

	event, err := sdk.NewEventEnvelope("game", "game-e2e-1", "GameFinished", map[string]any{"winner": "p1"})
	require.NoError(t, err)
	event.CorrelationID = "saga-e2e-1"
	result, err := store.AppendEnvelope("game-e2e-1", 0, event)
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Version)

	events, err := store.ReadEnvelopes("game-e2e-1")
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, event.EventID, events[0].EventID)
	require.Equal(t, "saga-e2e-1", events[0].CorrelationID)
	require.Equal(t, uint64(1), events[0].AggregateVersion)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(events[0].Payload, &payload))
	require.Equal(t, "p1", payload["winner"])
}
