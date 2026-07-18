package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/sdk"
	"github.com/stretchr/testify/require"
)

func TestEventStoreAppendReadSnapshotRoundTrip(t *testing.T) {
	ctx := GivenStandalone(t)
	ctx.WithTopic(fmt.Sprintf("eventstore-e2e-%d", time.Now().UnixNano()))
	defer ctx.Cleanup()

	store := sdk.NewEventStore(ctx.GetBrokerAddrs()[0], ctx.GetTopic(), "eventstore-e2e")
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	require.NoError(t, store.CreateTopic(1))

	first, err := store.Append("order-1", 0, &sdk.Event{
		Type:    "OrderCreated",
		Payload: `{"id":"order-1","total":10}`,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), first.Version)

	second, err := store.Append("order-1", 1, &sdk.Event{
		Type:    "ItemAdded",
		Payload: `{"sku":"widget","quantity":1}`,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), second.Version)

	_, err = store.Append("order-1", 1, &sdk.Event{Type: "Duplicate", Payload: `{}`})
	require.Error(t, err)
	require.Contains(t, err.Error(), "version_conflict")

	require.NoError(t, store.SaveSnapshot("order-1", 2, `{"items":1,"total":10}`))

	third, err := store.Append("order-1", 2, &sdk.Event{
		Type:    "OrderConfirmed",
		Payload: `{"confirmed":true}`,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), third.Version)

	stream, err := store.ReadStream("order-1")
	require.NoError(t, err)
	require.NotNil(t, stream.Snapshot)
	require.Equal(t, uint64(2), stream.Snapshot.Version)
	require.Len(t, stream.Events, 1)
	require.Equal(t, uint64(3), stream.Events[0].Version)
	require.Equal(t, "OrderConfirmed", stream.Events[0].Type)
	require.True(t, strings.Contains(stream.Events[0].Payload, "confirmed"))

	version, err := store.StreamVersion("order-1")
	require.NoError(t, err)
	require.Equal(t, uint64(3), version)
}
