package coordinator

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCoordinatorStateRoundTripPreservesGroupMetadata(t *testing.T) {
	source := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	require.NoError(t, source.RegisterGroup("orders", "workers", 4))
	_, err := source.AddConsumer("workers", "worker-1")
	require.NoError(t, err)

	exported := source.ExportState()
	restored := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	restored.ImportState(exported)

	status, err := restored.GetGroupStatus("workers")
	require.NoError(t, err)
	require.Equal(t, 4, status.PartitionCount)
	require.Equal(t, []int{0, 1, 2, 3}, restored.GetMemberAssignments("workers", "worker-1"))
	require.True(t, status.LastRebalance.Equal(exported["workers"].LastRebalance))
}

func TestCoordinatorImportStateReplacesExistingGroups(t *testing.T) {
	source := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	require.NoError(t, source.RegisterGroup("orders", "current", 2))

	restored := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	require.NoError(t, restored.RegisterGroup("legacy", "stale", 1))
	restored.ImportState(source.ExportState())

	require.ElementsMatch(t, []string{"current"}, restored.ListGroups())
	_, err := restored.GetGroupStatus("stale")
	require.Error(t, err)
}

func TestCoordinatorImportStateInfersPartitionsFromLegacySnapshot(t *testing.T) {
	var legacy map[string]*GroupStateSnapshot
	require.NoError(t, json.Unmarshal([]byte(`{
		"workers": {
			"topic": "orders",
			"generation": 3,
			"members": {"worker-1": [0, 1], "worker-2": [2, 3]},
			"offsets": {}
		}
	}`), &legacy))

	restored := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	restored.ImportState(legacy)
	restored.Rebalance("workers")

	status, err := restored.GetGroupStatus("workers")
	require.NoError(t, err)
	require.Equal(t, 4, status.PartitionCount)
	require.ElementsMatch(t, []int{0, 1}, restored.GetMemberAssignments("workers", "worker-1"))
	require.ElementsMatch(t, []int{2, 3}, restored.GetMemberAssignments("workers", "worker-2"))
}
