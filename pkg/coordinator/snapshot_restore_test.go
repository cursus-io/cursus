package coordinator

import (
	"context"
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
	require.NoError(t, restored.ImportState(exported))

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
	require.NoError(t, restored.ImportState(source.ExportState()))

	require.ElementsMatch(t, []string{"current"}, restored.ListGroups())
	_, err := restored.GetGroupStatus("stale")
	require.Error(t, err)
}

func TestCoordinatorImportStateRejectsIncompleteSnapshot(t *testing.T) {
	restored := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	err := restored.ImportState(map[string]*GroupStateSnapshot{
		"workers": {
			TopicName:  "orders",
			Generation: 3,
			Members:    map[string][]int{"worker-1": {0, 1}, "worker-2": {2, 3}},
			Offsets:    map[string]map[int]uint64{},
		},
	})
	require.ErrorContains(t, err, "missing registration epoch")
	require.Empty(t, restored.ListGroups())
}

func TestCoordinatorSnapshotPreservesDeletedGroupEpoch(t *testing.T) {
	source := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	t.Cleanup(source.Stop)
	require.NoError(t, source.RegisterGroup("orders", "workers", 1))
	require.NoError(t, source.DeleteGroup("workers"))

	exported := source.ExportState()
	require.True(t, exported["workers"].Deleted)
	require.Equal(t, uint64(2), exported["workers"].RegistrationEpoch)

	restored := NewCoordinator(context.Background(), &config.Config{}, &DummyPublisher{})
	t.Cleanup(restored.Stop)
	require.NoError(t, restored.ImportState(exported))
	require.Empty(t, restored.ListGroups())
	require.NoError(t, restored.RegisterGroup("orders", "workers", 1))

	reregistered := restored.ExportState()["workers"]
	require.False(t, reregistered.Deleted)
	require.Equal(t, uint64(3), reregistered.RegistrationEpoch)
}
