package coordinator

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestDeleteInactiveGroupsForTopicFailsClosedThenWritesTombstone(t *testing.T) {
	coordinator := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	require.NoError(t, coordinator.RegisterGroup("orders", "workers", 1))
	_, err := coordinator.AddConsumer("workers", "member-1")
	require.NoError(t, err)

	_, err = coordinator.DeleteInactiveGroupsForTopic("orders")
	require.ErrorContains(t, err, "active consumer group")
	require.NotNil(t, coordinator.GetGroup("workers"))

	require.NoError(t, coordinator.RemoveConsumer("workers", "member-1"))
	deleted, err := coordinator.DeleteInactiveGroupsForTopic("orders")
	require.NoError(t, err)
	require.Equal(t, []string{"workers"}, deleted)
	require.Nil(t, coordinator.GetGroup("workers"))
	require.True(t, coordinator.ExportState()["workers"].Deleted)
}
