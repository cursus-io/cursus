package coordinator

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestTopicCleanupPreservesSharedSubscriptions(t *testing.T) {
	for _, offsets := range []bool{false, true} {
		t.Run(map[bool]string{false: "before first commit", true: "retained offsets"}[offsets], func(t *testing.T) {
			c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
			t.Cleanup(c.Stop)
			require.NoError(t, c.RegisterGroup("orders", "a-single", 1))
			require.NoError(t, c.RegisterGroupSubscription("z-shared", []string{"orders", "payments"}, "", map[string]int{"orders": 1, "payments": 1}))
			if offsets {
				require.NoError(t, c.CommitOffset("z-shared", "orders", 0, 17))
				require.NoError(t, c.CommitOffset("z-shared", "payments", 0, 29))
			}
			before := c.ExportState()
			writes := 0
			c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { writes++; return nil })
			require.Len(t, c.TopicGroupReferences("orders"), 2)
			for range 2 {
				deleted, err := c.DeleteInactiveGroupsForTopic("orders")
				require.ErrorContains(t, err, "outside topic")
				require.Empty(t, deleted)
				require.Equal(t, before, c.ExportState())
			}
			require.Zero(t, writes, "a later shared group must block the whole preflight")
		})
	}
}

func TestTopicCleanupPreservesPatternSubscriptions(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "concrete expansion", true: "legacy wildcard"}[legacy], func(t *testing.T) {
			c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
			t.Cleanup(c.Stop)
			if legacy {
				require.NoError(t, c.RegisterGroup("orders-*", "workers", 1))
			} else {
				require.NoError(t, c.RegisterGroupSubscription("workers", []string{"orders-eu"}, "orders-*", map[string]int{"orders-eu": 1}))
			}
			before := c.ExportState()
			require.Len(t, c.TopicGroupReferences("orders-eu"), 1)
			deleted, err := c.DeleteInactiveGroupsForTopic("orders-eu")
			require.ErrorContains(t, err, "pattern subscription")
			require.Empty(t, deleted)
			require.Equal(t, before, c.ExportState())
			require.Empty(t, c.TopicGroupReferences("payments"))
		})
	}
}

func TestTopicCleanupPreflightsReservationsBeforeAnyDeletion(t *testing.T) {
	c, reservation, epoch := newReservationCoordinator(t)
	require.NoError(t, c.RegisterGroup("orders", "a-single", 1))
	require.NoError(t, c.PrepareOffsetReservation("workers", epoch, reservation))
	require.NoError(t, c.RemoveConsumerForGeneration("workers", "worker", reservation.Generation))
	before := c.ExportState()
	writes := 0
	c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { writes++; return nil })
	deleted, err := c.DeleteInactiveGroupsForTopic("orders")
	require.ErrorContains(t, err, "pending transaction")
	require.Empty(t, deleted)
	require.Equal(t, before, c.ExportState())
	require.Zero(t, writes)
}

func TestTopicCleanupSharedProtectionSurvivesSnapshotRestore(t *testing.T) {
	source := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(source.Stop)
	require.NoError(t, source.RegisterGroupSubscription("workers", []string{"orders", "payments"}, "", map[string]int{"orders": 1, "payments": 1}))
	require.NoError(t, source.CommitOffset("workers", "orders", 0, 17))
	require.NoError(t, source.CommitOffset("workers", "payments", 0, 29))
	before := source.ExportState()
	restored := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(restored.Stop)
	require.NoError(t, restored.ImportState(before))
	_, err := restored.DeleteInactiveGroupsForTopic("orders")
	require.ErrorContains(t, err, "outside topic")
	require.Equal(t, before, restored.ExportState())
}

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
