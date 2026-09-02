package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObserveConsumerGroupsStandaloneLifecycle(t *testing.T) {
	coordinator := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(coordinator.Stop)

	require.NoError(t, coordinator.RegisterGroup("events", "workers", 2))
	empty := requireSingleObservation(t, coordinator)
	assert.True(t, empty.CoordinatorUp)
	assert.Equal(t, ConsumerGroupStateEmpty, empty.State)
	assert.Zero(t, empty.MemberCount)
	assert.False(t, empty.LastActivity.IsZero())
	assert.True(t, empty.LastRebalance.IsZero())

	coordinator.mu.Lock()
	coordinator.groups["workers"].LastActivity = time.Unix(1, 0)
	coordinator.mu.Unlock()
	_, err := coordinator.AddConsumer("workers", "member-sensitive-id")
	require.NoError(t, err)
	joined := requireSingleObservation(t, coordinator)
	assert.Equal(t, ConsumerGroupStateStable, joined.State)
	assert.Equal(t, 1, joined.MemberCount)
	assert.True(t, joined.LastActivity.After(time.Unix(1, 0)))
	assert.False(t, joined.LastRebalance.IsZero())

	coordinator.mu.Lock()
	coordinator.groups["workers"].LastActivity = time.Unix(2, 0)
	coordinator.mu.Unlock()
	require.NoError(t, coordinator.RecordHeartbeatForGeneration("workers", "member-sensitive-id", joinedGeneration(coordinator, "workers")))
	heartbeat := requireSingleObservation(t, coordinator)
	assert.True(t, heartbeat.LastActivity.After(time.Unix(2, 0)))

	coordinator.mu.Lock()
	coordinator.groups["workers"].LastActivity = time.Unix(3, 0)
	coordinator.groups["workers"].LastRebalance = time.Unix(3, 0)
	coordinator.mu.Unlock()
	require.NoError(t, coordinator.RemoveConsumerForGeneration("workers", "member-sensitive-id", joinedGeneration(coordinator, "workers")))
	left := requireSingleObservation(t, coordinator)
	assert.Equal(t, ConsumerGroupStateEmpty, left.State)
	assert.Zero(t, left.MemberCount)
	assert.True(t, left.LastActivity.After(time.Unix(3, 0)))
	assert.True(t, left.LastRebalance.After(time.Unix(3, 0)))
}

func TestObserveConsumerGroupsHeartbeatTimeoutReturnsToZero(t *testing.T) {
	coordinator := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.RegisterGroup("events", "workers", 1))
	_, err := coordinator.AddConsumer("workers", "member-1")
	require.NoError(t, err)

	coordinator.mu.Lock()
	group := coordinator.groups["workers"]
	group.Members["member-1"].LastHeartbeat = time.Now().Add(-time.Minute)
	group.LastActivity = time.Unix(4, 0)
	group.LastRebalance = time.Unix(4, 0)
	coordinator.mu.Unlock()

	coordinator.checkSingleGroupTimeout("workers", time.Millisecond)
	observation := requireSingleObservation(t, coordinator)
	assert.Zero(t, observation.MemberCount)
	assert.Equal(t, ConsumerGroupStateEmpty, observation.State)
	assert.True(t, observation.LastActivity.After(time.Unix(4, 0)))
	assert.True(t, observation.LastRebalance.After(time.Unix(4, 0)))
}

func TestObserveConsumerGroupsFailsClosedOnCoordinatorLookup(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	coordinator := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.RegisterGroup("events", "workers", 1))
	_, err := coordinator.AddConsumer("workers", "member-1")
	require.NoError(t, err)
	coordinator.SetGroupObservationResolver(func(string) (bool, error) {
		return false, errors.New("dial broker.internal:9000: sensitive failure")
	})

	observation := requireSingleObservation(t, coordinator)
	assert.False(t, observation.CoordinatorUp)
	assert.Equal(t, ObservationFailureCoordinatorLookup, observation.ObservationError)
	assert.Zero(t, observation.MemberCount)
	assert.Empty(t, observation.State)
}

func TestObserveConsumerGroupsResolvesAllGroupsInOneBatch(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	coordinator := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.RegisterGroup("events", "alpha", 1))
	require.NoError(t, coordinator.RegisterGroup("events", "beta", 1))

	calls := 0
	coordinator.SetGroupObservationBatchResolver(func(groupNames []string) (map[string]bool, error) {
		calls++
		assert.ElementsMatch(t, []string{"alpha", "beta"}, groupNames)
		return map[string]bool{"alpha": true, "beta": false}, nil
	})

	observations := coordinator.ObserveConsumerGroups()
	require.Len(t, observations, 2)
	assert.Equal(t, 1, calls)
	assert.True(t, observations[0].CoordinatorUp)
	assert.False(t, observations[1].CoordinatorUp)
}

func TestObserveConsumerGroupsFailsClosedOnIncompleteBatchResult(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	coordinator := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.RegisterGroup("events", "workers", 1))
	coordinator.SetGroupObservationBatchResolver(func([]string) (map[string]bool, error) {
		return map[string]bool{}, nil
	})

	observation := requireSingleObservation(t, coordinator)
	assert.False(t, observation.CoordinatorUp)
	assert.Equal(t, ObservationFailureCoordinatorLookup, observation.ObservationError)
}

func TestObserveConsumerGroupsReportsConcurrentGroupDeletion(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	coordinator := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.RegisterGroup("events", "workers", 1))
	coordinator.SetGroupObservationResolver(func(groupName string) (bool, error) {
		coordinator.mu.Lock()
		delete(coordinator.groups, groupName)
		coordinator.mu.Unlock()
		return true, nil
	})

	observation := requireSingleObservation(t, coordinator)
	assert.False(t, observation.CoordinatorUp)
	assert.Equal(t, ObservationFailureGroupLookup, observation.ObservationError)
}

func TestObserveConsumerGroupsDoesNotFabricateUnknownGroup(t *testing.T) {
	coordinator := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(coordinator.Stop)
	require.Empty(t, coordinator.ObserveConsumerGroups())
}

func TestGroupSnapshotRequiresLastActivity(t *testing.T) {
	coordinator := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(coordinator.Stop)
	require.NoError(t, coordinator.RegisterGroup("events", "workers", 1))
	coordinator.mu.Lock()
	coordinator.groups["workers"].LastActivity = time.Unix(100, 0)
	coordinator.groups["workers"].LastRebalance = time.Unix(90, 0)
	coordinator.mu.Unlock()

	restored := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	t.Cleanup(restored.Stop)
	require.NoError(t, restored.ImportState(coordinator.ExportState()))
	assert.Equal(t, time.Unix(100, 0), requireSingleObservation(t, restored).LastActivity)

	incomplete := coordinator.ExportState()
	incomplete["workers"].LastActivity = time.Time{}
	require.ErrorContains(t, restored.ImportState(incomplete), "missing last activity")
	assert.Equal(t, time.Unix(100, 0), requireSingleObservation(t, restored).LastActivity)
}

func requireSingleObservation(t *testing.T, coordinator *Coordinator) ConsumerGroupObservation {
	t.Helper()
	observations := coordinator.ObserveConsumerGroups()
	require.Len(t, observations, 1)
	assert.Equal(t, "events", observations[0].TopicName)
	assert.Equal(t, "workers", observations[0].GroupName)
	return observations[0]
}

func joinedGeneration(coordinator *Coordinator, group string) int {
	return coordinator.GetGeneration(group)
}
