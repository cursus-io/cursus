package observability

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

var offsetMetricNames = []string{
	"cursus_consumer_group_committed_offset",
	"cursus_consumer_group_lag",
	"cursus_consumer_group_offset_out_of_range",
}

func gatherOffsetMetrics(t *testing.T, collector prometheus.Collector) []*dto.MetricFamily {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)
	families, err := registry.Gather()
	require.NoError(t, err)
	return families
}

func TestConsumerOffsetsFollowCoordinatorAndOmitStaleReplicas(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	owner := 0
	brokers := make([]*coordinator.Coordinator, 3)
	collectors := make([]*sampleCollector, 3)
	labels := map[string]string{"group": "workers", "topic": "gameplay.death", "partition": "0"}
	for index, committed := range []uint64{5142, 1950, 1844} {
		broker := coordinator.NewCoordinator(context.Background(), cfg, lifecycleTopicHandler{})
		t.Cleanup(broker.Stop)
		require.NoError(t, broker.RegisterGroup("gameplay.death", "workers", 1))
		state := broker.ExportState()
		state["workers"].Offsets = map[string]map[int]uint64{"gameplay.death": {0: committed}}
		require.NoError(t, broker.ImportState(state))
		broker.SetGroupObservationBatchResolver(func(names []string) (map[string]bool, error) {
			require.Equal(t, []string{"workers"}, names)
			if owner < 0 {
				return nil, errors.New("no active coordinator")
			}
			return map[string]bool{"workers": index == owner}, nil
		})
		brokers[index] = broker
		collectors[index] = newSampleCollector(fixedTopics{snapshot: topic.RuntimeSnapshot{
			TopicCount: 1, Partitions: []topic.PartitionRuntimeSnapshot{{Topic: "gameplay.death", Partition: 0, LogStart: 5142, HighWatermark: 5142, LogEnd: 5142}},
		}}, broker, fixedDisk{}, nil, nil, fixedReadiness(true))
	}
	check := func() {
		t.Helper()
		series, maxLag, maxOutOfRange := 0, 0.0, 0.0
		for index, collector := range collectors {
			families := gatherOffsetMetrics(t, collector)
			up := 0.0
			if index == owner {
				up = 1
			}
			assertGauge(t, families, "cursus_consumer_group_coordinator_up", map[string]string{"topic": "gameplay.death", "group": "workers"}, up)
			if index != owner {
				for _, name := range offsetMetricNames {
					assertMetricMissing(t, families, name, labels)
				}
				assertMetricFamilyMissing(t, families, "cursus_consumer_group_generation")
				assertMetricFamilyMissing(t, families, "cursus_consumer_group_assigned_partitions")
				continue
			}
			series++
			assertGauge(t, families, offsetMetricNames[0], labels, 5142)
			lag, found := metricGaugeValue(families, offsetMetricNames[1], labels)
			require.True(t, found)
			outside, found := metricGaugeValue(families, offsetMetricNames[2], labels)
			require.True(t, found)
			maxLag, maxOutOfRange = max(maxLag, lag), max(maxOutOfRange, outside)
		}
		if owner < 0 {
			require.Zero(t, series)
		} else {
			require.Equal(t, 1, series)
		}
		require.Zero(t, maxLag)
		require.Zero(t, maxOutOfRange)
	}
	check()
	owner = -1
	check() // no owner means absence, not three reassuring zero lag values
	// Ownership replay installs the current commit before the new owner serves.
	require.NoError(t, brokers[1].ImportState(brokers[0].ExportState()))
	owner = 1
	check()
}

// unknownAuthorityGroups deliberately provides no lifecycle/ownership contract.
type unknownAuthorityGroups struct {
	state map[string]*coordinator.GroupStateSnapshot
}

func (s unknownAuthorityGroups) ExportState() map[string]*coordinator.GroupStateSnapshot {
	return s.state
}

func TestConsumerOffsetUnknownStateIsNotZero(t *testing.T) {
	state := map[string]*coordinator.GroupStateSnapshot{"workers": {
		TopicName: "events", Offsets: map[string]map[int]uint64{"events": {0: 0, 2: 10}},
	}}
	topics := fixedTopics{snapshot: topic.RuntimeSnapshot{TopicCount: 1, Partitions: []topic.PartitionRuntimeSnapshot{
		{Topic: "events", Partition: 0, HighWatermark: 5}, {Topic: "events", Partition: 1, HighWatermark: 5},
	}}}
	known := gatherOffsetMetrics(t, newSampleCollector(topics, fixedGroups{snapshot: state}, fixedDisk{}, nil, nil, fixedReadiness(true)))
	assertGauge(t, known, offsetMetricNames[0], map[string]string{"group": "workers", "topic": "events", "partition": "0"}, 0)
	assertGauge(t, known, offsetMetricNames[1], map[string]string{"group": "workers", "topic": "events", "partition": "0"}, 5)
	for _, partition := range []string{"1", "2"} {
		for _, name := range offsetMetricNames {
			assertMetricMissing(t, known, name, map[string]string{"group": "workers", "topic": "events", "partition": partition})
		}
	}
	unknown := gatherOffsetMetrics(t, newSampleCollector(topics, unknownAuthorityGroups{state: state}, fixedDisk{}, nil, nil, fixedReadiness(true)))
	for _, name := range offsetMetricNames {
		assertMetricFamilyMissing(t, unknown, name)
	}
	assertGauge(t, unknown, "cursus_consumer_group_coordinator_up", map[string]string{"topic": "events", "group": "workers"}, 0)
	assertCounter(t, unknown, "cursus_consumer_group_observation_failures_total", map[string]string{"topic": "events", "group": "workers", "reason": "coordinator_lookup"}, 1)
}

func TestConsumerOffsetsUseOneAuthorityObservationPerSample(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	broker := coordinator.NewCoordinator(context.Background(), cfg, lifecycleTopicHandler{})
	t.Cleanup(broker.Stop)
	require.NoError(t, broker.RegisterGroup("events", "workers", 1))
	state := broker.ExportState()
	state["workers"].Offsets = map[string]map[int]uint64{"events": {0: 5}}
	require.NoError(t, broker.ImportState(state))
	calls := 0
	broker.SetGroupObservationBatchResolver(func([]string) (map[string]bool, error) {
		calls++
		return map[string]bool{"workers": calls%2 == 1}, nil
	})
	collector := newSampleCollector(fixedTopics{snapshot: topic.RuntimeSnapshot{Partitions: []topic.PartitionRuntimeSnapshot{{Topic: "events", Partition: 0, HighWatermark: 5}}}}, broker, fixedDisk{}, nil, nil, fixedReadiness(true))
	first := gatherOffsetMetrics(t, collector)
	require.Equal(t, 1, calls)
	assertGauge(t, first, offsetMetricNames[0], map[string]string{"group": "workers", "topic": "events", "partition": "0"}, 5)
	second := gatherOffsetMetrics(t, collector)
	require.Equal(t, 2, calls)
	for _, name := range offsetMetricNames {
		assertMetricFamilyMissing(t, second, name)
	}
}

func TestConsumerOffsetsAreAbsentDuringRecovery(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	broker := coordinator.NewCoordinator(context.Background(), cfg, lifecycleTopicHandler{})
	t.Cleanup(broker.Stop)
	require.NoError(t, broker.RegisterGroup("events", "workers", 1))
	state := broker.ExportState()
	state["workers"].Offsets = map[string]map[int]uint64{"events": {0: 5}}
	require.NoError(t, broker.ImportState(state))
	broker.SetGroupObservationResolver(func(string) (bool, error) { return true, nil })
	collector := newSampleCollector(fixedTopics{snapshot: topic.RuntimeSnapshot{Partitions: []topic.PartitionRuntimeSnapshot{{Topic: "events", Partition: 0, HighWatermark: 5}}}}, broker, fixedDisk{}, nil, nil, fixedReadiness(true))
	release, err := broker.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	defer release()
	families := gatherOffsetMetrics(t, collector)
	for _, name := range offsetMetricNames {
		assertMetricFamilyMissing(t, families, name)
	}
	assertMetricFamilyMissing(t, families, "cursus_consumer_group_coordinator_up")
	release()
	families = gatherOffsetMetrics(t, collector)
	assertGauge(t, families, offsetMetricNames[0], map[string]string{"group": "workers", "topic": "events", "partition": "0"}, 5)
}

func TestConsumerOffsetWildcardUsesSubscriptionAuthorityAndConcreteTopic(t *testing.T) {
	state := map[string]*coordinator.GroupStateSnapshot{"workers": {
		TopicName: "events.*", Offsets: map[string]map[int]uint64{"events.orders": {0: 5}},
	}, "deleted": {TopicName: "events.*", Deleted: true, Offsets: map[string]map[int]uint64{"events.orders": {0: 1}}}}
	families := gatherOffsetMetrics(t, newSampleCollector(fixedTopics{snapshot: topic.RuntimeSnapshot{
		Partitions: []topic.PartitionRuntimeSnapshot{{Topic: "events.orders", Partition: 0, HighWatermark: 5}},
	}}, fixedGroups{snapshot: state}, fixedDisk{}, nil, nil, fixedReadiness(true)))
	assertGauge(t, families, "cursus_consumer_group_coordinator_up", map[string]string{"group": "workers", "topic": "events.*"}, 1)
	assertGauge(t, families, offsetMetricNames[0], map[string]string{"group": "workers", "topic": "events.orders", "partition": "0"}, 5)
	for _, name := range offsetMetricNames {
		assertMetricMissing(t, families, name, map[string]string{"group": "deleted", "topic": "events.orders", "partition": "0"})
	}
}
