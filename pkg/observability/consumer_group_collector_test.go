package observability

import (
	"context"
	"errors"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lifecycleTopicHandler struct{}

func (lifecycleTopicHandler) CreateTopic(string, int, bool, bool) error { return nil }
func (lifecycleTopicHandler) Publish(string, *types.Message) error      { return nil }

func TestCollectorExportsAuthoritativeConsumerLifecycle(t *testing.T) {
	groups := coordinator.NewCoordinator(context.Background(), config.DefaultConfig(), lifecycleTopicHandler{})
	t.Cleanup(groups.Stop)
	require.NoError(t, groups.RegisterGroup("events", "workers", 1))
	_, err := groups.AddConsumer("workers", "member-sensitive-id")
	require.NoError(t, err)

	registry := prometheus.NewRegistry()
	registry.MustRegister(NewCollector(
		fixedTopics{snapshot: topic.RuntimeSnapshot{TopicCount: 1, Partitions: []topic.PartitionRuntimeSnapshot{{Topic: "events", Partition: 0}}}},
		groups, fixedDisk{}, nil, nil, fixedReadiness(true),
	))
	families, err := registry.Gather()
	require.NoError(t, err)

	assertGauge(t, families, "cursus_consumer_group_members", map[string]string{"topic": "events", "group": "workers"}, 1)
	assertGauge(t, families, "cursus_consumer_group_state", map[string]string{"topic": "events", "group": "workers", "state": "stable"}, 1)
	assertGauge(t, families, "cursus_consumer_group_state", map[string]string{"topic": "events", "group": "workers", "state": "empty"}, 0)
	assertGauge(t, families, "cursus_consumer_group_coordinator_up", map[string]string{"topic": "events", "group": "workers"}, 1)
	assertPositiveGauge(t, families, "cursus_consumer_group_last_activity_timestamp_seconds", map[string]string{"topic": "events", "group": "workers"})
	assertPositiveGauge(t, families, "cursus_consumer_group_last_rebalance_timestamp_seconds", map[string]string{"topic": "events", "group": "workers"})
	assertMetricFamilyMissing(t, families, "cursus_consumer_group_observation_failures_total")
}

func TestCollectorCountsBoundedObservationFailuresAndOmitsStaleState(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	groups := coordinator.NewCoordinator(context.Background(), cfg, lifecycleTopicHandler{})
	t.Cleanup(groups.Stop)
	require.NoError(t, groups.RegisterGroup("events", "workers", 1))
	_, err := groups.AddConsumer("workers", "member-sensitive-id")
	require.NoError(t, err)
	groups.SetGroupObservationResolver(func(string) (bool, error) {
		return false, errors.New("dial broker.internal:9000: raw-sensitive-error")
	})

	registry := prometheus.NewRegistry()
	registry.MustRegister(NewCollector(
		fixedTopics{snapshot: topic.RuntimeSnapshot{TopicCount: 1, Partitions: []topic.PartitionRuntimeSnapshot{{Topic: "events", Partition: 0}}}},
		groups, fixedDisk{}, nil, nil, fixedReadiness(true),
	))
	first, err := registry.Gather()
	require.NoError(t, err)
	assertGauge(t, first, "cursus_consumer_group_coordinator_up", map[string]string{"topic": "events", "group": "workers"}, 0)
	assertMetricMissing(t, first, "cursus_consumer_group_members", map[string]string{"topic": "events", "group": "workers"})
	assertCounter(t, first, "cursus_consumer_group_observation_failures_total", map[string]string{
		"topic": "events", "group": "workers", "reason": "coordinator_lookup",
	}, 1)

	second, err := registry.Gather()
	require.NoError(t, err)
	assertCounter(t, second, "cursus_consumer_group_observation_failures_total", map[string]string{
		"topic": "events", "group": "workers", "reason": "coordinator_lookup",
	}, 2)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(recorder, request)
	body := recorder.Body.String()
	for _, sensitive := range []string{"member-sensitive-id", "broker.internal:9000", "raw-sensitive-error"} {
		assert.NotContains(t, body, sensitive)
	}
}

func TestCollectorCountsTopicLookupFailuresWithoutInventingExpectedGroups(t *testing.T) {
	groups := coordinator.NewCoordinator(context.Background(), config.DefaultConfig(), lifecycleTopicHandler{})
	t.Cleanup(groups.Stop)
	require.NoError(t, groups.RegisterGroup("missing-topic", "known-group", 1))
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewCollector(fixedTopics{}, groups, fixedDisk{}, nil, nil, fixedReadiness(true)))

	families, err := registry.Gather()
	require.NoError(t, err)
	assertCounter(t, families, "cursus_consumer_group_observation_failures_total", map[string]string{
		"topic": "missing-topic", "group": "known-group", "reason": "topic_lookup",
	}, 1)
	assertGauge(t, families, "cursus_consumer_group_coordinator_up", map[string]string{"topic": "missing-topic", "group": "known-group"}, 1)
	assertMetricMissing(t, families, "cursus_consumer_group_members", map[string]string{"topic": "missing-topic", "group": "known-group"})
	assertMetricMissing(t, families, "cursus_consumer_group_members", map[string]string{"topic": "absent-topic", "group": "absent-group"})
}

func TestCollectorThreeBrokerCoordinatorMovementConvergesWithoutDuplicateMembers(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	brokers := make([]*coordinator.Coordinator, 3)
	for i := range brokers {
		brokers[i] = coordinator.NewCoordinator(context.Background(), cfg, lifecycleTopicHandler{})
		t.Cleanup(brokers[i].Stop)
	}
	require.NoError(t, brokers[0].RegisterGroup("events", "workers", 1))
	_, err := brokers[0].AddConsumer("workers", "member-1")
	require.NoError(t, err)
	state := brokers[0].ExportState()
	brokers[1].ImportState(state)
	brokers[2].ImportState(state)

	owner := 1
	collectors := make([]*Collector, len(brokers))
	for index, broker := range brokers {
		brokerIndex := index
		broker.SetGroupObservationBatchResolver(func(groupNames []string) (map[string]bool, error) {
			resolved := make(map[string]bool, len(groupNames))
			for _, groupName := range groupNames {
				resolved[groupName] = brokerIndex == owner
			}
			return resolved, nil
		})
		collectors[index] = NewCollector(
			fixedTopics{snapshot: topic.RuntimeSnapshot{TopicCount: 1, Partitions: []topic.PartitionRuntimeSnapshot{{Topic: "events", Partition: 0}}}},
			broker, fixedDisk{}, nil, nil, fixedReadiness(true),
		)
	}
	assertThreeBrokerLifecycleAggregate(t, collectors, owner, 1)

	owner = 2
	require.NoError(t, brokers[owner].RecordHeartbeatForGeneration("workers", "member-1", brokers[owner].GetGeneration("workers")))
	assertThreeBrokerLifecycleAggregate(t, collectors, owner, 1)
}

func TestConsumerLifecycleMetricLabelsAreBounded(t *testing.T) {
	groups := coordinator.NewCoordinator(context.Background(), config.DefaultConfig(), lifecycleTopicHandler{})
	t.Cleanup(groups.Stop)
	require.NoError(t, groups.RegisterGroup("events", "workers", 1))
	require.NoError(t, groups.RegisterGroup("missing-topic", "broken", 1))
	_, err := groups.AddConsumer("workers", "member-sensitive-id")
	require.NoError(t, err)
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewCollector(
		fixedTopics{snapshot: topic.RuntimeSnapshot{Partitions: []topic.PartitionRuntimeSnapshot{{Topic: "events", Partition: 0}}}},
		groups, fixedDisk{}, nil, nil, fixedReadiness(true),
	))
	families, err := registry.Gather()
	require.NoError(t, err)

	expected := map[string][]string{
		"cursus_consumer_group_members":                          {"group", "topic"},
		"cursus_consumer_group_state":                            {"group", "state", "topic"},
		"cursus_consumer_group_coordinator_up":                   {"group", "topic"},
		"cursus_consumer_group_last_activity_timestamp_seconds":  {"group", "topic"},
		"cursus_consumer_group_last_rebalance_timestamp_seconds": {"group", "topic"},
		"cursus_consumer_group_observation_failures_total":       {"group", "reason", "topic"},
	}
	for metricName, want := range expected {
		family := metricFamily(t, families, metricName)
		for _, metric := range family.Metric {
			got := make([]string, 0, len(metric.Label))
			for _, pair := range metric.Label {
				got = append(got, pair.GetName())
			}
			sort.Strings(got)
			assert.Equal(t, want, got, metricName)
		}
	}
}

func assertThreeBrokerLifecycleAggregate(t *testing.T, collectors []*Collector, owner, wantMembers int) {
	t.Helper()
	coordinatorSum := 0.0
	memberSeries := 0
	memberSum := 0.0
	for index, collector := range collectors {
		registry := prometheus.NewRegistry()
		registry.MustRegister(collector)
		families, err := registry.Gather()
		require.NoError(t, err)
		up, found := metricGaugeValue(families, "cursus_consumer_group_coordinator_up", map[string]string{"topic": "events", "group": "workers"})
		require.True(t, found)
		coordinatorSum += up
		members, hasMembers := metricGaugeValue(families, "cursus_consumer_group_members", map[string]string{"topic": "events", "group": "workers"})
		if hasMembers {
			memberSeries++
			memberSum += members
			assert.Equal(t, owner, index)
		}
	}
	assert.Equal(t, 1.0, coordinatorSum)
	assert.Equal(t, 1, memberSeries)
	assert.Equal(t, float64(wantMembers), memberSum)
}

func assertPositiveGauge(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) {
	t.Helper()
	value, found := metricGaugeValue(families, name, labels)
	require.True(t, found, name)
	assert.Greater(t, value, float64(time.Unix(1, 0).Unix()))
}

func assertMetricMissing(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) {
	t.Helper()
	_, found := metricGaugeValue(families, name, labels)
	assert.False(t, found, "%s%v unexpectedly present", name, labels)
}

func metricGaugeValue(families []*dto.MetricFamily, name string, labels map[string]string) (float64, bool) {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if labelsMatch(metric.Label, labels) {
				return metric.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func metricFamily(t *testing.T, families []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %s not found", name)
	return nil
}

func assertMetricFamilyMissing(t *testing.T, families []*dto.MetricFamily, name string) {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			t.Fatalf("metric family %s unexpectedly present", name)
		}
	}
}
