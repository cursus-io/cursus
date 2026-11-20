package sdk

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resetMetricsState() {
	metricsOnce = sync.Once{}
	metricsRegistry = nil
	defaultMetrics = nil
	producerMessagesSent = nil
	producerSendErrors = nil
	producerBatchLatency = nil
	consumerMessagesReceived = nil
	consumerCommitTotal = nil
	consumerCommitErrors = nil
	consumerPollLatency = nil
	consumerRebalanceTotal = nil
	consumerOffsetGapTotal = nil
	consumerCompactedOffsetsSkipped = nil
	consumerStaleWorkers = nil
	consumerCoordinatorCommands = nil
	consumerCoordinatorRedirects = nil
	consumerCoordinatorResolution = nil
	consumerCoordinatorCacheRefresh = nil
	consumerCoordinatorRetry = nil
	consumerCoordinatorResolutionLag = nil
}

func TestNewMetrics_ReusesCollectorsAcrossConsumers(t *testing.T) {
	registry := prometheus.NewRegistry()
	first, err := NewMetrics(registry, registry)
	require.NoError(t, err)
	second, err := NewMetrics(registry, registry)
	require.NoError(t, err)

	first.consumerCoordinatorCommands.WithLabelValues("heartbeat", "consumer", "cache", "success").Inc()
	second.consumerCoordinatorCommands.WithLabelValues("heartbeat", "consumer", "cache", "success").Inc()

	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "cursus_consumer_coordinator_commands_total" {
			continue
		}
		require.Len(t, family.Metric, 1)
		require.Equal(t, float64(2), family.Metric[0].GetCounter().GetValue())
		return
	}
	t.Fatal("coordinator command metric was not gathered")
}

func TestMetricsHandler_SharesApplicationRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	applicationCollector := prometheus.NewGauge(prometheus.GaugeOpts{Name: "commerce_consumer_ready"})
	require.NoError(t, registry.Register(applicationCollector))
	metrics, err := NewMetrics(registry, registry)
	require.NoError(t, err)

	applicationCollector.Set(1)
	metrics.consumerMessagesReceived.WithLabelValues("orders", "workers").Inc()
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), "commerce_consumer_ready")
	require.Contains(t, recorder.Body.String(), "cursus_consumer_messages_received_total")
}

func TestInitMetrics_CreatesRegistry(t *testing.T) {
	resetMetricsState()
	t.Cleanup(resetMetricsState)

	assert.Nil(t, metricsRegistry)
	initMetrics()
	assert.NotNil(t, metricsRegistry)
	assert.NotNil(t, producerMessagesSent)
	assert.NotNil(t, consumerMessagesReceived)
}

func TestInitMetrics_IdempotentViaSyncOnce(t *testing.T) {
	resetMetricsState()
	t.Cleanup(resetMetricsState)

	initMetrics()
	first := metricsRegistry

	initMetrics()
	initMetrics()
	assert.Same(t, first, metricsRegistry)
}

func TestMetricsHandler_ReturnsNonNil(t *testing.T) {
	resetMetricsState()
	t.Cleanup(resetMetricsState)

	handler := MetricsHandler()
	assert.NotNil(t, handler)
	assert.Implements(t, (*http.Handler)(nil), handler)
}

func TestProducerMetrics_CanBeIncremented(t *testing.T) {
	resetMetricsState()
	t.Cleanup(resetMetricsState)
	initMetrics()

	producerMessagesSent.WithLabelValues("orders").Inc()
	producerSendErrors.WithLabelValues("orders").Inc()
	producerBatchLatency.WithLabelValues("orders").Observe(0.05)

	metrics, err := metricsRegistry.Gather()
	assert.NoError(t, err)

	names := make(map[string]bool)
	for _, m := range metrics {
		names[m.GetName()] = true
	}
	assert.True(t, names["cursus_producer_messages_sent_total"])
	assert.True(t, names["cursus_producer_send_errors_total"])
	assert.True(t, names["cursus_producer_batch_latency_seconds"])
}

func TestConsumerMetrics_CanBeIncremented(t *testing.T) {
	resetMetricsState()
	t.Cleanup(resetMetricsState)
	initMetrics()

	consumerMessagesReceived.WithLabelValues("events", "grp1").Inc()
	consumerCommitTotal.WithLabelValues("events", "grp1").Inc()
	consumerCommitErrors.WithLabelValues("events", "grp1").Inc()
	consumerPollLatency.WithLabelValues("events", "grp1").Observe(0.01)
	consumerRebalanceTotal.WithLabelValues("events", "grp1").Inc()
	consumerCompactedOffsetsSkipped.WithLabelValues("events", "grp1").Add(3)
	consumerStaleWorkers.WithLabelValues("events", "grp1", "poller").Inc()
	consumerCoordinatorCommands.WithLabelValues("heartbeat", "consumer", "cache", "success").Inc()
	consumerCoordinatorRedirects.WithLabelValues("heartbeat", "consumer", "cache").Inc()
	consumerCoordinatorResolution.WithLabelValues("success", "initial", "consumer").Inc()
	consumerCoordinatorCacheRefresh.WithLabelValues("success", "initial", "consumer").Inc()
	consumerCoordinatorRetry.WithLabelValues("heartbeat", "success", "consumer").Inc()
	consumerCoordinatorResolutionLag.WithLabelValues("success", "initial", "consumer").Observe(0.01)

	metrics, err := metricsRegistry.Gather()
	assert.NoError(t, err)

	names := make(map[string]bool)
	for _, m := range metrics {
		names[m.GetName()] = true
	}
	assert.True(t, names["cursus_consumer_messages_received_total"])
	assert.True(t, names["cursus_consumer_commit_total"])
	assert.True(t, names["cursus_consumer_commit_errors_total"])
	assert.True(t, names["cursus_consumer_poll_latency_seconds"])
	assert.True(t, names["cursus_consumer_rebalance_total"])
	assert.True(t, names["cursus_consumer_compacted_offsets_skipped_total"])
	assert.True(t, names["cursus_consumer_stale_workers_total"])
	assert.True(t, names["cursus_consumer_coordinator_commands_total"])
	assert.True(t, names["cursus_consumer_coordinator_redirects_total"])
	assert.True(t, names["cursus_consumer_coordinator_resolution_total"])
	assert.True(t, names["cursus_consumer_coordinator_cache_refresh_total"])
	assert.True(t, names["cursus_consumer_coordinator_retry_total"])
	assert.True(t, names["cursus_consumer_coordinator_resolution_duration_seconds"])
}

func TestMetricsHandler_ServesPrometheusFormat(t *testing.T) {
	resetMetricsState()
	t.Cleanup(resetMetricsState)

	handler := MetricsHandler()
	producerMessagesSent.WithLabelValues("test-topic").Add(3)
	consumerMessagesReceived.WithLabelValues("test-topic", "test-group").Add(7)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.True(t, strings.Contains(body, "cursus_producer_messages_sent_total"))
	assert.True(t, strings.Contains(body, "cursus_consumer_messages_received_total"))
	assert.True(t, strings.Contains(body, "test-topic"))
	assert.True(t, strings.Contains(body, `topic="test-topic"`))
}

func TestRecordStaleWorkerCountsOnlySupersededAssignments(t *testing.T) {
	resetMetricsState()
	t.Cleanup(resetMetricsState)
	initMetrics()

	consumer := &Consumer{config: &ConsumerConfig{Topic: "events", GroupID: "grp1", EnableMetrics: true}}
	consumer.assignmentGeneration.Store(2)
	consumer.recordStaleWorker("poller", 2)
	consumer.recordStaleWorker("poller", 1)

	metric, err := consumerStaleWorkers.GetMetricWithLabelValues("events", "grp1", "poller")
	assert.NoError(t, err)
	value := &dto.Metric{}
	assert.NoError(t, metric.Write(value))
	assert.Equal(t, float64(1), value.GetCounter().GetValue())
}
