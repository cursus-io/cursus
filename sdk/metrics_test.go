package sdk

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func resetMetricsState() {
	metricsOnce = sync.Once{}
	metricsRegistry = nil
	producerMessagesSent = nil
	producerSendErrors = nil
	producerBatchLatency = nil
	consumerMessagesReceived = nil
	consumerCommitTotal = nil
	consumerCommitErrors = nil
	consumerPollLatency = nil
	consumerRebalanceTotal = nil
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
