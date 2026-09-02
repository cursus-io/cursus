package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllMetricsInitialized(t *testing.T) {
	// Exported metric handles remain initialized even when a runtime collector
	// owns the corresponding scrape-time metric name.
	assert.NotNil(t, MessagesProcessed)
	assert.NotNil(t, MessagesPerSec)
	assert.NotNil(t, LatencyHist)
	assert.NotNil(t, QueueSize)
	assert.NotNil(t, CleanupCount)
	assert.NotNil(t, SeqNumGapTotal)
	assert.NotNil(t, SeqNumDuplicateTotal)
	assert.NotNil(t, ClientConnectionsTotal)
	assert.NotNil(t, ClientConnectionsActive)
	assert.NotNil(t, CommandsTotal)
	assert.NotNil(t, CommandDuration)
	assert.NotNil(t, CommandErrors)
	assert.NotNil(t, ClusterReplicationLag)
}

func TestMetricIncrements(t *testing.T) {
	// Just ensure they don't panic when used
	QueueSize.Set(100)
	CleanupCount.Inc()
	ClusterReplicationLag.WithLabelValues("t1", "0", "b1").Observe(0.01)

	SeqNumGapTotal.WithLabelValues("t1", "0", "producer-1").Inc()
	SeqNumDuplicateTotal.WithLabelValues("t1", "0").Inc()
}
