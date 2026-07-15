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
	assert.NotNil(t, ConsumerLag)
	assert.NotNil(t, ClientConnectionsTotal)
	assert.NotNil(t, ClientConnectionsActive)
	assert.NotNil(t, CommandsTotal)
	assert.NotNil(t, CommandDuration)
	assert.NotNil(t, CommandErrors)
	assert.NotNil(t, ClusterBrokersTotal)
	assert.NotNil(t, PartitionLeadersTotal)
	assert.NotNil(t, ClusterReplicationLag)
	assert.NotNil(t, LeaderElectionTotal)
	assert.NotNil(t, ISRSize)
	assert.NotNil(t, ReplicationLagBytes)
}

func TestMetricIncrements(t *testing.T) {
	// Just ensure they don't panic when used
	QueueSize.Set(100)
	CleanupCount.Inc()
	ClusterBrokersTotal.WithLabelValues("c1").Set(3)
	PartitionLeadersTotal.WithLabelValues("b1").Inc()
	ISRSize.WithLabelValues("t1", "0").Set(2)

	SeqNumGapTotal.WithLabelValues("t1", "0", "producer-1").Inc()
	SeqNumDuplicateTotal.WithLabelValues("t1", "0").Inc()
	ConsumerLag.WithLabelValues("t1", "0", "g1").Set(42)
}
