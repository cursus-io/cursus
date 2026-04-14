package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllMetricsRegistered(t *testing.T) {
	// If any of these are not nil, it means they were initialized.
	// Since init() calls MustRegister, they should be valid.
	assert.NotNil(t, MessagesProcessed)
	assert.NotNil(t, MessagesPerSec)
	assert.NotNil(t, LatencyHist)
	assert.NotNil(t, QueueSize)
	assert.NotNil(t, CleanupCount)
	
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
}
