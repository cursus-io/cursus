package sdk

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestFetchMetadataEnablesCompactionGapClassification(t *testing.T) {
	response := "OK topic=state partitions=1 leaders=broker-1:9001 epochs=1 cleanup_policy=compact"
	addr, result := startAdminTestServer(t, response)
	config := NewDefaultConsumerConfig()
	config.BrokerAddrs = []string{addr}
	config.Topic = "state"
	consumer, err := NewConsumer(config)
	require.NoError(t, err)

	require.NoError(t, consumer.fetchMetadata())
	require.True(t, consumer.compactionEnabled.Load())
	require.Equal(t, "METADATA topic=state", receiveAdminTestCommand(t, result))
	require.Equal(t, "broker-1:9001", consumer.getPartitionLeaderAddr(0), "metadata response: %s", response)
}

func TestCleanupPolicyIncludesCompaction(t *testing.T) {
	tests := map[string]bool{
		"delete":         false,
		"compact":        true,
		"delete,compact": true,
		"compact,delete": true,
		"invalid,compact": false,
		"":               false,
	}
	for policy, expected := range tests {
		require.Equal(t, expected, cleanupPolicyIncludesCompaction(policy), policy)
	}
}

func TestRecordOffsetAdvanceClassifiesCompactedHoles(t *testing.T) {
	resetMetricsState()
	t.Cleanup(resetMetricsState)
	initMetrics()

	config := NewDefaultConsumerConfig()
	config.Topic = "state"
	config.GroupID = "reader"
	config.EnableMetrics = true
	consumer := &Consumer{config: config}
	consumer.compactionEnabled.Store(true)
	partition := &PartitionConsumer{partitionID: 0, consumer: consumer}

	partition.recordOffsetAdvance(0, []Message{{Offset: 3}, {Offset: 5}}, true)

	require.Equal(t, float64(4), counterValue(t, consumerCompactedOffsetsSkipped, "state", "reader"))
	require.Zero(t, counterValue(t, consumerOffsetGapTotal, "state", "reader"))
}

func TestRecordOffsetAdvancePreservesUnexpectedGapMetricForDeletePolicy(t *testing.T) {
	resetMetricsState()
	t.Cleanup(resetMetricsState)
	initMetrics()

	config := NewDefaultConsumerConfig()
	config.Topic = "events"
	config.GroupID = "reader"
	config.EnableMetrics = true
	consumer := &Consumer{config: config}
	partition := &PartitionConsumer{partitionID: 0, consumer: consumer}

	partition.recordOffsetAdvance(2, []Message{{Offset: 5}}, true)

	require.Equal(t, float64(3), counterValue(t, consumerOffsetGapTotal, "events", "reader"))
	require.Zero(t, counterValue(t, consumerCompactedOffsetsSkipped, "events", "reader"))
}

func TestCountSkippedOffsetsIncludesInteriorHoles(t *testing.T) {
	require.Equal(t, uint64(4), countSkippedOffsets(0, []Message{{Offset: 3}, {Offset: 5}}))
	require.Zero(t, countSkippedOffsets(3, []Message{{Offset: 3}, {Offset: 4}}))
}

func counterValue(t *testing.T, counter *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	metric, err := counter.GetMetricWithLabelValues(labels...)
	require.NoError(t, err)
	value := &dto.Metric{}
	require.NoError(t, metric.Write(value))
	return value.GetCounter().GetValue()
}
