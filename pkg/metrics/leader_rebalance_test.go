package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func collectedMetricValues(t *testing.T, collector prometheus.Collector) []float64 {
	t.Helper()
	stream := make(chan prometheus.Metric)
	go func() {
		collector.Collect(stream)
		close(stream)
	}()
	var values []float64
	for metric := range stream {
		var value dto.Metric
		require.NoError(t, metric.Write(&value))
		switch {
		case value.Gauge != nil:
			values = append(values, value.GetGauge().GetValue())
		case value.Counter != nil:
			values = append(values, value.GetCounter().GetValue())
		}
	}
	return values
}

func collectedMetricValue(t *testing.T, collector prometheus.Collector) float64 {
	t.Helper()
	values := collectedMetricValues(t, collector)
	require.Len(t, values, 1)
	return values[0]
}

func TestLeaderRebalanceMetricsUseBoundedLabelsAndReplaceBrokerSeries(t *testing.T) {
	metrics := newLeaderRebalanceMetrics()
	metrics.setPlan(3, 2, map[string]int{"n1": 1, "n2": 2}, map[string]int{"n1": 3})
	require.Equal(t, float64(3), collectedMetricValue(t, metrics.imbalanced))
	require.Equal(t, float64(2), collectedMetricValue(t, metrics.eligible))
	require.Equal(t, float64(2), collectedMetricValue(t, metrics.broker.WithLabelValues("n2", "current")))

	metrics.setPlan(0, 0, map[string]int{"n1": 1}, map[string]int{"n1": 1})
	require.Len(t, collectedMetricValues(t, metrics.broker), 2)
	metrics.recordMove("unexpected", "private error text")
	require.Equal(t, float64(1), collectedMetricValue(t, metrics.moves.WithLabelValues("failure", "rejected")))
	metrics.observeCycle(time.Second, 1)
	require.Positive(t, collectedMetricValue(t, metrics.lastSuccess))
}
