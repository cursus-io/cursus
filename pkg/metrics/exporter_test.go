package metrics_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func getCounterValue(c prometheus.Counter) float64 {
	m := &dto.Metric{}
	_ = c.Write(m)
	return m.GetCounter().GetValue()
}

func getGaugeValue(g prometheus.Gauge) float64 {
	m := &dto.Metric{}
	_ = g.Write(m)
	return m.GetGauge().GetValue()
}

func getHistogramCount(h prometheus.Histogram) uint64 {
	m := &dto.Metric{}
	_ = h.Write(m)
	return m.GetHistogram().GetSampleCount()
}

func TestPushMetric(t *testing.T) {
	initialMessages := getCounterValue(metrics.MessagesProcessed)
	initialLatency := getHistogramCount(metrics.LatencyHist)

	metrics.PushMetric("testTopic", 0.5)
	metrics.PushMetric("testTopic", 0.2)

	if getCounterValue(metrics.MessagesProcessed) != initialMessages+2 {
		t.Fatalf("MessagesProcessed counter expected %v, got %v", initialMessages+2, getCounterValue(metrics.MessagesProcessed))
	}

	if getHistogramCount(metrics.LatencyHist) != initialLatency+2 {
		t.Fatalf("LatencyHist count expected %v, got %v", initialLatency+2, getHistogramCount(metrics.LatencyHist))
	}

	if got := getGaugeValue(metrics.MessagesPerSec); got != 1.0/0.2 {
		t.Fatalf("MessagesPerSec expected %v, got %v", 1.0/0.2, got)
	}
}
