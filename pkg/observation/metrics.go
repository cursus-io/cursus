package observation

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Descriptors are shared by the runtime and default-registry snapshot collectors.
var Descriptors = []*prometheus.Desc{
	prometheus.NewDesc("cursus_observation_valid", "Whether the completed observation is successful, fresh, and within its execution budget.", []string{"component"}, nil),
	prometheus.NewDesc("cursus_observation_in_progress", "Whether an observation call is still executing, including after its deadline.", []string{"component"}, nil),
	prometheus.NewDesc("cursus_observation_timed_out", "Whether an executing observation has exceeded its deadline.", []string{"component"}, nil),
	prometheus.NewDesc("cursus_observation_stale", "Whether the last completed observation is absent, superseded or expired.", []string{"component"}, nil),
	prometheus.NewDesc("cursus_observation_last_completion_timestamp_seconds", "Last actual completion time; absent until a call returns.", []string{"component"}, nil),
	prometheus.NewDesc("cursus_observation_age_seconds", "Age of the last completed observation; absent until a call returns.", []string{"component"}, nil),
	prometheus.NewDesc("cursus_observation_last_failed", "Whether the last completed observation failed; absent until a call returns.", []string{"component"}, nil),
	prometheus.NewDesc("cursus_observation_failures_total", "Completed failed observations, including calls returning after their deadline.", []string{"component"}, nil),
}

// CollectMetrics exports observation state without querying its sources.
func CollectMetrics(ch chan<- prometheus.Metric, component string, status Status) {
	ch <- metricGauge(Descriptors[0], boolValue(status.Valid()), component)
	ch <- metricGauge(Descriptors[1], boolValue(status.InProgress), component)
	ch <- metricGauge(Descriptors[2], boolValue(status.TimedOut), component)
	ch <- metricGauge(Descriptors[3], boolValue(status.Stale), component)
	if !status.CompletedAt.IsZero() {
		ch <- metricGauge(Descriptors[4], float64(status.CompletedAt.UnixNano())/1e9, component)
		ch <- metricGauge(Descriptors[5], time.Since(status.CompletedAt).Seconds(), component)
		ch <- metricGauge(Descriptors[6], boolValue(status.Error != ""), component)
	}
	ch <- prometheus.MustNewConstMetric(Descriptors[7], prometheus.CounterValue, float64(status.Failures), component)
}

func metricGauge(desc *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}
func boolValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
