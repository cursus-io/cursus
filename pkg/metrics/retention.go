package metrics

import "github.com/prometheus/client_golang/prometheus"

// Bounded labels only; never include a topic, partition, path or timestamp.
var RetentionHolds = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "cursus_retention_holds_total",
	Help: "Retention decisions held for missing evidence or safety boundaries.",
}, []string{"reason"})

func RecordRetentionHold(reason string) {
	switch reason {
	case "clock_step", "unknown_time", "future_time", "corrupt_data", "event_history", "committed_boundary", "partition_unavailable":
	default:
		reason = "other"
	}
	RetentionHolds.WithLabelValues(reason).Inc()
}
