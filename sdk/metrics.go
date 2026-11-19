package sdk

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricsOnce     sync.Once
	metricsRegistry *prometheus.Registry

	producerMessagesSent *prometheus.CounterVec
	producerSendErrors   *prometheus.CounterVec
	producerBatchLatency *prometheus.HistogramVec

	consumerMessagesReceived         *prometheus.CounterVec
	consumerCommitTotal              *prometheus.CounterVec
	consumerCommitErrors             *prometheus.CounterVec
	consumerPollLatency              *prometheus.HistogramVec
	consumerRebalanceTotal           *prometheus.CounterVec
	consumerOffsetGapTotal           *prometheus.CounterVec
	consumerCompactedOffsetsSkipped  *prometheus.CounterVec
	consumerStaleWorkers             *prometheus.CounterVec
	consumerCoordinatorCommands      *prometheus.CounterVec
	consumerCoordinatorRedirects     *prometheus.CounterVec
	consumerCoordinatorResolution    *prometheus.CounterVec
	consumerCoordinatorCacheRefresh  *prometheus.CounterVec
	consumerCoordinatorRetry         *prometheus.CounterVec
	consumerCoordinatorResolutionLag *prometheus.HistogramVec
)

func initMetrics() {
	metricsOnce.Do(func() {
		metricsRegistry = prometheus.NewRegistry()

		producerMessagesSent = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "producer",
				Name:      "messages_sent_total",
				Help:      "Total number of messages successfully sent.",
			},
			[]string{"topic"},
		)

		producerSendErrors = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "producer",
				Name:      "send_errors_total",
				Help:      "Total number of send errors.",
			},
			[]string{"topic"},
		)

		producerBatchLatency = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "cursus",
				Subsystem: "producer",
				Name:      "batch_latency_seconds",
				Help:      "Latency of batch send operations in seconds.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"topic"},
		)

		consumerMessagesReceived = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "messages_received_total",
				Help:      "Total number of messages received.",
			},
			[]string{"topic", "group"},
		)

		consumerCommitTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "commit_total",
				Help:      "Total number of offset commits.",
			},
			[]string{"topic", "group"},
		)

		consumerCommitErrors = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "commit_errors_total",
				Help:      "Total number of offset commit errors.",
			},
			[]string{"topic", "group"},
		)

		consumerPollLatency = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "poll_latency_seconds",
				Help:      "Latency of poll operations in seconds.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"topic", "group"},
		)

		consumerRebalanceTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "rebalance_total",
				Help:      "Total number of consumer rebalance events.",
			},
			[]string{"topic", "group"},
		)

		consumerOffsetGapTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "offset_gap_total",
				Help:      "Total number of offset gaps detected during consumption.",
			},
			[]string{"topic", "group"},
		)

		consumerCompactedOffsetsSkipped = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "compacted_offsets_skipped_total",
				Help:      "Total number of logical offsets skipped while consuming compacted topics.",
			},
			[]string{"topic", "group"},
		)

		consumerStaleWorkers = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "stale_workers_total",
				Help:      "Assignment workers fenced after their consumer generation became stale.",
			},
			[]string{"topic", "group", "worker"},
		)

		consumerCoordinatorCommands = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "coordinator_commands_total",
				Help:      "Completed consumer coordinator command attempts by bounded command, client role, route source, and result.",
			},
			[]string{"command", "client_role", "route_source", "result"},
		)

		consumerCoordinatorRedirects = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "coordinator_redirects_total",
				Help:      "NOT_COORDINATOR responses received by consumer coordinator commands.",
			},
			[]string{"command", "client_role", "route_source"},
		)

		consumerCoordinatorResolution = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "coordinator_resolution_total",
				Help:      "Consumer coordinator resolution attempts by bounded result, reason, and client role.",
			},
			[]string{"result", "reason", "client_role"},
		)

		consumerCoordinatorCacheRefresh = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "coordinator_cache_refresh_total",
				Help:      "Consumer coordinator cache refresh outcomes by bounded result, reason, and client role.",
			},
			[]string{"result", "reason", "client_role"},
		)

		consumerCoordinatorRetry = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "coordinator_retry_total",
				Help:      "Consumer coordinator command attempts made on a route learned after NOT_COORDINATOR.",
			},
			[]string{"command", "result", "client_role"},
		)

		consumerCoordinatorResolutionLag = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "cursus",
				Subsystem: "consumer",
				Name:      "coordinator_resolution_duration_seconds",
				Help:      "Time spent resolving or reconnecting a consumer coordinator.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"result", "reason", "client_role"},
		)

		metricsRegistry.MustRegister(
			producerMessagesSent,
			producerSendErrors,
			producerBatchLatency,
			consumerMessagesReceived,
			consumerCommitTotal,
			consumerCommitErrors,
			consumerPollLatency,
			consumerRebalanceTotal,
			consumerOffsetGapTotal,
			consumerCompactedOffsetsSkipped,
			consumerStaleWorkers,
			consumerCoordinatorCommands,
			consumerCoordinatorRedirects,
			consumerCoordinatorResolution,
			consumerCoordinatorCacheRefresh,
			consumerCoordinatorRetry,
			consumerCoordinatorResolutionLag,
		)
	})
}

// MetricsHandler returns an http.Handler that serves Prometheus metrics.
// Use this to expose metrics on a dedicated HTTP endpoint (e.g., /metrics).
func MetricsHandler() http.Handler {
	initMetrics()
	return promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{})
}
