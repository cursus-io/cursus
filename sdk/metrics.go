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

	consumerMessagesReceived *prometheus.CounterVec
	consumerCommitTotal      *prometheus.CounterVec
	consumerCommitErrors     *prometheus.CounterVec
	consumerPollLatency      *prometheus.HistogramVec
	consumerRebalanceTotal   *prometheus.CounterVec
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

		metricsRegistry.MustRegister(
			producerMessagesSent,
			producerSendErrors,
			producerBatchLatency,
			consumerMessagesReceived,
			consumerCommitTotal,
			consumerCommitErrors,
			consumerPollLatency,
			consumerRebalanceTotal,
		)
	})
}

// MetricsHandler returns an http.Handler that serves Prometheus metrics.
// Use this to expose metrics on a dedicated HTTP endpoint (e.g., /metrics).
func MetricsHandler() http.Handler {
	initMetrics()
	return promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{})
}
