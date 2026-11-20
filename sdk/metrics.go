package sdk

import (
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns the SDK collectors registered with a Prometheus registerer and
// exposes the corresponding gatherer. It is safe to share one Metrics value
// among multiple producers and consumers.
type Metrics struct {
	gatherer prometheus.Gatherer

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
}

var (
	metricsOnce     sync.Once
	metricsRegistry *prometheus.Registry
	defaultMetrics  *Metrics

	// These aliases preserve package-internal compatibility for the producer
	// and existing tests. New consumer instrumentation uses Consumer.metrics.
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

// NewMetrics registers the SDK collectors with registerer and returns an
// object that gathers from gatherer. When compatible SDK collectors are
// already registered, NewMetrics reuses them instead of returning a duplicate
// registration error or creating duplicate series.
func NewMetrics(registerer prometheus.Registerer, gatherer prometheus.Gatherer) (*Metrics, error) {
	if registerer == nil {
		return nil, fmt.Errorf("prometheus registerer is required")
	}
	if gatherer == nil {
		return nil, fmt.Errorf("prometheus gatherer is required")
	}

	m := &Metrics{gatherer: gatherer}
	var err error

	if m.producerMessagesSent, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "producer", Name: "messages_sent_total", Help: "Total number of messages successfully sent."},
		[]string{"topic"},
	)); err != nil {
		return nil, err
	}
	if m.producerSendErrors, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "producer", Name: "send_errors_total", Help: "Total number of send errors."},
		[]string{"topic"},
	)); err != nil {
		return nil, err
	}
	if m.producerBatchLatency, err = registerCollector(registerer, prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: "cursus", Subsystem: "producer", Name: "batch_latency_seconds", Help: "Latency of batch send operations in seconds.", Buckets: prometheus.DefBuckets},
		[]string{"topic"},
	)); err != nil {
		return nil, err
	}

	if m.consumerMessagesReceived, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "messages_received_total", Help: "Total number of messages received."},
		[]string{"topic", "group"},
	)); err != nil {
		return nil, err
	}
	if m.consumerCommitTotal, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "commit_total", Help: "Total number of offset commits."},
		[]string{"topic", "group"},
	)); err != nil {
		return nil, err
	}
	if m.consumerCommitErrors, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "commit_errors_total", Help: "Total number of offset commit errors."},
		[]string{"topic", "group"},
	)); err != nil {
		return nil, err
	}
	if m.consumerPollLatency, err = registerCollector(registerer, prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: "cursus", Subsystem: "consumer", Name: "poll_latency_seconds", Help: "Latency of poll operations in seconds.", Buckets: prometheus.DefBuckets},
		[]string{"topic", "group"},
	)); err != nil {
		return nil, err
	}
	if m.consumerRebalanceTotal, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "rebalance_total", Help: "Total number of consumer rebalance events."},
		[]string{"topic", "group"},
	)); err != nil {
		return nil, err
	}
	if m.consumerOffsetGapTotal, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "offset_gap_total", Help: "Total number of offset gaps detected during consumption."},
		[]string{"topic", "group"},
	)); err != nil {
		return nil, err
	}
	if m.consumerCompactedOffsetsSkipped, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "compacted_offsets_skipped_total", Help: "Total number of logical offsets skipped while consuming compacted topics."},
		[]string{"topic", "group"},
	)); err != nil {
		return nil, err
	}
	if m.consumerStaleWorkers, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "stale_workers_total", Help: "Assignment workers fenced after their consumer generation became stale."},
		[]string{"topic", "group", "worker"},
	)); err != nil {
		return nil, err
	}
	if m.consumerCoordinatorCommands, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "coordinator_commands_total", Help: "Completed consumer coordinator command attempts by bounded command, client role, route source, and result."},
		[]string{"command", "client_role", "route_source", "result"},
	)); err != nil {
		return nil, err
	}
	if m.consumerCoordinatorRedirects, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "coordinator_redirects_total", Help: "NOT_COORDINATOR responses received by consumer coordinator commands."},
		[]string{"command", "client_role", "route_source"},
	)); err != nil {
		return nil, err
	}
	if m.consumerCoordinatorResolution, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "coordinator_resolution_total", Help: "Consumer coordinator resolution attempts by bounded result, reason, and client role."},
		[]string{"result", "reason", "client_role"},
	)); err != nil {
		return nil, err
	}
	if m.consumerCoordinatorCacheRefresh, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "coordinator_cache_refresh_total", Help: "Consumer coordinator cache refresh outcomes by bounded result, reason, and client role."},
		[]string{"result", "reason", "client_role"},
	)); err != nil {
		return nil, err
	}
	if m.consumerCoordinatorRetry, err = registerCollector(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "cursus", Subsystem: "consumer", Name: "coordinator_retry_total", Help: "Consumer coordinator command attempts made on a route learned after NOT_COORDINATOR."},
		[]string{"command", "result", "client_role"},
	)); err != nil {
		return nil, err
	}
	if m.consumerCoordinatorResolutionLag, err = registerCollector(registerer, prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: "cursus", Subsystem: "consumer", Name: "coordinator_resolution_duration_seconds", Help: "Time spent resolving or reconnecting a consumer coordinator.", Buckets: prometheus.DefBuckets},
		[]string{"result", "reason", "client_role"},
	)); err != nil {
		return nil, err
	}

	return m, nil
}

func registerCollector[T prometheus.Collector](registerer prometheus.Registerer, collector T) (T, error) {
	err := registerer.Register(collector)
	if err == nil {
		return collector, nil
	}
	var alreadyRegistered prometheus.AlreadyRegisteredError
	if errors.As(err, &alreadyRegistered) {
		if existing, ok := alreadyRegistered.ExistingCollector.(T); ok {
			return existing, nil
		}
	}
	var zero T
	return zero, fmt.Errorf("register Cursus SDK Prometheus collector: %w", err)
}

func initMetrics() {
	metricsOnce.Do(func() {
		metricsRegistry = prometheus.NewRegistry()
		var err error
		defaultMetrics, err = NewMetrics(metricsRegistry, metricsRegistry)
		if err != nil {
			panic(fmt.Sprintf("initialize Cursus SDK metrics: %v", err))
		}

		producerMessagesSent = defaultMetrics.producerMessagesSent
		producerSendErrors = defaultMetrics.producerSendErrors
		producerBatchLatency = defaultMetrics.producerBatchLatency
		consumerMessagesReceived = defaultMetrics.consumerMessagesReceived
		consumerCommitTotal = defaultMetrics.consumerCommitTotal
		consumerCommitErrors = defaultMetrics.consumerCommitErrors
		consumerPollLatency = defaultMetrics.consumerPollLatency
		consumerRebalanceTotal = defaultMetrics.consumerRebalanceTotal
		consumerOffsetGapTotal = defaultMetrics.consumerOffsetGapTotal
		consumerCompactedOffsetsSkipped = defaultMetrics.consumerCompactedOffsetsSkipped
		consumerStaleWorkers = defaultMetrics.consumerStaleWorkers
		consumerCoordinatorCommands = defaultMetrics.consumerCoordinatorCommands
		consumerCoordinatorRedirects = defaultMetrics.consumerCoordinatorRedirects
		consumerCoordinatorResolution = defaultMetrics.consumerCoordinatorResolution
		consumerCoordinatorCacheRefresh = defaultMetrics.consumerCoordinatorCacheRefresh
		consumerCoordinatorRetry = defaultMetrics.consumerCoordinatorRetry
		consumerCoordinatorResolutionLag = defaultMetrics.consumerCoordinatorResolutionLag
	})
}

func defaultSDKMetrics() *Metrics {
	initMetrics()
	return defaultMetrics
}

func (c *Consumer) activeMetrics() *Metrics {
	if c == nil {
		return nil
	}
	if c.metrics != nil {
		return c.metrics
	}
	if c.config != nil && c.config.EnableMetrics {
		return defaultSDKMetrics()
	}
	return nil
}

// Handler returns an http.Handler that serves the gatherer supplied to
// NewMetrics. Application and SDK collectors can therefore share one endpoint.
func (m *Metrics) Handler() http.Handler {
	if m == nil || m.gatherer == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "Cursus SDK metrics are not initialized", http.StatusServiceUnavailable)
		})
	}
	return promhttp.HandlerFor(m.gatherer, promhttp.HandlerOpts{})
}

// MetricsHandler returns an http.Handler that serves the default SDK
// Prometheus registry. Existing EnableMetrics users can continue to expose it
// on a dedicated HTTP endpoint.
func MetricsHandler() http.Handler {
	return defaultSDKMetrics().Handler()
}
