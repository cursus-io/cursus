package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	MessagesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "broker_messages_processed_total",
		Help: "Total number of messages processed by the broker",
	})

	MessagesPerSec = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_messages_per_second",
		Help: "Current throughput in messages per second",
	})

	LatencyHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "broker_message_latency_seconds",
		Help:    "Histogram of message latency during processing",
		Buckets: prometheus.DefBuckets,
	})

	QueueSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_queue_size",
		Help: "Current queue size in the topic manager",
	})

	CleanupCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "broker_cleanup_count_total",
		Help: "Total number of deduped message IDs cleaned up from memory",
	})

	SeqNumGapTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "broker_seqnum_gap_total",
		Help: "Total number of sequence number gaps detected per producer",
	}, []string{"topic", "partition", "producer_id"})

	SeqNumDuplicateTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "broker_seqnum_duplicate_total",
		Help: "Total number of duplicate sequence numbers detected",
	}, []string{"topic", "partition"})

	// ConsumerLag is retained for Go API compatibility. The broker exporter
	// serves this name from its scrape-time runtime collector.
	// Deprecated: use cursus_consumer_group_lag from the broker exporter.
	ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "broker_consumer_lag",
		Help: "Deprecated consumer lag compatibility gauge",
	}, []string{"topic", "partition", "group"})

	ClientConnectionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cursus_broker_client_connections_total",
		Help: "Total client TCP connections accepted by the broker",
	})

	ClientConnectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cursus_broker_client_connections_active",
		Help: "Client TCP connections currently handled by the broker",
	})

	CommandsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cursus_broker_commands_total",
		Help: "Broker text commands completed by command and result",
	}, []string{"command", "result"})

	CommandDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cursus_broker_command_duration_seconds",
		Help:    "Broker text command handling latency before streamed payload transfer",
		Buckets: prometheus.DefBuckets,
	}, []string{"command"})

	CommandErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cursus_broker_command_errors_total",
		Help: "Broker text command errors by bounded command and wire error code",
	}, []string{"command", "code"})
)
