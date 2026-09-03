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

	PublishAcknowledgements = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cursus_broker_publish_acknowledgements_total",
		Help: "Publish requests by normalized acknowledgement mode and result",
	}, []string{"ack_mode", "result"})

	AsyncReplicationFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cursus_broker_async_replication_failures_total",
		Help: "Asynchronous partition replication failures by topic and bounded error class",
	}, []string{"topic", "error_class"})

	LogCompactionRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cursus_broker_log_compaction_runs_total",
		Help: "Log compaction passes by bounded result and reason",
	}, []string{"result", "reason"})
)
