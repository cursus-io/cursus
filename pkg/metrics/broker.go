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

	ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "broker_consumer_lag",
		Help: "Consumer lag per partition (LEO - committed offset)",
	}, []string{"topic", "partition", "group"})
)
