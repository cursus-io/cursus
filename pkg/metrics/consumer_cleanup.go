package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ConsumerCleanupReplayReads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_cleanup_replay_reads_total",
		Help: "Bounded physical segment reads used to reconcile committed consumer group cleanup.",
	})
	ConsumerCleanupReplayRecords = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_cleanup_replay_records_total",
		Help: "Storage records read while reconciling committed consumer group cleanup, including filtered records.",
	})
	ConsumerCleanupReplayBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_cleanup_replay_bytes_total",
		Help: "Serialized storage bytes read while reconciling committed consumer group cleanup.",
	})
	ConsumerCleanupReplayInvalidations = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_cleanup_replay_invalidations_total",
		Help: "Consumer cleanup scans initialized or invalidated by a changed metadata log view.",
	})
)
