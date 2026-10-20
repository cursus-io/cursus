package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ConsumerSnapshotBusy = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "consumer_snapshot_busy_total",
		Help: "Raft snapshot captures refused because consumer state was busy.",
	})
	ConsumerSnapshotBusyConsecutive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_snapshot_busy_consecutive",
		Help: "Busy consumer snapshot refusals since the last successful FSM snapshot capture; resets on process restart.",
	})
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
