package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ConsumerHistoryPhaseSeconds     = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "cursus_consumer_history_phase_seconds", Help: "Consumer history capture, certification, and publication wall time; publication includes exclusive admission", Buckets: prometheus.ExponentialBuckets(0.001, 4, 10)}, []string{"phase"})
	ConsumerHistoryRuns             = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "cursus_consumer_history_runs_total", Help: "Consumer history maintenance outcomes"}, []string{"result"})
	ConsumerHistoryReadRecords      = prometheus.NewCounter(prometheus.CounterOpts{Name: "cursus_consumer_history_source_records_total", Help: "Decoded certificate source records, including repeated reads"})
	ConsumerHistoryReadPayloadBytes = prometheus.NewCounter(prometheus.CounterOpts{Name: "cursus_consumer_history_source_payload_bytes_total", Help: "Decoded key and payload bytes read by all certificate attempts; not physical I/O"})
	ConsumerHistoryRemovedRecords   = prometheus.NewCounter(prometheus.CounterOpts{Name: "cursus_consumer_history_removed_records_total", Help: "Records removed by certified publication"})
	ConsumerHistoryRemovedBytes     = prometheus.NewCounter(prometheus.CounterOpts{Name: "cursus_consumer_history_removed_frame_bytes_total", Help: "Original minus retained log frame bytes after certified publication; not physical I/O"})
)
