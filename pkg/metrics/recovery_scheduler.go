package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var RecoveryQueue = prometheus.NewGauge(prometheus.GaugeOpts{Name: "cursus_recovery_queue_entries", Help: "Admitted continuations, including ISR watermark maintenance; bounded by recovery.queue_entries."})
var RecoveryRunning = prometheus.NewGauge(prometheus.GaugeOpts{Name: "cursus_recovery_running_turns", Help: "Owned recovery turns, including canceled work until it exits."})
var RecoveryDeferred = prometheus.NewCounter(prometheus.CounterOpts{Name: "cursus_recovery_admission_deferred_total", Help: "Nonblocking recovery dispatches deferred by byte, slot or source limits."})
var RecoveryFirstService = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "cursus_recovery_first_service_seconds", Help: "Time from admission to first scheduled turn.", Buckets: prometheus.ExponentialBuckets(.001, 4, 10)})
var RecoveryResidence = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "cursus_recovery_residence_seconds", Help: "Time from admission to successful recovery completion; compare with under-replicated gauges when tuning quotas.", Buckets: prometheus.ExponentialBuckets(.01, 4, 12)})
var RecoveryOldest = prometheus.NewGauge(prometheus.GaugeOpts{Name: "cursus_recovery_oldest_pending_seconds", Help: "Age of oldest admitted continuation, including ISR maintenance, quota and retry waits."})
var RecoveryTurnDuration = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "cursus_recovery_turn_seconds", Help: "Owned recovery turn duration, including I/O and apply.", Buckets: prometheus.ExponentialBuckets(.001, 4, 9)})

func init() {
	prometheus.MustRegister(RecoveryQueue, RecoveryRunning, RecoveryDeferred, RecoveryFirstService, RecoveryResidence, RecoveryOldest, RecoveryTurnDuration)
}

func ObserveRecoveryTurn(start time.Time) { RecoveryTurnDuration.Observe(time.Since(start).Seconds()) }
