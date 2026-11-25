package metrics

import (
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type leaderRebalanceMetrics struct {
	imbalanced  prometheus.Gauge
	eligible    prometheus.Gauge
	moves       *prometheus.CounterVec
	duration    prometheus.Histogram
	lastSuccess prometheus.Gauge
	broker      *prometheus.GaugeVec
}

func newLeaderRebalanceMetrics() *leaderRebalanceMetrics {
	return &leaderRebalanceMetrics{
		imbalanced: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cursus_leader_rebalance_imbalanced_partitions",
			Help: "Partitions whose current leader differs from the first replica in the durable assignment.",
		}),
		eligible: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cursus_leader_rebalance_eligible_partitions",
			Help: "Imbalanced partitions currently safe for preferred leader election.",
		}),
		moves: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cursus_leader_rebalance_moves_total",
			Help: "Preferred leader election attempts by bounded result and reason.",
		}, []string{"result", "reason"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cursus_leader_rebalance_cycle_duration_seconds",
			Help:    "Preferred leader rebalance cycle duration.",
			Buckets: prometheus.DefBuckets,
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cursus_leader_rebalance_last_success_timestamp",
			Help: "Unix timestamp of the last successfully applied preferred leader move.",
		}),
		broker: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cursus_leader_rebalance_broker_leaders",
			Help: "Current and preferred partition leader counts for durable broker IDs.",
		}, []string{"broker_id", "kind"}),
	}
}

func (metrics *leaderRebalanceMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{metrics.imbalanced, metrics.eligible, metrics.moves, metrics.duration, metrics.lastSuccess, metrics.broker}
}

func (metrics *leaderRebalanceMetrics) setPlan(imbalanced, eligible int, current, preferred map[string]int) {
	metrics.imbalanced.Set(float64(imbalanced))
	metrics.eligible.Set(float64(eligible))
	metrics.broker.Reset()
	ids := make(map[string]struct{}, len(current)+len(preferred))
	for id := range current {
		ids[id] = struct{}{}
	}
	for id := range preferred {
		ids[id] = struct{}{}
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	for _, id := range ordered {
		metrics.broker.WithLabelValues(id, "current").Set(float64(current[id]))
		metrics.broker.WithLabelValues(id, "preferred").Set(float64(preferred[id]))
	}
}

func (metrics *leaderRebalanceMetrics) clearPlan() {
	metrics.setPlan(0, 0, nil, nil)
}

func (metrics *leaderRebalanceMetrics) recordMove(result, reason string) {
	if result != "success" && result != "failure" {
		result = "failure"
	}
	switch reason {
	case "applied", "rejected", "canceled", "not_leader":
	default:
		reason = "rejected"
	}
	metrics.moves.WithLabelValues(result, reason).Inc()
}

func (metrics *leaderRebalanceMetrics) observeCycle(duration time.Duration, successfulMoves int) {
	metrics.duration.Observe(duration.Seconds())
	if successfulMoves > 0 {
		metrics.lastSuccess.SetToCurrentTime()
	}
}

var preferredLeaderRebalanceMetrics = newLeaderRebalanceMetrics()

func SetLeaderRebalancePlan(imbalanced, eligible int, current, preferred map[string]int) {
	preferredLeaderRebalanceMetrics.setPlan(imbalanced, eligible, current, preferred)
}

func ClearLeaderRebalancePlan() { preferredLeaderRebalanceMetrics.clearPlan() }

func RecordLeaderRebalanceMove(result, reason string) {
	preferredLeaderRebalanceMetrics.recordMove(result, reason)
}

func ObserveLeaderRebalanceCycle(duration time.Duration, successfulMoves int) {
	preferredLeaderRebalanceMetrics.observeCycle(duration, successfulMoves)
}
