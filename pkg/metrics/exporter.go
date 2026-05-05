package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	prometheus.MustRegister(MessagesProcessed, MessagesPerSec, LatencyHist, QueueSize, CleanupCount, SeqNumGapTotal, SeqNumDuplicateTotal, ConsumerLag)
	prometheus.MustRegister(ClusterBrokersTotal, PartitionLeadersTotal, ClusterReplicationLag, LeaderElectionTotal, ISRSize)
	prometheus.MustRegister(ReplicationLagBytes, ISRChangesTotal, LeaderElectionFailures, BrokerHealthStatus, QuorumOperations, PartitionReassignments)
}

func StartMetricsServer(port int) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		addr := fmt.Sprintf(":%d", port)
		fmt.Println("[METRICS] Prometheus exporter listening on", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Printf("[METRICS] Failed to start metrics server: %v\n", err)
		}
	}()
}

// PushMetric updates Prometheus metrics for each processed message.
func PushMetric(topic string, elapsedSeconds float64) {
	MessagesProcessed.Inc()
	LatencyHist.Observe(elapsedSeconds)
	if elapsedSeconds > 0 {
		MessagesPerSec.Set(1.0 / elapsedSeconds)
	}
}
