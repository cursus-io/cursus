package metrics

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	prometheus.MustRegister(MessagesProcessed, MessagesPerSec, LatencyHist, QueueSize, CleanupCount, SeqNumGapTotal, SeqNumDuplicateTotal)
	prometheus.MustRegister(ClientConnectionsTotal, ClientConnectionsActive, CommandsTotal, CommandDuration, CommandErrors)
	prometheus.MustRegister(ClusterBrokersTotal, PartitionLeadersTotal, ClusterReplicationLag, LeaderElectionTotal, ISRSize)
	prometheus.MustRegister(ReplicationLagBytes, ISRChangesTotal, LeaderElectionFailures, BrokerHealthStatus, QuorumOperations, PartitionReassignments)
}

// Handler returns an isolated metrics handler containing process and runtime collectors.
func Handler(collectors ...prometheus.Collector) (http.Handler, error) {
	runtimeRegistry := prometheus.NewRegistry()
	for _, collector := range collectors {
		if collector == nil {
			continue
		}
		if err := runtimeRegistry.Register(collector); err != nil {
			return nil, fmt.Errorf("register runtime collector: %w", err)
		}
	}
	gatherer := prometheus.Gatherers{prometheus.DefaultGatherer, runtimeRegistry}
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{EnableOpenMetrics: true}), nil
}

// StartMetricsServer starts the exporter and reports bind failures synchronously.
func StartMetricsServer(port int, collectors ...prometheus.Collector) (*http.Server, error) {
	return StartMetricsServerAddress(fmt.Sprintf(":%d", port), collectors...)
}

// StartMetricsServerAddress starts the exporter on an explicit listen address.
func StartMetricsServerAddress(addr string, collectors ...prometheus.Collector) (*http.Server, error) {
	handler, err := Handler(collectors...)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for metrics on %s: %w", addr, err)
	}
	server.Addr = listener.Addr().String()
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			fmt.Printf("[METRICS] exporter stopped: %v\n", serveErr)
		}
	}()
	fmt.Println("[METRICS] Prometheus exporter listening on", listener.Addr())
	return server, nil
}

// PushMetric updates Prometheus metrics for each processed message.
func PushMetric(topic string, elapsedSeconds float64) {
	MessagesProcessed.Inc()
	LatencyHist.Observe(elapsedSeconds)
	if elapsedSeconds > 0 {
		MessagesPerSec.Set(1.0 / elapsedSeconds)
	}
}
