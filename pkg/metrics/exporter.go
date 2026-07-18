package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsServer owns the listener used by a Prometheus exporter. It keeps
// each exporter isolated from http.DefaultServeMux.
type MetricsServer struct {
	server   *http.Server
	listener net.Listener
	done     chan struct{}
}

func init() {
	prometheus.MustRegister(MessagesProcessed, MessagesPerSec, LatencyHist, QueueSize, CleanupCount, SeqNumGapTotal, SeqNumDuplicateTotal, ConsumerLag)
	prometheus.MustRegister(ClusterBrokersTotal, PartitionLeadersTotal, ClusterReplicationLag, LeaderElectionTotal, ISRSize)
	prometheus.MustRegister(ReplicationLagBytes, ISRChangesTotal, LeaderElectionFailures, BrokerHealthStatus, QuorumOperations, PartitionReassignments)
}

// StartMetricsServer starts an exporter on port and returns an independently
// owned server. Listener creation is synchronous so callers receive bind
// errors instead of discovering them only in a background goroutine.
func StartMetricsServer(port int) (*MetricsServer, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen for metrics exporter: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{Handler: mux}
	metricsServer := &MetricsServer{
		server:   server,
		listener: listener,
		done:     make(chan struct{}),
	}

	go func() {
		defer close(metricsServer.done)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("[METRICS] Metrics server stopped unexpectedly: %v\n", err)
		}
	}()

	fmt.Println("[METRICS] Prometheus exporter listening on", listener.Addr())
	return metricsServer, nil
}

// Addr returns the address selected for the exporter, including a dynamically
// assigned port when StartMetricsServer receives port 0.
func (s *MetricsServer) Addr() net.Addr {
	return s.listener.Addr()
}

// Shutdown gracefully stops the exporter and waits for its serving goroutine
// to exit. It is safe to call after Close.
func (s *MetricsServer) Shutdown(ctx context.Context) error {
	err := s.server.Shutdown(ctx)
	<-s.done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Close immediately stops the exporter and waits for the listener to be
// released.
func (s *MetricsServer) Close() error {
	err := s.server.Close()
	<-s.done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// PushMetric updates Prometheus metrics for each processed message.
func PushMetric(topic string, elapsedSeconds float64) {
	MessagesProcessed.Inc()
	LatencyHist.Observe(elapsedSeconds)
	if elapsedSeconds > 0 {
		MessagesPerSec.Set(1.0 / elapsedSeconds)
	}
}
