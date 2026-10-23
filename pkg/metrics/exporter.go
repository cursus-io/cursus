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
	prometheus.MustRegister(recoveryWork, recoveryAttempts, RetentionHolds)
	prometheus.MustRegister(MessagesProcessed, MessagesPerSec, LatencyHist, QueueSize, CleanupCount, SeqNumGapTotal, SeqNumDuplicateTotal)
	prometheus.MustRegister(ClientConnectionsTotal, ClientConnectionsActive, ClientResponseWriteFailures, RequestsInflight, RequestBytesInflight, RequestAdmissionWaiters, RequestAdmissionRejections, CommandsTotal, CommandDuration, CommandErrors, PublishAcknowledgements, AsyncReplicationFailures, ReplicationRetries, LogCompactionRuns)
	prometheus.MustRegister(ClusterReplicationLag)
	prometheus.MustRegister(ConsumerCleanupReplayRecords, ConsumerCleanupReplayBytes, ConsumerCleanupReplayReads, ConsumerCleanupReplayInvalidations)
	prometheus.MustRegister(ConsumerSnapshotBusy, ConsumerSnapshotBusyConsecutive)
}

// Handler returns an isolated metrics handler containing process and runtime collectors.
// Runtime collectors must read memory snapshots only. The caller must Close the
// returned handler when using it outside StartMetricsServerAddress.
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
	process := newProcessSnapshot(prometheus.DefaultGatherer)
	// The status descriptors are shared with the runtime collector. Register
	// them separately, then merge families with distinct component labels.
	statusRegistry := prometheus.NewRegistry()
	if err := statusRegistry.Register(process); err != nil {
		process.worker.Close()
		return nil, err
	}
	gatherer := prometheus.Gatherers{process, runtimeRegistry, statusRegistry}
	return &snapshotHandler{Handler: promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{EnableOpenMetrics: true}), process: process}, nil
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
	closeHandler := func() { _ = handler.(*snapshotHandler).Close() }
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
	server.RegisterOnShutdown(closeHandler)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		closeHandler()
		return nil, fmt.Errorf("listen for metrics on %s: %w", addr, err)
	}
	server.Addr = listener.Addr().String()
	go func() {
		defer closeHandler()
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
