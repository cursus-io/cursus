package metrics_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/metrics"
)

func TestStartMetricsServer_IndependentInstancesAndShutdown(t *testing.T) {
	first, err := metrics.StartMetricsServer(0)
	if err != nil {
		t.Fatalf("start first exporter: %v", err)
	}
	second, err := metrics.StartMetricsServer(0)
	if err != nil {
		_ = first.Close()
		t.Fatalf("start second exporter: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	assertMetricsEndpoint(t, first)
	assertMetricsEndpoint(t, second)

	addr := first.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := first.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown first exporter: %v", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("exporter listener was not released: %v", err)
	}
	_ = listener.Close()
}

func TestStartMetricsServer_ReturnsListenError(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("reserve exporter port: %v", err)
	}
	defer func() { _ = listener.Close() }()

	port := listener.Addr().(*net.TCPAddr).Port
	server, err := metrics.StartMetricsServer(port)
	if err == nil {
		_ = server.Close()
		t.Fatal("expected exporter listener error")
	}
}

func assertMetricsEndpoint(t *testing.T, server *metrics.MetricsServer) {
	t.Helper()
	_, port, err := net.SplitHostPort(server.Addr().String())
	if err != nil {
		t.Fatalf("split exporter address: %v", err)
	}
	url := fmt.Sprintf("http://127.0.0.1:%s/metrics", port)
	deadline := time.Now().Add(2 * time.Second)
	for {
		response, err := http.Get(url)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("metrics endpoint status = %d, want %d", response.StatusCode, http.StatusOK)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics endpoint never became reachable: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
