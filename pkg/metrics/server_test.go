package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type fixedCollector struct {
	desc *prometheus.Desc
}

func (collector fixedCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- collector.desc
}

func (collector fixedCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(collector.desc, prometheus.GaugeValue, 7)
}

func TestStartMetricsServerServesRuntimeCollector(t *testing.T) {
	collector := fixedCollector{desc: prometheus.NewDesc("cursus_test_runtime_value", "test", nil, nil)}
	server, err := StartMetricsServerAddress("127.0.0.1:0", collector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	response, err := http.Get("http://" + server.Addr + "/metrics") // #nosec G107 -- test server address is loopback-only.
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d", response.StatusCode)
	}
	if !strings.Contains(string(body), "cursus_test_runtime_value 7") {
		t.Fatalf("runtime metric missing from response: %s", body)
	}
}

func TestStartMetricsServerReportsBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	server, err := StartMetricsServerAddress(listener.Addr().String())
	if err == nil || server != nil {
		t.Fatalf("StartMetricsServerAddress() = (%v, %v), want bind error", server, err)
	}
}
