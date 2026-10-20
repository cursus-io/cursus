package observability

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/observation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

type delayedDisk struct {
	block   atomic.Bool
	fail    atomic.Bool
	calls   atomic.Int32
	release chan struct{}
}

func (d *delayedDisk) RuntimeSnapshot() disk.RuntimeSnapshot {
	d.calls.Add(1)
	if d.block.Load() {
		<-d.release
	}
	if d.fail.Load() {
		return disk.RuntimeSnapshot{StatFailures: 1}
	}
	return disk.RuntimeSnapshot{Bytes: 4096, FilesystemFreeBytes: 8192, FilesystemTotalBytes: 16384, FilesystemHeadroomOK: true}
}

func TestMetricsHTTPRemainsResponsiveDuringBlockedObservation(t *testing.T) {
	d := &delayedDisk{release: make(chan struct{})}
	c := NewCollector(nil, nil, d, nil, nil, fixedReadiness(false))
	c.worker = observation.New[[]prometheus.Metric](10*time.Millisecond, 50*time.Millisecond, 100*time.Millisecond)
	t.Cleanup(c.Close)
	registry := prometheus.NewRegistry()
	registry.MustRegister(c)
	server := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: time.Second}
	scrape := func() string {
		response, err := client.Get(server.URL)
		require.NoError(t, err)
		defer func() { _ = response.Body.Close() }()
		body, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		return string(body)
	}
	initial := scrape()
	require.NotContains(t, initial, "cursus_storage_bytes")
	require.NotContains(t, initial, "cursus_observation_last_completion_timestamp_seconds")
	require.Contains(t, initial, `cursus_observation_valid{component="runtime"} 0`)
	c.Start()
	require.Eventually(t, func() bool { return c.worker.Snapshot().Valid() }, time.Second, time.Millisecond)
	require.Contains(t, scrape(), "cursus_storage_bytes 4096")
	d.block.Store(true)
	require.Eventually(t, func() bool { return c.worker.Snapshot().TimedOut }, time.Second, time.Millisecond)
	calls := d.calls.Load()
	for i := 0; i < 40; i++ {
		body := scrape()
		require.Contains(t, body, `cursus_observation_in_progress{component="runtime"} 1`)
		require.Contains(t, body, `cursus_observation_timed_out{component="runtime"} 1`)
		require.NotContains(t, body, "cursus_storage_bytes")
	}
	require.Equal(t, calls, d.calls.Load(), "scrapes and deadlines must not enqueue work")
	d.block.Store(false)
	close(d.release)
	require.Eventually(t, func() bool { return c.worker.Snapshot().Valid() }, time.Second, time.Millisecond)
	require.Contains(t, scrape(), "cursus_storage_bytes 4096")
	d.fail.Store(true)
	require.Eventually(t, func() bool { return c.worker.Snapshot().Error != "" }, time.Second, time.Millisecond)
	body := scrape()
	require.NotContains(t, body, "cursus_storage_filesystem_free_bytes")
	require.Contains(t, body, `cursus_observation_last_failed{component="runtime"} 1`)
	d.fail.Store(false)
	require.Eventually(t, func() bool { return c.worker.Snapshot().Valid() }, time.Second, time.Millisecond)
}

func TestMetricsOmitsExpiredSnapshot(t *testing.T) {
	c := NewCollector(nil, nil, fixedDisk{snapshot: disk.RuntimeSnapshot{Bytes: 42}}, nil, nil, nil)
	c.worker = observation.New[[]prometheus.Metric](time.Second, time.Second, 30*time.Millisecond)
	c.Start()
	t.Cleanup(c.Close)
	require.Eventually(t, func() bool { return c.worker.Snapshot().Valid() }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return c.worker.Snapshot().Stale }, time.Second, time.Millisecond)
	registry := prometheus.NewRegistry()
	registry.MustRegister(c)
	families, err := registry.Gather()
	require.NoError(t, err)
	assertMetricFamilyMissing(t, families, "cursus_storage_bytes")
	assertGauge(t, families, "cursus_observation_stale", map[string]string{"component": "runtime"}, 1)
}
