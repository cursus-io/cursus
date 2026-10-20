package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/observability"
	"github.com/cursus-io/cursus/pkg/observation"
	"github.com/stretchr/testify/require"
)

func TestHealthHTTPBoundedAcrossStallFailureAndRecovery(t *testing.T) {
	state := NewHealthState()
	state.worker = observation.New[healthResult](10*time.Millisecond, 50*time.Millisecond, 100*time.Millisecond)
	t.Cleanup(state.Close)
	var calls atomic.Int32
	var blocked, failed atomic.Bool
	release := make(chan struct{})
	state.AddCheck("storage", func(context.Context) error {
		calls.Add(1)
		if blocked.Load() {
			<-release
		}
		if failed.Load() {
			return errors.New("filesystem unavailable")
		}
		return nil
	})
	state.SetReady(true)
	server := httptest.NewServer(newHealthHandler(state))
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: time.Second}
	get := func(path string, want int) {
		t.Helper()
		resp, err := client.Get(server.URL + path)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		_, err = io.Copy(io.Discard, resp.Body)
		require.NoError(t, err)
		require.Equal(t, want, resp.StatusCode)
	}
	get("/live", 200)
	get("/ready", 503) // No completed observation yet.
	state.Start()
	require.Eventually(t, state.IsReady, time.Second, time.Millisecond)
	get("/ready", 200)
	blocked.Store(true)
	require.Eventually(t, func() bool { return state.ObservationStatus().TimedOut }, time.Second, time.Millisecond)
	before := calls.Load()
	// Real concurrent HTTP requests must complete while storage stays blocked.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				get("/live", 200)
				get("/ready", 503)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, before, calls.Load())
	require.Eventually(t, func() bool { return state.ObservationStatus().Stale }, time.Second, time.Millisecond)
	get("/ready", 503)
	blocked.Store(false)
	close(release)
	require.Eventually(t, state.IsReady, time.Second, time.Millisecond)
	require.GreaterOrEqual(t, state.ObservationStatus().Failures, uint64(1))
	failed.Store(true)
	require.Eventually(t, func() bool { return state.ObservationStatus().Error != "" }, time.Second, time.Millisecond)
	get("/live", 200)
	get("/ready", 503)
	failed.Store(false)
	require.Eventually(t, state.IsReady, time.Second, time.Millisecond)
	get("/ready", 200)
	state.Close()
	get("/live", 200)
	get("/ready", 503)
}

type blockedStorageObservation struct {
	release chan struct{}
	calls   atomic.Int32
}

func (s *blockedStorageObservation) check(context.Context) error {
	s.calls.Add(1)
	<-s.release
	return nil
}

func (s *blockedStorageObservation) RuntimeSnapshot() disk.RuntimeSnapshot {
	_ = s.check(context.Background())
	return disk.RuntimeSnapshot{Bytes: 42}
}

func TestAllDiagnosticEndpointsRespondWhileStorageIsBlocked(t *testing.T) {
	storage := &blockedStorageObservation{release: make(chan struct{})}
	defer close(storage.release)
	state := NewHealthState()
	state.AddCheck("storage", storage.check)
	state.SetReady(true)
	state.Start()
	defer state.Close()
	collector := observability.NewCollector(nil, nil, storage, nil, nil, state)
	collector.Start()
	defer collector.Close()
	metricsHandler, err := metrics.Handler(collector)
	require.NoError(t, err)
	defer func() { require.NoError(t, metricsHandler.(io.Closer).Close()) }()
	mux := http.NewServeMux()
	mux.Handle("/", newHealthHandler(state))
	mux.Handle("/metrics", metricsHandler)
	server := httptest.NewServer(mux)
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool { return storage.calls.Load() == 2 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return state.ObservationStatus().TimedOut }, 3*time.Second, 10*time.Millisecond)
	for i := 0; i < 20; i++ {
		for path, want := range map[string]int{"/live": 200, "/ready": 503, "/metrics": 200} {
			response, err := client.Get(server.URL + path)
			require.NoError(t, err)
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, want, response.StatusCode)
			if path == "/metrics" {
				require.Contains(t, string(body), `cursus_observation_in_progress{component="runtime"} 1`)
				require.Contains(t, string(body), `cursus_observation_in_progress{component="readiness"} 1`)
				require.NotContains(t, string(body), "cursus_storage_bytes")
			}
		}
	}
	require.EqualValues(t, 2, storage.calls.Load())
}

func TestHealthExpiresSuccessfulSnapshotAndInvalidatesNewChecks(t *testing.T) {
	state := NewHealthState()
	state.worker = observation.New[healthResult](time.Second, time.Second, 40*time.Millisecond)
	t.Cleanup(state.Close)
	state.SetReady(true)
	state.Start()
	require.Eventually(t, state.IsReady, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return state.ObservationStatus().Stale }, time.Second, time.Millisecond)
	require.False(t, state.IsReady())
	state.AddCheck("storage", func(context.Context) error { return errors.New("unavailable") })
	ready, checks := state.report()
	require.False(t, ready)
	require.Equal(t, "not yet checked", checks["observation"])
}
