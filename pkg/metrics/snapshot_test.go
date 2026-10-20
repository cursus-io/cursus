package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/observation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestProcessGatherDoesNotAccumulateOnBlockedCollector(t *testing.T) {
	s := &processSnapshot{worker: observation.New[[]*dto.MetricFamily](time.Millisecond, 20*time.Millisecond, time.Second)}
	t.Cleanup(s.worker.Close)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	s.worker.Start(func(context.Context) ([]*dto.MetricFamily, error) {
		calls.Add(1)
		close(entered)
		<-release
		return nil, nil
	})
	<-entered
	registry := prometheus.NewRegistry()
	registry.MustRegister(s, fixedCollector{desc: prometheus.NewDesc("runtime_still_available", "test", nil, nil)})
	handler := promhttp.HandlerFor(prometheus.Gatherers{s, registry}, promhttp.HandlerOpts{})
	require.Eventually(t, func() bool { return s.worker.Snapshot().TimedOut }, time.Second, time.Millisecond)
	for i := 0; i < 100; i++ {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		require.Equal(t, http.StatusOK, response.Code)
		require.Contains(t, response.Body.String(), "runtime_still_available 7")
		require.Contains(t, response.Body.String(), `cursus_observation_timed_out{component="process"} 1`)
	}
	require.EqualValues(t, 1, calls.Load())
	s.worker.Close()
	close(release)
	select {
	case <-s.worker.Done():
	case <-time.After(time.Second):
		t.Fatal("process worker did not stop")
	}
}
