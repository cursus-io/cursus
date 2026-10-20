package metrics

import (
	"context"
	"net/http"

	"github.com/cursus-io/cursus/pkg/observation"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// processSnapshot also isolates the default Go/process collectors (including
// procfs reads on Linux). One outstanding Gather is allowed; its registry has a
// fixed set of collector goroutines, never a fresh set per timed-out request.
type processSnapshot struct {
	worker *observation.Worker[[]*dto.MetricFamily]
}

func newProcessSnapshot(source prometheus.Gatherer) *processSnapshot {
	s := &processSnapshot{worker: observation.New[[]*dto.MetricFamily](observation.Interval, observation.Timeout, observation.MaxAge)}
	s.worker.Start(func(context.Context) ([]*dto.MetricFamily, error) { return source.Gather() })
	return s
}

func (s *processSnapshot) Gather() ([]*dto.MetricFamily, error) {
	snapshot := s.worker.Snapshot()
	if !snapshot.Valid() {
		return nil, nil
	}
	return snapshot.Value, nil
}

func (s *processSnapshot) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range observation.Descriptors {
		ch <- d
	}
}

func (s *processSnapshot) Collect(ch chan<- prometheus.Metric) {
	observation.CollectMetrics(ch, "process", s.worker.Snapshot().Status)
}

type snapshotHandler struct {
	http.Handler
	process *processSnapshot
}

// Close is non-blocking even when a default collector cannot be cancelled.
func (h *snapshotHandler) Close() error { h.process.worker.Close(); return nil }
