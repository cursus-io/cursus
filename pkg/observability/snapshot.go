package observability

import (
	"github.com/cursus-io/cursus/pkg/observation"
	"github.com/prometheus/client_golang/prometheus"
)

// Start begins one sequential collection worker. The owner must call Close.
// Construction, registration and scraping perform no runtime-source queries.
func (c *Collector) Start() { c.worker.Start(c.sample) }
func (c *Collector) Close() { c.worker.Close() }

// Collect only reads immutable observations. ReadinessSource must be memory-only;
// the broker HealthState satisfies this contract even when its checks are blocked.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ready := c.readiness != nil && c.readiness.IsReady()
	ch <- gauge(c.ready, boolValue(ready))
	if source, ok := c.readiness.(interface{ ObservationStatus() observation.Status }); ok {
		observation.CollectMetrics(ch, "readiness", source.ObservationStatus())
	}
	snapshot := c.worker.Snapshot()
	observation.CollectMetrics(ch, "runtime", snapshot.Status)
	if !snapshot.Valid() {
		return
	}
	for _, metric := range snapshot.Value {
		ch <- metric
	}
}
