package observability

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

// Existing mapping/label tests exercise one deterministic sampling pass. The
// separate snapshot tests exercise the public asynchronous Collect contract.
type sampleCollector struct{ *Collector }

func newSampleCollector(topics topicSource, groups groupSource, disk diskSource, streams streamSource, cluster clusterSource, readiness ReadinessSource, transactions ...transactionSource) *sampleCollector {
	return &sampleCollector{NewCollector(topics, groups, disk, streams, cluster, readiness, transactions...)}
}

func (c *sampleCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- gauge(c.ready, boolValue(c.readiness != nil && c.readiness.IsReady()))
	metrics, err := c.sample(context.Background())
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.ready, err)
		return
	}
	for _, m := range metrics {
		ch <- m
	}
}
