package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ClusterReplicationLag = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "cluster_replication_lag_seconds",
			Help: "Replication lag across cluster",
		},
		[]string{"topic", "partition", "broker"},
	)
)
