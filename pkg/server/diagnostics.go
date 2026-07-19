package server

import (
	"context"
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/observability"
	"github.com/cursus-io/cursus/pkg/topic"
)

func addStorageReadinessChecks(state *HealthState, tm *topic.TopicManager, dm *disk.DiskManager) {
	state.AddCheck("storage", func(context.Context) error {
		if tm == nil || dm == nil {
			return fmt.Errorf("storage subsystem unavailable")
		}
		return dm.Ready()
	})
	state.AddCheck("topic_metadata", func(context.Context) error {
		if tm == nil {
			return fmt.Errorf("topic metadata state unavailable")
		}
		return tm.MetadataReadinessError()
	})
}

// RunTopicMetadataDiagnostics serves liveness, readiness, and metrics without
// opening broker client, internal, discovery, or Raft listeners.
func RunTopicMetadataDiagnostics(ctx context.Context, cfg *config.Config, tm *topic.TopicManager, dm *disk.DiskManager) error {
	if ctx == nil {
		return fmt.Errorf("diagnostics context must not be nil")
	}
	if cfg == nil {
		return fmt.Errorf("diagnostics config must not be nil")
	}

	state := NewHealthState()
	addStorageReadinessChecks(state, tm, dm)
	state.SetReady(true)
	collector := observability.NewCollector(tm, nil, dm, nil, nil, state)

	if cfg.EnableExporter {
		metricsServer, err := metrics.StartMetricsServer(cfg.ExporterPort, collector)
		if err != nil {
			return fmt.Errorf("start diagnostics metrics exporter: %w", err)
		}
		defer shutdownHTTPServer(metricsServer)
	}

	healthPort := cfg.HealthCheckPort
	if healthPort == 0 {
		healthPort = DefaultHealthCheckPort
	}
	healthServer, err := startHealthCheckServer(healthPort, state)
	if err != nil {
		return fmt.Errorf("start diagnostics health server: %w", err)
	}
	defer shutdownHTTPServer(healthServer)

	<-ctx.Done()
	state.SetReady(false)
	return nil
}
