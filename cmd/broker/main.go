package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/server"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

var runServerContext = server.RunServerContext
var runTopicMetadataDiagnostics = server.RunTopicMetadataDiagnostics

func main() {
	// Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		util.Fatal("❌ Failed to load config: %v", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		util.Error("Failed to marshal config: %v", err)
	} else {
		util.Info("Configuration:\n%s", string(data))
	}

	fmt.Print(`
                         _______  ______________  _______
                        / ___/ / / / ___/ ___/ / / / ___/
                       / /__/ /_/ / /  (__  ) /_/ (__  )
                       \___/\__,_/_/  /____/\__,_/____/

                                            version.0.1.0
`)

	util.Info("🚀 Starting broker on port %d\n", cfg.BrokerPort)
	util.Info("📊 Exporter: %v\n", cfg.EnableExporter)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runBroker(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		util.Fatal("❌ Broker failed: %v", err)
	}
}

func runBroker(ctx context.Context, cfg *config.Config) error {
	dm := disk.NewDiskManager(cfg)
	sm := stream.NewStreamManager(cfg.MaxStreamConnections, cfg.StreamTimeout, cfg.StreamHeartbeatInterval)
	smAdapter, err := topic.NewStreamManagerAdapter(sm)
	if err != nil {
		return fmt.Errorf("create stream manager adapter: %w", err)
	}

	storageProvider, err := newStorageProvider(dm, cfg.LogDir)
	if err != nil {
		util.Fatal("Failed to configure storage provider: %v", err)
	}

	tm := topic.NewTopicManager(cfg, storageProvider, smAdapter)
	if err := tm.RestoreTopics(); err != nil {
		util.Error("Failed to restore durable topic metadata; serving diagnostics only: %v", err)
		return runTopicMetadataDiagnostics(ctx, cfg, tm, dm)
	}

	cd := coordinator.NewCoordinator(ctx, cfg, tm)
	tm.SetCoordinator(cd)
	for _, gcfg := range cfg.StaticConsumerGroups {
		for _, topicName := range gcfg.Topics {
			current := tm.GetTopic(topicName)
			if current == nil {
				util.Error("⚠️ Topic %q does not exist; skipping static consumer group registration", topicName)
				continue
			}
			if _, err := tm.RegisterConsumerGroup(topicName, gcfg.Name, gcfg.ConsumerCount); err != nil {
				util.Error("⚠️ Failed to register static consumer group %q on topic %q: %v", gcfg.Name, topicName, err)
			}
		}
	}

	return runServerContext(ctx, cfg, tm, dm, cd, sm)
}
