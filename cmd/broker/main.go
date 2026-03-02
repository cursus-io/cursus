package main

import (
	"encoding/json"
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/server"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

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

	// Initialization
	dm := disk.NewDiskManager(cfg)
	sm := stream.NewStreamManager(cfg.MaxStreamConnections, cfg.StreamTimeout, cfg.StreamHeartbeatInterval)
	smAdapter := topic.NewStreamManagerAdapter(sm)

	tm := topic.NewTopicManager(cfg, dm, smAdapter)
	cd := coordinator.NewCoordinator(cfg, tm)
	tm.SetCoordinator(cd)

	// Static consumer groups
	for _, gcfg := range cfg.StaticConsumerGroups {
		for _, topicName := range gcfg.Topics {
			t := tm.GetTopic(topicName)
			if t == nil {
				util.Error("⚠️ Topic %q does not exist; skipping static consumer group registration", topicName)
			} else {
				if _, err := tm.RegisterConsumerGroup(topicName, gcfg.Name, gcfg.ConsumerCount); err != nil {
					util.Error("⚠️ Failed to register static consumer group %q on topic %q: %v", gcfg.Name, topicName, err)
				}
			}
		}
	}

	if err := server.RunServer(cfg, tm, dm, cd, sm); err != nil {
		util.Fatal("❌ Broker failed: %v", err)
	}
}
