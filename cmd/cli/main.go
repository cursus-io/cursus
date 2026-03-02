package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Println("❌ Failed to load config:", err)
		os.Exit(1)
	}

	dm := disk.NewDiskManager(cfg)
	sm := stream.NewStreamManager(cfg.MaxStreamConnections, cfg.StreamTimeout, cfg.StreamHeartbeatInterval)
	smAdapter := topic.NewStreamManagerAdapter(sm)
	tm := topic.NewTopicManager(cfg, dm, smAdapter)
	cd := coordinator.NewCoordinator(cfg, tm)
	tm.SetCoordinator(cd)

	ctx := controller.NewClientContext("default-group", 0)
	ch := controller.NewCommandHandler(tm, cfg, cd, sm, nil)

	fmt.Println("🔹 Broker ready. Type HELP for commands.")
	fmt.Println("")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.EqualFold(line, "EXIT") {
			break
		}
		result := ch.HandleCommand(line, ctx)
		fmt.Println(result)
	}
}
