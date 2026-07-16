package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	smAdapter, err := topic.NewStreamManagerAdapter(sm)
	if err != nil {
		fmt.Println("❌ Failed to create stream manager adapter:", err)
		os.Exit(1)
	}
	tm := topic.NewTopicManager(cfg, dm, smAdapter)
	cd := coordinator.NewCoordinator(context.Background(), cfg, tm)
	tm.SetCoordinator(cd)

	ctx := controller.NewClientContext("default-group", 0)
	ch := controller.NewCommandHandler(tm, cfg, cd, sm, nil)
	if !cfg.EnabledDistribution {
		journalPath := filepath.Join(cfg.LogDir, "__transaction_state.journal")
		if err := ch.ConfigureTransactionJournal(journalPath); err != nil {
			fmt.Println("❌ Failed to initialize transaction journal:", err)
			os.Exit(1)
		}
	}
	if err := ch.RecoverPreparedTransactions(); err != nil {
		fmt.Println("❌ Failed to recover prepared transactions:", err)
		os.Exit(1)
	}

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
