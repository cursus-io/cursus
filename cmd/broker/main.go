package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/cursus-io/cursus/pkg/buildinfo"
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
var runConsumerMetadataDiagnostics = server.RunConsumerMetadataDiagnostics

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--verify-deployment-contract" {
		if err := buildinfo.VerifyDeploymentContract(); err != nil {
			util.Fatal("deployment contract verification failed: %v", err)
		}
		fmt.Printf("version=%s revision=%s wire=%s broker=%s snapshot=%s record=%s\n", buildinfo.Version, buildinfo.Revision, buildinfo.WireProtocolVersion, buildinfo.BrokerProtocolVersion, buildinfo.SnapshotFormatVersion, buildinfo.RecordFormatVersion)
		return
	}
	// Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		util.Fatal("❌ Failed to load config: %v", err)
	}

	data, err := config.MarshalRedactedJSON(cfg)
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

func runBroker(ctx context.Context, cfg *config.Config) (runErr error) {
	storageLock, err := disk.LockStorageDirectory(cfg.LogDir)
	if err != nil {
		return fmt.Errorf("lock broker storage: %w", err)
	}
	defer func() { _ = storageLock.Close() }()
	dm := disk.NewDiskManager(cfg)
	defer func() {
		if err := dm.Shutdown(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("shutdown broker storage: %w", err))
		}
	}()
	sm := stream.NewStreamManager(cfg.MaxStreamConnections, cfg.StreamTimeout)
	smAdapter, err := topic.NewStreamManagerAdapter(sm)
	if err != nil {
		return fmt.Errorf("create stream manager adapter: %w", err)
	}

	storageProvider, err := newStorageProvider(dm, cfg.LogDir)
	if err != nil {
		util.Fatal("Failed to configure storage provider: %v", err)
	}

	tm := topic.NewTopicManager(cfg, storageProvider, smAdapter)
	defer tm.Stop()
	if err := tm.RestoreTopics(); err != nil {
		util.Error("Failed to restore durable topic metadata; serving diagnostics only: %v", err)
		runErr = runTopicMetadataDiagnostics(ctx, cfg, tm, dm)
		if errors.Is(runErr, context.Canceled) {
			runErr = nil
		}
		return runErr
	}

	var cd *coordinator.Coordinator
	if cfg.EnabledDistribution {
		cd, err = coordinator.NewCoordinatorAwaitingDistributedRecovery(ctx, cfg, tm)
	} else {
		cd, err = coordinator.NewCoordinatorWithRecovery(ctx, cfg, tm)
	}
	if cd != nil {
		defer cd.Stop()
	}
	if err != nil {
		util.Error("Failed to recover durable consumer metadata; serving diagnostics only: %v", err)
		runErr = runConsumerMetadataDiagnostics(ctx, cfg, tm, dm, cd)
		if errors.Is(runErr, context.Canceled) {
			runErr = nil
		}
		return runErr
	}
	tm.SetCoordinator(cd)

	runErr = runServerContext(ctx, cfg, tm, dm, cd, sm)
	if errors.Is(runErr, server.ErrConsumerMetadataRecovery) {
		util.Error("Failed to recover durable consumer metadata; serving diagnostics only: %v", runErr)
		runErr = runConsumerMetadataDiagnostics(ctx, cfg, tm, dm, cd)
	}
	if errors.Is(runErr, context.Canceled) {
		runErr = nil
	}
	return runErr
}
