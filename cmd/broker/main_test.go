package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
)

func TestRunBrokerUsesDiagnosticsWhenTopicManifestRestoreFails(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = false
	if err := os.WriteFile(filepath.Join(cfg.LogDir, topic.TopicMetadataFileName), []byte(`{"version":`), 0o600); err != nil {
		t.Fatal(err)
	}

	originalDiagnostics := runTopicMetadataDiagnostics
	originalServer := runServerContext
	t.Cleanup(func() {
		runTopicMetadataDiagnostics = originalDiagnostics
		runServerContext = originalServer
	})
	diagnosticsCalled := false
	serverCalled := false
	runTopicMetadataDiagnostics = func(_ context.Context, _ *config.Config, tm *topic.TopicManager, _ *disk.DiskManager) error {
		diagnosticsCalled = true
		if err := tm.MetadataReadinessError(); err == nil {
			t.Error("diagnostics received topic manager without readiness error")
		}
		return nil
	}
	runServerContext = func(context.Context, *config.Config, *topic.TopicManager, *disk.DiskManager, *coordinator.Coordinator, *stream.StreamManager) error {
		serverCalled = true
		return nil
	}

	if err := runBroker(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if !diagnosticsCalled || serverCalled {
		t.Fatalf("diagnosticsCalled=%t serverCalled=%t", diagnosticsCalled, serverCalled)
	}
}

func TestRunBrokerStartsMainServerAfterSuccessfulTopicRestore(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = false

	originalDiagnostics := runTopicMetadataDiagnostics
	originalServer := runServerContext
	t.Cleanup(func() {
		runTopicMetadataDiagnostics = originalDiagnostics
		runServerContext = originalServer
	})
	diagnosticsCalled := false
	serverCalled := false
	runTopicMetadataDiagnostics = func(context.Context, *config.Config, *topic.TopicManager, *disk.DiskManager) error {
		diagnosticsCalled = true
		return nil
	}
	runServerContext = func(_ context.Context, _ *config.Config, _ *topic.TopicManager, dm *disk.DiskManager, _ *coordinator.Coordinator, _ *stream.StreamManager) error {
		serverCalled = true
		dm.CloseAllHandlers()
		return nil
	}

	if err := runBroker(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if diagnosticsCalled || !serverCalled {
		t.Fatalf("diagnosticsCalled=%t serverCalled=%t", diagnosticsCalled, serverCalled)
	}
}
