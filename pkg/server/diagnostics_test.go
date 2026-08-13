package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

func TestRunTopicMetadataDiagnosticsServesFailureWithoutBrokerListener(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = false
	cfg.EnableExporter = true
	cfg.HealthCheckPort = reserveTCPPort(t)
	cfg.ExporterPort = reserveTCPPort(t)
	cfg.BrokerPort = reserveTCPPort(t)
	if err := os.WriteFile(filepath.Join(cfg.LogDir, topic.TopicMetadataFileName), []byte(`{"version":`), 0o600); err != nil {
		t.Fatal(err)
	}

	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	tm := topic.NewTopicManager(cfg, dm, nil)
	if err := tm.RestoreTopics(); err == nil {
		t.Fatal("expected corrupt manifest restore error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunTopicMetadataDiagnostics(ctx, cfg, tm, dm) }()

	healthBase := fmt.Sprintf("http://127.0.0.1:%d", cfg.HealthCheckPort)
	waitForHTTPStatus(t, healthBase+"/live", http.StatusOK)
	response, err := http.Get(healthBase + "/ready") // #nosec G107 -- loopback test address.
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	var report healthResponse
	if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(report.Checks["topic_metadata"], "manifest load failure") {
		t.Fatalf("topic_metadata check = %q", report.Checks["topic_metadata"])
	}

	metricsURL := fmt.Sprintf("http://127.0.0.1:%d/metrics", cfg.ExporterPort)
	waitForHTTPStatus(t, metricsURL, http.StatusOK)
	metricsResponse, err := http.Get(metricsURL) // #nosec G107 -- loopback test address.
	if err != nil {
		t.Fatal(err)
	}
	metricsBody, err := io.ReadAll(metricsResponse.Body)
	_ = metricsResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"cursus_broker_ready 0",
		"cursus_topic_metadata_manifest_load_failure 1",
		"cursus_topic_metadata_orphan_topics 0",
		"cursus_topic_metadata_durability_warning 0",
		"cursus_topic_metadata_durability_warnings_total 0",
	} {
		if !strings.Contains(string(metricsBody), expected) {
			t.Fatalf("metrics missing %q", expected)
		}
	}

	connection, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.BrokerPort), 100*time.Millisecond)
	if err == nil {
		_ = connection.Close()
		t.Fatal("diagnostics mode unexpectedly opened the broker listener")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("diagnostics returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("diagnostics did not stop after cancellation")
	}
}

func TestRunConsumerMetadataDiagnosticsExposesFailureAndMetrics(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = false
	cfg.EnableExporter = true
	cfg.HealthCheckPort = reserveTCPPort(t)
	cfg.ExporterPort = reserveTCPPort(t)
	cfg.BrokerPort = reserveTCPPort(t)

	seedDM := disk.NewDiskManager(cfg)
	seedTM := topic.NewTopicManager(cfg, seedDM, nil)
	seedCoordinator, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, seedTM)
	if err != nil {
		t.Fatal(err)
	}
	if err := seedTM.PublishWithAck(config.ConsumerOffsetsTopicName, &types.Message{Key: "corrupt", Payload: "not-json"}); err != nil {
		t.Fatal(err)
	}
	seedCoordinator.Stop()
	seedDM.CloseAllHandlers()

	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	tm := topic.NewTopicManager(cfg, dm, nil)
	if err := tm.RestoreTopics(); err != nil {
		t.Fatal(err)
	}
	failedCoordinator, recoveryErr := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, tm)
	if recoveryErr == nil {
		t.Fatal("expected consumer metadata recovery failure")
	}
	t.Cleanup(failedCoordinator.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunConsumerMetadataDiagnostics(ctx, cfg, tm, dm, failedCoordinator) }()

	healthBase := fmt.Sprintf("http://127.0.0.1:%d", cfg.HealthCheckPort)
	waitForHTTPStatus(t, healthBase+"/live", http.StatusOK)
	response, err := http.Get(healthBase + "/ready") // #nosec G107 -- loopback test address.
	if err != nil {
		t.Fatal(err)
	}
	var report healthResponse
	decodeErr := json.NewDecoder(response.Body).Decode(&report)
	_ = response.Body.Close()
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(report.Checks["consumer_metadata"], "decode internal metadata") {
		t.Fatalf("status=%d consumer_metadata=%q", response.StatusCode, report.Checks["consumer_metadata"])
	}

	metricsURL := fmt.Sprintf("http://127.0.0.1:%d/metrics", cfg.ExporterPort)
	waitForHTTPStatus(t, metricsURL, http.StatusOK)
	metricsResponse, err := http.Get(metricsURL) // #nosec G107 -- loopback test address.
	if err != nil {
		t.Fatal(err)
	}
	metricsBody, err := io.ReadAll(metricsResponse.Body)
	_ = metricsResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`cursus_consumer_metadata_recovery_ready{phase="consumer_metadata_scan"} 0`,
		"cursus_consumer_metadata_corrupt_records 1",
		"cursus_consumer_metadata_restored_groups 0",
		"cursus_consumer_metadata_restored_offsets 0",
	} {
		if !strings.Contains(string(metricsBody), expected) {
			t.Fatalf("metrics missing %q", expected)
		}
	}

	connection, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.BrokerPort), 100*time.Millisecond)
	if err == nil {
		_ = connection.Close()
		t.Fatal("consumer diagnostics mode unexpectedly opened the broker listener")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consumer diagnostics returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consumer diagnostics did not stop after cancellation")
	}
}
func TestRunTopicMetadataDiagnosticsRejectsMissingInputs(t *testing.T) {
	var nilContext context.Context
	if err := RunTopicMetadataDiagnostics(nilContext, config.DefaultConfig(), nil, nil); err == nil {
		t.Fatal("expected nil context error")
	}
	if err := RunTopicMetadataDiagnostics(context.Background(), nil, nil, nil); err == nil {
		t.Fatal("expected nil config error")
	}
}

func reserveTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func waitForHTTPStatus(t *testing.T, url string, status int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(url) // #nosec G107 -- loopback test address.
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == status {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s did not return status %d", url, status)
}
