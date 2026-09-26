package e2e_benchmark

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDurableBenchmarkConfigRequiresCompleteMetadata(t *testing.T) {
	t.Setenv("RUN_E2E_DURABLE_BENCHMARK", "1")
	t.Setenv("CURSUS_DURABLE_BENCHMARK_LOG_DIR", "")
	t.Setenv("CURSUS_DURABLE_BENCHMARK_RESULT", "")
	t.Setenv("CURSUS_BENCHMARK_REVISION", "")
	t.Setenv("CURSUS_DURABLE_BENCHMARK_STORAGE", "")
	if _, _, err := durableBenchmarkConfigFromEnv(); err == nil {
		t.Fatal("expected incomplete durable benchmark configuration to fail")
	}
}

func TestDurableBenchmarkResultWritesValidatedJSON(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "segment.log"), []byte("durable"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := durableBenchmarkConfig{
		LogDir:          dir,
		ResultPath:      filepath.Join(dir, "result.json"),
		Revision:        "test-revision",
		StorageIdentity: "test-filesystem",
	}
	startedAt := time.Now().Add(-time.Second)
	publisherLogs := "Failed messages : 0\nMessage missing : 0\nDuplicate (MessageID) : 0\nDuplicate (Offset) : 0"
	consumerLogs := "All messages consumed\nMessage missing : 0\nDuplicate (MessageID) : 0\nDuplicate (Offset) : 0"
	result, err := durableResult(config, startedAt, time.Now(), publisherLogs, consumerLogs)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDurableBenchmarkResult(config.ResultPath, result); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(config.ResultPath)
	if err != nil {
		t.Fatal(err)
	}
	var decoded durableBenchmarkResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Revision != config.Revision || decoded.Storage.FileCount != 1 || decoded.Storage.Bytes != int64(len("durable")) {
		t.Fatalf("unexpected durable result: %+v", decoded)
	}
}

func TestDurableResultRejectsMissingCorrectnessCounters(t *testing.T) {
	config := durableBenchmarkConfig{LogDir: t.TempDir(), Revision: "test", StorageIdentity: "test"}
	if _, err := durableResult(config, time.Now().Add(-time.Second), time.Now(), "Failed messages : 0", ""); err == nil {
		t.Fatal("expected missing correctness counters to fail")
	}
}
