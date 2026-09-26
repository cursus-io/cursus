package e2e_benchmark

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const durableBenchmarkResultVersion = 1

func benchmarkResultSummary(logs string) string {
	wanted := []string{"PRODUCER BENCHMARK SUMMARY", "CONSUMER BENCHMARK SUMMARY", "Partitions", "Total Batches", "Total Messages", "Failed messages", "Retry Count", "Publish elapsed Time", "Publish Message Throughput", "Latency P95", "Latency P99", "Elapsed Time", "Overall TPS", "Duplicate (MessageID)", "Duplicate (Offset)", "Message missing"}
	var out []string
	for _, line := range strings.Split(logs, "\n") {
		for _, token := range wanted {
			if strings.Contains(line, token) {
				out = append(out, strings.TrimSpace(line))
				break
			}
		}
	}
	return strings.Join(out, "\n")
}

func benchmarkCounter(logs, label string) (int, bool) {
	pattern := regexp.MustCompile(fmt.Sprintf(`(?im)%s[[:space:]]*:[[:space:]]*([0-9]+)`, regexp.QuoteMeta(label)))
	match := pattern.FindStringSubmatch(logs)
	if match == nil {
		return 0, false
	}
	count, err := strconv.Atoi(match[1])
	return count, err == nil
}

type durableBenchmarkConfig struct {
	LogDir          string
	ResultPath      string
	Revision        string
	StorageIdentity string
}

type durableBenchmarkStorage struct {
	Path      string `json:"path"`
	Identity  string `json:"identity"`
	FileCount int    `json:"file_count"`
	Bytes     int64  `json:"bytes"`
}

type durableBenchmarkResult struct {
	Version          int                     `json:"version"`
	Workload         string                  `json:"workload"`
	Revision         string                  `json:"revision"`
	StartedAt        time.Time               `json:"started_at"`
	CompletedAt      time.Time               `json:"completed_at"`
	DurationMS       int64                   `json:"duration_ms"`
	GoVersion        string                  `json:"go_version"`
	OS               string                  `json:"os"`
	Arch             string                  `json:"arch"`
	Storage          durableBenchmarkStorage `json:"storage"`
	Correctness      map[string]int          `json:"correctness"`
	PublisherSummary string                  `json:"publisher_summary"`
	ConsumerSummary  string                  `json:"consumer_summary"`
}

func durableBenchmarkConfigFromEnv() (durableBenchmarkConfig, bool, error) {
	if os.Getenv("RUN_E2E_DURABLE_BENCHMARK") != "1" {
		return durableBenchmarkConfig{}, false, nil
	}
	config := durableBenchmarkConfig{
		LogDir:          strings.TrimSpace(os.Getenv("CURSUS_DURABLE_BENCHMARK_LOG_DIR")),
		ResultPath:      strings.TrimSpace(os.Getenv("CURSUS_DURABLE_BENCHMARK_RESULT")),
		Revision:        strings.TrimSpace(os.Getenv("CURSUS_BENCHMARK_REVISION")),
		StorageIdentity: strings.TrimSpace(os.Getenv("CURSUS_DURABLE_BENCHMARK_STORAGE")),
	}
	if config.LogDir == "" || config.ResultPath == "" || config.Revision == "" || config.StorageIdentity == "" {
		return durableBenchmarkConfig{}, false, fmt.Errorf("durable benchmark requires CURSUS_DURABLE_BENCHMARK_LOG_DIR, CURSUS_DURABLE_BENCHMARK_RESULT, CURSUS_BENCHMARK_REVISION, and CURSUS_DURABLE_BENCHMARK_STORAGE")
	}
	logDir, err := filepath.Abs(config.LogDir)
	if err != nil {
		return durableBenchmarkConfig{}, false, fmt.Errorf("resolve durable benchmark log directory: %w", err)
	}
	info, err := os.Stat(logDir)
	if err != nil {
		return durableBenchmarkConfig{}, false, fmt.Errorf("stat durable benchmark log directory: %w", err)
	}
	if !info.IsDir() {
		return durableBenchmarkConfig{}, false, fmt.Errorf("durable benchmark log directory %q is not a directory", logDir)
	}
	resultPath, err := filepath.Abs(config.ResultPath)
	if err != nil {
		return durableBenchmarkConfig{}, false, fmt.Errorf("resolve durable benchmark result path: %w", err)
	}
	if parent, err := os.Stat(filepath.Dir(resultPath)); err != nil || !parent.IsDir() {
		return durableBenchmarkConfig{}, false, fmt.Errorf("durable benchmark result directory is unavailable: %s", filepath.Dir(resultPath))
	}
	config.LogDir = logDir
	config.ResultPath = resultPath
	return config, true, nil
}

func durableResult(config durableBenchmarkConfig, startedAt, completedAt time.Time, publisherLogs, consumerLogs string) (durableBenchmarkResult, error) {
	storage, err := durableStorageStats(config.LogDir, config.StorageIdentity)
	if err != nil {
		return durableBenchmarkResult{}, err
	}
	correctness := make(map[string]int, 4)
	for _, label := range []string{"Failed messages", "Message missing", "Duplicate (MessageID)", "Duplicate (Offset)"} {
		count, ok := benchmarkCounter(publisherLogs+"\n"+consumerLogs, label)
		if !ok {
			return durableBenchmarkResult{}, fmt.Errorf("durable benchmark result is missing correctness counter %q", label)
		}
		correctness[label] = count
	}
	return durableBenchmarkResult{
		Version:          durableBenchmarkResultVersion,
		Workload:         "standalone-durable-compose",
		Revision:         config.Revision,
		StartedAt:        startedAt.UTC(),
		CompletedAt:      completedAt.UTC(),
		DurationMS:       completedAt.Sub(startedAt).Milliseconds(),
		GoVersion:        runtime.Version(),
		OS:               runtime.GOOS,
		Arch:             runtime.GOARCH,
		Storage:          storage,
		Correctness:      correctness,
		PublisherSummary: benchmarkResultSummary(publisherLogs),
		ConsumerSummary:  benchmarkResultSummary(consumerLogs),
	}, nil
}

func durableStorageStats(root, identity string) (durableBenchmarkStorage, error) {
	stats := durableBenchmarkStorage{Path: root, Identity: identity}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		stats.FileCount++
		stats.Bytes += info.Size()
		return nil
	})
	if err != nil {
		return durableBenchmarkStorage{}, fmt.Errorf("inspect durable benchmark storage: %w", err)
	}
	return stats, nil
}

func writeDurableBenchmarkResult(path string, result durableBenchmarkResult) error {
	if err := validateDurableBenchmarkResult(result); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode durable benchmark result: %w", err)
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create durable benchmark result: %w", err)
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()
	if _, err := temp.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write durable benchmark result: %w", errors.Join(err, temp.Close()))
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync durable benchmark result: %w", errors.Join(err, temp.Close()))
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close durable benchmark result: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace durable benchmark result: %w", err)
	}
	return nil
}

func validateDurableBenchmarkResult(result durableBenchmarkResult) error {
	if result.Version != durableBenchmarkResultVersion || result.Workload == "" || result.Revision == "" || result.StartedAt.IsZero() || result.CompletedAt.IsZero() || result.CompletedAt.Before(result.StartedAt) {
		return fmt.Errorf("invalid durable benchmark result metadata")
	}
	if result.Storage.Path == "" || result.Storage.Identity == "" || result.Storage.FileCount < 0 || result.Storage.Bytes < 0 {
		return fmt.Errorf("invalid durable benchmark storage metadata")
	}
	keys := make([]string, 0, len(result.Correctness))
	for key, value := range result.Correctness {
		if value < 0 {
			return fmt.Errorf("invalid durable benchmark correctness count %q", key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "Duplicate (MessageID),Duplicate (Offset),Failed messages,Message missing" {
		return fmt.Errorf("durable benchmark result is missing required correctness counters")
	}
	return nil
}
