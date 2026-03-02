package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cursus-io/cursus/util"
)

const sep = "========================================"

type PartitionStat struct {
	PartitionID int
	BatchCount  int
	AvgDuration time.Duration
}

type BenchmarkResult struct {
	Timestamp      time.Time         `json:"timestamp"`
	TotalTarget    int               `json:"total_target"`
	UniqueSent     int               `json:"unique_sent"`
	TotalPublished int               `json:"total_published"`
	RetryCount     int               `json:"retry_count"`
	SentMessages   int               `json:"sent_messages"`
	FailedMessages int               `json:"failed_messages"`
	SuccessRate    float64           `json:"success_rate"`
	TotalDuration  float64           `json:"duration_seconds"`
	MsgPerSec      float64           `json:"msg_per_sec"`
	LatencyP95     float64           `json:"latency_p95_ms"`
	LatencyP99     float64           `json:"latency_p99_ms"`
	Errors         map[string]uint64 `json:"errors,omitempty"`
}

func CalculateLatencyPercentiles(latencies []time.Duration) (p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0
	}

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	n := len(sorted)
	p95Idx := int(float64(n) * 0.95)
	p99Idx := int(float64(n) * 0.99)

	if p95Idx >= n {
		p95Idx = n - 1
	}
	if p99Idx >= n {
		p99Idx = n - 1
	}

	return sorted[p95Idx], sorted[p99Idx]
}

func GenerateMessage(size int, seqNum int) string {
	if size <= 0 {
		return fmt.Sprintf("%s #%d", "Hello World!", seqNum)
	}

	header := fmt.Sprintf("msg-%d-", seqNum)
	paddingSize := size - len(header)
	if paddingSize < 0 {
		paddingSize = 0
	}

	padding := strings.Repeat("x", paddingSize)
	return header + padding
}

func PrintBenchmarkSummaryFixedTo(
	w io.Writer,
	partitionStats []PartitionStat,
	totalSent int,
	publishedMessages int,
	targetCount int,
	totalDuration time.Duration,
	errSummary map[string]uint64,
	allLatencies []time.Duration,
) {
	totalBatches := 0
	for _, ps := range partitionStats {
		totalBatches += ps.BatchCount
	}

	retryCount := totalSent - publishedMessages
	if retryCount < 0 {
		retryCount = 0
	}

	failedCount := targetCount - publishedMessages
	if failedCount < 0 {
		failedCount = 0
	}

	seconds := totalDuration.Seconds()
	if seconds <= 0 {
		seconds = 0.001
	}

	successRate := 0.0
	if targetCount > 0 {
		successRate = (float64(publishedMessages) / float64(targetCount)) * 100
	}

	messagesPerSec := float64(publishedMessages) / seconds
	batchesPerSec := float64(totalBatches) / seconds

	p95, p99 := CalculateLatencyPercentiles(allLatencies)

	_, _ = fmt.Fprint(w, "\r\n")
	_, _ = fmt.Fprintln(w, sep)
	_, _ = fmt.Fprintln(w, "📊 PRODUCER BENCHMARK SUMMARY")
	_, _ = fmt.Fprintf(w, "%-28s : %d\n", "Partitions", len(partitionStats))
	_, _ = fmt.Fprintf(w, "%-28s : %d\n", "Total Batches", totalBatches)
	_, _ = fmt.Fprintf(w, "%-28s : %d / %d (rate: %.2f%%)\n", "Total Messages", publishedMessages, targetCount, successRate)
	_, _ = fmt.Fprintf(w, "%-28s : %d\n", "Failed messages", failedCount)
	_, _ = fmt.Fprintf(w, "%-28s : %d\n", "Retry Count", retryCount)
	_, _ = fmt.Fprintf(w, "%-28s : %.3fs\n", "Publish elapsed Time", seconds)
	_, _ = fmt.Fprintf(w, "%-28s : %.2f batches/s\n", "Publish Batch Throughput", batchesPerSec)
	_, _ = fmt.Fprintf(w, "%-28s : %.2f msg/s\n", "Publish Message Throughput", messagesPerSec)
	_, _ = fmt.Fprintf(w, "%-28s : %.2f ms\n", "Latency P95", float64(p95.Microseconds())/1000.0)
	_, _ = fmt.Fprintf(w, "%-28s : %.2f ms\n", "Latency P99", float64(p99.Microseconds())/1000.0)
	_, _ = fmt.Fprint(w, "\r\n")

	_, _ = fmt.Fprintln(w, "Partition Breakdown:")
	for _, ps := range partitionStats {
		_, _ = fmt.Fprintf(w, "  #%-2d batches=%-4d avg_batch=%.2fms\n", ps.PartitionID, ps.BatchCount, float64(ps.AvgDuration.Microseconds())/1000.0)
	}

	if len(errSummary) > 0 {
		_, _ = fmt.Fprintln(w, "\n❌ Error Root Cause Analysis:")
		for msg, count := range errSummary {
			_, _ = fmt.Fprintf(w, "  - [%d occurrences]: %s\n", count, msg)
		}
	}
	_, _ = fmt.Fprintln(w, sep)

	result := BenchmarkResult{
		Timestamp:      time.Now(),
		TotalTarget:    targetCount,
		UniqueSent:     publishedMessages,
		TotalPublished: totalSent,
		RetryCount:     retryCount,
		SentMessages:   publishedMessages,
		FailedMessages: failedCount,
		SuccessRate:    successRate,
		TotalDuration:  seconds,
		MsgPerSec:      messagesPerSec,
		LatencyP95:     float64(p95.Microseconds()) / 1000.0,
		LatencyP99:     float64(p99.Microseconds()) / 1000.0,
		Errors:         errSummary,
	}
	saveResultToJSON(result)
}

func saveResultToJSON(res BenchmarkResult) {
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		util.Error("Failed to marshal benchmark result to JSON: %v", err)
		return
	}

	dir := "bench_results"
	if err := os.MkdirAll(dir, 0755); err != nil {
		util.Error("Failed to create directory '%s': %v", dir, err)
		return
	}

	filename := fmt.Sprintf("%s/bench_%d.json", dir, time.Now().Unix())
	err = os.WriteFile(filename, data, 0644)
	if err != nil {
		util.Error("Failed to save benchmark JSON file '%s': %v", filename, err)
		return
	}
	util.Info("✅ Benchmark result successfully saved to %s", filename)
}
