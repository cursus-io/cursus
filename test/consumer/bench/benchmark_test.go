package bench_test

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/consumer/bench"
)

func TestConsumerMetrics_PhaseSeparation(t *testing.T) {
	const total = int64(12)
	m := bench.NewConsumerMetrics(total, false)
	m.RecordBatch(0, 5)

	m.RebalanceStart()
	time.Sleep(10 * time.Millisecond)

	m.OnFirstConsumeAfterRebalance()
	m.RecordBatch(1, 7)

	ok, reason := m.IsFullyConsumed(12)
	if !ok {
		t.Fatalf("expected fully consumed, got reason: %s", reason)
	}
}

func TestConsumerMetrics_RecordBatch_TotalCount(t *testing.T) {
	const total = int64(10)
	m := bench.NewConsumerMetrics(total, false)

	m.RecordBatch(0, 3)
	m.RecordBatch(1, 2)
	m.RecordBatch(0, 5)

	ok, reason := m.IsFullyConsumed(10)
	if !ok {
		t.Fatalf("expected fully consumed, got: %s", reason)
	}
}

func TestConsumerMetrics_PrintSummary_Output(t *testing.T) {
	const total = int64(30)
	const shortDelay = 15 * time.Millisecond

	m := bench.NewConsumerMetrics(total, false)

	// initial
	m.RecordBatch(0, 10)
	time.Sleep(shortDelay)
	m.RecordBatch(0, 10)

	// rebalance
	m.RebalanceStart()
	time.Sleep(5 * time.Millisecond)
	m.OnFirstConsumeAfterRebalance()

	// rebalanced
	m.RecordBatch(1, 5)
	time.Sleep(1100 * time.Millisecond)
	m.RecordBatch(1, 5)

	var buf bytes.Buffer
	m.PrintSummaryTo(&buf)
	out := buf.String()

	required := []string{
		"CONSUMER BENCHMARK SUMMARY",
		"Overall TPS",
		"Rebalancing Cost",
		"Phase: initial",
		"Phase: rebalanced",
		"p95 Partition Avg TPS",
		"p99 Partition Avg TPS",
	}

	for _, r := range required {
		if !strings.Contains(out, r) {
			t.Fatalf("summary output missing %q\n%s", r, out)
		}
	}
}

func TestConsumerMetrics_Concurrency_BenchOnly(t *testing.T) {
	const (
		partitions = 4
		perWorker  = 100
		expected   = int64(partitions * perWorker)
	)

	m := bench.NewConsumerMetrics(expected, false)

	var wg sync.WaitGroup
	for pid := 0; pid < partitions; pid++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				m.RecordBatch(p, 1)
			}
		}(pid)
	}

	wg.Wait()

	ok, reason := m.IsFullyConsumed(int64(partitions * perWorker))
	if !ok {
		t.Fatalf("concurrency test failed: %s", reason)
	}
}
