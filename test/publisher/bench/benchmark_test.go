package bench_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/test/publisher/bench"
)

func TestGenerateMessage(t *testing.T) {
	tests := []struct {
		size   int
		seqNum int
		want   string
	}{
		{0, 1, "Hello World! #1"},
		{5, 1, "msg-1-"},
		{15, 2, "msg-2-xxxxxxx"},
	}

	for _, tt := range tests {
		got := bench.GenerateMessage(tt.size, tt.seqNum)
		if tt.size <= 0 {
			if got != tt.want {
				t.Errorf("GenerateMessage(%d, %d) = %q; want %q", tt.size, tt.seqNum, got, tt.want)
			}
		} else {
			if !strings.HasPrefix(got, "msg-") {
				t.Errorf("GenerateMessage(%d, %d) = %q; want prefix 'msg-'", tt.size, tt.seqNum, got)
			}
			if len(got) < tt.size {
				t.Errorf("GenerateMessage(%d, %d) length = %d; want >= %d", tt.size, tt.seqNum, len(got), tt.size)
			}
		}
	}
}

func TestPrintBenchmarkSummaryFixedTo(t *testing.T) {
	stats := []bench.PartitionStat{
		{PartitionID: 0, BatchCount: 10, AvgDuration: 20 * time.Millisecond},
	}

	latencies := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		latencies[i] = time.Duration(i) * time.Millisecond
	}

	totalTarget := 1000
	uniqueSent := 800
	totalPublished := 950
	totalDuration := 2 * time.Second
	errSummary := map[string]uint64{"timeout": 200}

	var buf bytes.Buffer
	bench.PrintBenchmarkSummaryFixedTo(&buf, stats, totalPublished, uniqueSent, totalTarget, totalDuration, errSummary, latencies)

	got := buf.String()

	expectedKeywords := []string{
		"PRODUCER BENCHMARK SUMMARY",
		"Total Messages",
		"Latency P95",
		"Latency P99",
		"rate: 80.00%",
		"Failed messages",
		"Retry Count",
	}

	for _, keyword := range expectedKeywords {
		if !strings.Contains(got, keyword) {
			t.Errorf("Output missing keyword '%s'", keyword)
		}
	}
}

func TestPrintBenchmarkSummaryNoErrors(t *testing.T) {
	stats := []bench.PartitionStat{{PartitionID: 0, BatchCount: 5, AvgDuration: 10 * time.Millisecond}}
	totalTarget := 100
	uniqueSent := 100
	totalPublished := 100
	testDuration := time.Second
	testLatencies := []time.Duration{10 * time.Millisecond}

	errSummary := make(map[string]uint64)

	var buf bytes.Buffer
	bench.PrintBenchmarkSummaryFixedTo(&buf, stats, totalPublished, uniqueSent, totalTarget, testDuration, errSummary, testLatencies)

	got := buf.String()
	if strings.Contains(got, "Error Root Cause Analysis") {
		t.Error("Output should NOT contain Error Analysis section when there are no errors")
	}
}

func TestCalculateLatencyPercentiles(t *testing.T) {
	tests := []struct {
		name    string
		input   []time.Duration
		wantP95 time.Duration
		wantP99 time.Duration
	}{
		{
			name:    "Empty slice should return zeros",
			input:   []time.Duration{},
			wantP95: 0,
			wantP99: 0,
		},
		{
			name:    "Single element should return that element for both",
			input:   []time.Duration{100 * time.Millisecond},
			wantP95: 100 * time.Millisecond,
			wantP99: 100 * time.Millisecond,
		},
		{
			name: "Perfectly sorted 100 elements",
			input: func() []time.Duration {
				d := make([]time.Duration, 100)
				for i := 0; i < 100; i++ {
					d[i] = time.Duration(i+1) * time.Millisecond
				}
				return d
			}(),
			// n=100 -> p95Idx = 95, p99Idx = 99
			wantP95: 96 * time.Millisecond,
			wantP99: 100 * time.Millisecond,
		},
		{
			name:    "Unsorted input should be handled correctly",
			input:   []time.Duration{500 * time.Millisecond, 100 * time.Millisecond, 300 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond},
			wantP95: 500 * time.Millisecond, // n=5, 5*0.95 = 4.75 -> idx 4
			wantP99: 500 * time.Millisecond, // n=5, 5*0.99 = 4.95 -> idx 4
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotP95, gotP99 := bench.CalculateLatencyPercentiles(tt.input)
			if gotP95 != tt.wantP95 {
				t.Errorf("calculate latencyPercentiles gotP95 = %v, want %v", gotP95, tt.wantP95)
			}
			if gotP99 != tt.wantP99 {
				t.Errorf("calculate latencyPercentiles gotP99 = %v, want %v", gotP99, tt.wantP99)
			}
		})
	}
}
