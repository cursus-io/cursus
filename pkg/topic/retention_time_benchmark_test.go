package topic

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// Includes durable append and the broker timestamp clock guard. Setup and
// recovery are outside the timer. This measures local requests, not network RTT.
func BenchmarkRetentionTimeAppend(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("enabled=%v", enabled), func(b *testing.B) {
			cfg := config.DefaultConfig()
			cfg.LogDir = b.TempDir()
			cfg.RetentionTimeEnabled = enabled
			cfg.RetentionCheckIntervalMS = 3600000
			cfg.CompactionCheckIntervalMS = 3600000
			cfg.SegmentRollTimeMS = 0
			d, err := disk.NewDiskHandler(cfg, "orders", 0)
			require.NoError(b, err)
			p := NewPartition(0, "orders", d, nil, cfg)
			b.Cleanup(func() { p.Close(); require.NoError(b, d.Close()) })
			message := types.Message{Payload: strings.Repeat("x", 1024)}
			durations := make([]int64, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				err := p.EnqueueSync(message)
				durations[i] = time.Since(start).Nanoseconds()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
			b.ReportMetric(float64(durations[(b.N-1)/2]), "p50-ns")
			b.ReportMetric(float64(durations[(b.N-1)*95/100]), "p95-ns")
		})
	}
}
