package topic

import (
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

// Isolates real disk-page decode work from handlers, offset commits and network
// timing. Setup and persistence are outside the timer; the same eight visible
// records are read from a larger committed backlog on every iteration.
func BenchmarkReadCommittedSmallPage(b *testing.B) {
	cfg := config.DefaultConfig()
	cfg.LogDir = b.TempDir()
	cfg.RetentionCheckIntervalMS = 3600000
	cfg.CompactionCheckIntervalMS = 3600000
	cfg.SegmentRollTimeMS = 0
	util.SetLevel(util.LogLevelInfo)
	d, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(b, err)
	records := make([]types.DiskMessage, 1600)
	for i := range records {
		records[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: strings.Repeat("x", 128)}
	}
	require.NoError(b, d.WriteBatchSync(records))
	p := NewPartition(0, "orders", d, nil, cfg)
	p.SetHWM(1600)
	b.Cleanup(func() { p.Close(); require.NoError(b, d.Close()) })
	b.ReportAllocs()
	b.SetBytes(8 * 128)
	decoded := int64(0)
	b.ResetTimer()
	for range b.N {
		messages, bytes, next, err := p.ReadCommittedPage(0, 8, 4<<20, true)
		if err != nil || len(messages) != 8 || next != 8 {
			b.Fatalf("page = %d records, next %d, error %v", len(messages), next, err)
		}
		decoded += int64(bytes)
	}
	b.StopTimer()
	b.ReportMetric(float64(decoded)/float64(b.N), "decoded-B/op")
}
