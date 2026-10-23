package disk

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func retentionWorkFixture(t testing.TB) (*DiskHandler, *config.Config, []types.DiskMessage) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.SegmentSize = 16 << 20
	cfg.IndexSize = 1 << 20
	cfg.SegmentRollTimeMS = 0
	cfg.RetentionCheckIntervalMS = 3600000
	cfg.CompactionCheckIntervalMS = 3600000
	d, err := NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	records := make([]types.DiskMessage, 4096)
	stamp := time.Unix(1700000000, 0)
	for i := range records {
		records[i] = types.DiskMessage{Topic: "orders", Partition: 1, Offset: uint64(i), AppendTimeMS: stamp.UnixMilli(), Payload: strings.Repeat("x", 1024)}
	}
	require.NoError(t, d.WriteBatchSync(records))
	require.NoError(t, d.RollSegmentAt(uint64(len(records))))
	setRetentionTestClock(d, stamp.Add(30*time.Minute))
	return d, cfg, records
}

// Setup, appends, initial startup and teardown are outside the benchmark timer.
// Read bytes are actual Read return counts, not physical device operations.
func BenchmarkRetentionTimeSummary(b *testing.B) {
	for _, cold := range []bool{true, false} {
		name := "warm"
		if cold {
			name = "cold"
		}
		b.Run(name, func(b *testing.B) {
			d, cfg, _ := retentionWorkFixture(b)
			d.EnforceRetention(cfg)
			before := d.RetentionTimeWorkSnapshot()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if cold {
					d.mu.Lock()
					d.retentionTimes = nil
					d.mu.Unlock()
				}
				d.EnforceRetention(cfg)
			}
			b.StopTimer()
			after := d.RetentionTimeWorkSnapshot()
			b.ReportMetric(float64(after.ReadBytes-before.ReadBytes)/float64(b.N), "read-B/op")
			b.ReportMetric(float64(after.Records-before.Records)/float64(b.N), "records/op")
			b.ReportMetric(0, "write-B/op")
		})
	}
}

func TestRetentionTimeRecoveryWork(t *testing.T) {
	d, cfg, records := retentionWorkFixture(t)
	stat, err := os.Stat(d.GetSegmentPath(0))
	require.NoError(t, err)
	var legacyBytes, timedBytes int64
	for _, r := range records {
		encoded, err := util.SerializeDiskMessage(r)
		require.NoError(t, err)
		timedBytes += int64(len(encoded) + 4)
		r.AppendTimeMS = 0
		encoded, err = util.SerializeDiskMessage(r)
		require.NoError(t, err)
		legacyBytes += int64(len(encoded) + 4)
	}
	require.Equal(t, timedBytes, stat.Size())
	require.Equal(t, int64(8*len(records)), timedBytes-legacyBytes)
	d.EnforceRetention(cfg)
	work := d.RetentionTimeWorkSnapshot()
	d.EnforceRetention(cfg)
	require.Equal(t, work.ReadBytes, d.RetentionTimeWorkSnapshot().ReadBytes)
	token, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	before := d.RetainedPageWorkSnapshot()
	export, err := d.ExportRetainedBootstrapContext(context.Background(), token, 0, 0, uint64(len(records)), bootstrapTestFence(1, uint64(len(records))))
	require.NoError(t, err)
	defer func() { require.NoError(t, export.Close()) }()
	var transfer, temporary int64
	for _, file := range export.Manifest().Files {
		temporary += file.Size
		for off := int64(0); off < file.Size; {
			chunk, err := export.ReadChunk(file.ID, off, BootstrapChunkBytes)
			require.NoError(t, err)
			require.NotEmpty(t, chunk)
			off += int64(len(chunk))
			transfer += int64(len(chunk))
		}
	}
	after := d.RetainedPageWorkSnapshot()
	t.Logf("records=%d retained_log_bytes=%d legacy_log_bytes=%d metadata_overhead_bytes=%d summary_read_bytes=%d summary_records=%d summary_write_bytes=0 summary_temp_bytes=0 warm_extra_read_bytes=0", len(records), stat.Size(), legacyBytes, timedBytes-legacyBytes, work.ReadBytes, work.Records)
	t.Logf("subsequent_bootstrap page_read_bytes=%d page_decoded_records=%d canonical_write_bytes=%d hash_read_bytes=%d chunk_transfer_bytes=%d export_payload_temp_bytes=%d pages=%d (logical API bytes; no physical I/O attribution)", after.BytesRead-before.BytesRead, after.DecodedFrames-before.DecodedFrames, export.RetainedWork().CanonicalWriteBytes, export.RetainedWork().HashReadBytes, transfer, temporary, export.RetainedWork().Pages)
}
