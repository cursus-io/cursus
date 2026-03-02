package disk

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskHandler_EnforceRetention(t *testing.T) {
	createSegment := func(t *testing.T, dh *DiskHandler, segNum int, size int, modTime time.Time) {
		logPath := dh.GetSegmentPath(uint64(segNum))
		indexPath := dh.GetIndexPath(uint64(segNum))

		require.NoError(t, os.MkdirAll(filepath.Dir(logPath), 0755))
		require.NoError(t, os.WriteFile(logPath, make([]byte, size), 0644))
		require.NoError(t, os.Chtimes(logPath, modTime, modTime))
		require.NoError(t, os.WriteFile(indexPath, []byte("index_data"), 0644))

		dh.segments = append(dh.segments, uint64(segNum))
	}

	tests := []struct {
		name           string
		cfg            *config.Config
		setup          func(t *testing.T, dh *DiskHandler)
		expectedDelete []int
		expectedKeep   []int
	}{
		{
			name: "RetentionBytes_OldSegmentsDeleted",
			cfg: &config.Config{
				RetentionBytes: 300,
				RetentionHours: 0,
			},
			setup: func(t *testing.T, dh *DiskHandler) {
				for i := 0; i < 5; i++ {
					createSegment(t, dh, i, 100, time.Now())
				}
			},
			expectedDelete: []int{0, 1},
			expectedKeep:   []int{2, 3, 4},
		},
		{
			name: "RetentionHours_ExpiredSegmentsDeleted",
			cfg: &config.Config{
				RetentionBytes: 0,
				RetentionHours: 24,
			},
			setup: func(t *testing.T, dh *DiskHandler) {
				oldTime := time.Now().Add(-48 * time.Hour)
				createSegment(t, dh, 10, 100, oldTime)
				createSegment(t, dh, 11, 100, time.Now())
			},
			expectedDelete: []int{10},
			expectedKeep:   []int{11},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			baseName := filepath.Join(tmpDir, "test_topic", "partition_0")
			dh := &DiskHandler{BaseName: baseName}
			tt.setup(t, dh)

			sort.Slice(dh.segments, func(i, j int) bool {
				return dh.segments[i] < dh.segments[j]
			})

			dh.EnforceRetention(tt.cfg)

			for _, segNum := range tt.expectedDelete {
				assert.NotContains(t, dh.segments, uint64(segNum), "Segment %d should be removed from memory slice", segNum)
			}

			assert.Equal(t, len(tt.expectedKeep), len(dh.segments), "Memory slice size mismatch")
			for _, segNum := range tt.expectedKeep {
				assert.Contains(t, dh.segments, uint64(segNum), "Segment %d should exist in memory slice", segNum)
			}

			for _, segNum := range tt.expectedDelete {
				assert.FileExists(t, dh.GetSegmentPath(uint64(segNum))+".deleted", "Log %d should be marked as deleted", segNum)
				assert.FileExists(t, dh.GetIndexPath(uint64(segNum))+".deleted", "Index %d should be marked as deleted", segNum)
			}
			for _, segNum := range tt.expectedKeep {
				logPath := dh.GetSegmentPath(uint64(segNum))
				indexPath := dh.GetIndexPath(uint64(segNum))

				assert.FileExists(t, logPath, "Log %d should still exist", segNum)
				assert.NoFileExists(t, logPath+".deleted", "Log %d should NOT have .deleted suffix", segNum)

				assert.FileExists(t, indexPath, "Index %d should still exist", segNum)
				assert.NoFileExists(t, indexPath+".deleted", "Index %d should NOT have .deleted suffix", segNum)
			}
		})
	}
}
