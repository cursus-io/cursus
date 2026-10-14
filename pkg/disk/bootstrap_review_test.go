package disk

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestBootstrapExportChecksEachPhysicalSegment(t *testing.T) {
	for _, damage := range []string{"none", "truncated-unmarked-tail", "overlap"} {
		t.Run(damage, func(t *testing.T) {
			source, _ := bootstrapTestHandler(t, 1)
			require.NoError(t, source.WriteCompactedReplicaRange(0, 5, []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 1, Payload: "compacted"}}))
			for i := 5; i < 10; i++ {
				offset, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: fmt.Sprint(i)})
				require.NoError(t, err)
				require.Equal(t, uint64(i), offset)
			}
			require.NoError(t, source.RollSegmentAt(10))
			switch damage {
			case "truncated-unmarked-tail":
				var keep int64
				require.NoError(t, scanCompactionSegment(source.GetSegmentPath(5), 5, false, false, func(frame compactionFrame) error {
					if frame.message.Offset <= 6 {
						keep += int64(frame.size)
					}
					return nil
				}))
				require.NoError(t, os.Truncate(source.GetSegmentPath(5), keep))
			case "overlap":
				frame, err := util.SerializeDiskMessage(types.DiskMessage{Topic: "orders", Partition: 1, Offset: 10, Payload: "wrong segment"})
				require.NoError(t, err)
				f, err := os.OpenFile(source.GetSegmentPath(5), os.O_APPEND|os.O_WRONLY, 0o600)
				require.NoError(t, err)
				require.NoError(t, binary.Write(f, binary.BigEndian, uint32(len(frame))))
				_, err = f.Write(frame)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}
			bootstrapBudget.Lock()
			beforeActive, beforeBytes := bootstrapBudget.active, bootstrapBudget.bytes
			bootstrapBudget.Unlock()
			e, err := source.ExportBootstrap(0, 10, bootstrapTestFence(1, 10), nil)
			if damage != "none" {
				require.Error(t, err)
				require.Nil(t, e)
				entries, err := os.ReadDir(filepath.Dir(source.BaseName))
				require.NoError(t, err)
				for _, entry := range entries {
					require.False(t, strings.HasPrefix(entry.Name(), "partition_1.bootstrap-"))
				}
				bootstrapBudget.Lock()
				afterActive, afterBytes := bootstrapBudget.active, bootstrapBudget.bytes
				bootstrapBudget.Unlock()
				require.Equal(t, beforeActive, afterActive)
				require.Equal(t, beforeBytes, afterBytes)
				return
			}
			require.NoError(t, err)
			defer func() { require.NoError(t, e.Close()) }()
			require.True(t, e.Manifest().Compacted)
			target, _ := bootstrapTestHandler(t, 1)
			r := bootstrapTestReceive(t, target, e)
			require.NoError(t, r.Install())
			require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
			want, err := source.RetainedRangeDigest(0, 10)
			require.NoError(t, err)
			got, err := target.RetainedRangeDigest(0, 10)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestBootstrapExportPreservesSubrangesAndActiveUncommittedTail(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	for i := 0; i < 4; i++ {
		_, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: fmt.Sprint(i)})
		require.NoError(t, err)
	}
	// Only [1,2) is committed for this request; physical offsets 2 and 3 are an
	// allowed active tail and the earlier offset 0 lies outside the subrange.
	e, err := source.ExportBootstrap(1, 2, bootstrapTestFence(1, 2), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, e.Close()) }()
	require.False(t, e.Manifest().Compacted)
	target, _ := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, target, e)
	require.NoError(t, r.Install())
	require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
	records := bootstrapRawRecords(t, target.GetSegmentPath(1))
	require.Len(t, records, 1)
	require.Equal(t, uint64(1), records[0].Offset)
	require.Equal(t, "1", records[0].Payload)
}

func TestBootstrapPendingIndexGeometrySurvivesConfigurationChange(t *testing.T) {
	for _, end := range []uint64{0, 5} {
		t.Run(fmt.Sprint(end), func(t *testing.T) {
			source, _ := bootstrapTestHandler(t, 1)
			if end != 0 {
				require.NoError(t, source.WriteCompactedReplicaRange(0, end, nil))
			}
			e := bootstrapTestExport(t, source, 0, end)
			target, cfg := bootstrapTestHandler(t, 1)
			r := bootstrapTestReceive(t, target, e)
			require.NoError(t, r.Install())
			require.NoError(t, target.Close())
			cfg.IndexSize = 8192
			for attempt := 0; attempt < 2; attempt++ {
				var err error
				target, err = NewDiskHandler(cfg, "orders", 1)
				require.NoError(t, err)
				require.Equal(t, uint64(4096), target.IndexSize)
				info, err := os.Stat(target.GetIndexPath(end))
				require.NoError(t, err)
				require.Equal(t, int64(4096), info.Size())
				if attempt == 1 {
					require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
				}
				require.NoError(t, target.Close())
			}
			var err error
			target, err = NewDiskHandler(cfg, "orders", 1)
			require.NoError(t, err)
			defer func() { require.NoError(t, target.Close()) }()
			require.Equal(t, uint64(8192), target.IndexSize)
		})
	}
}

func TestBootstrapInstallRetriesAfterPreIntentFailure(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	_, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: "new"})
	require.NoError(t, err)
	e := bootstrapTestExport(t, source, 0, 1)
	target, _ := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, target, e)
	// This invalid owned-file shape fails inventory after index generation and
	// before durable intent. Removing it makes the same received transfer usable.
	require.NoError(t, os.Mkdir(target.BaseName+".hwm", 0o700))
	require.Error(t, r.Install())
	pending, err := target.PendingBootstrap()
	require.NoError(t, err)
	require.Nil(t, pending)
	require.NoError(t, os.Remove(target.BaseName+".hwm"))
	require.NoError(t, r.Install())
	require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
	records := bootstrapRawRecords(t, target.GetSegmentPath(0))
	require.Len(t, records, 1)
	require.Equal(t, "new", records[0].Payload)
}
