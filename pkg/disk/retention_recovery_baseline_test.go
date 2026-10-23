package disk

import (
	"os"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// This preserves the audited predicate explicitly, even after production
// retention stops using mtime. All files are private fixtures; no Chtimes.
func TestRetentionRecoveryBaselineMtimeDrift(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	records := []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 0, Payload: "same"}}
	require.NoError(t, source.WriteBatchSync(records))
	require.NoError(t, source.RollSegmentAt(1))
	original, err := os.Stat(source.GetSegmentPath(0))
	require.NoError(t, err)
	// Advance real filesystem creation time, not an artificial mtime mutation.
	time.Sleep(100 * time.Millisecond)
	incremental, _ := bootstrapTestHandler(t, 1)
	messages, err := source.ReadMessages(0, 1)
	require.NoError(t, err)
	require.NoError(t, incremental.AppendMessageWithOffset("orders", 1, &messages[0]))
	require.NoError(t, incremental.RollSegmentAt(1))
	export := retainedExportTest(t, source, 0, 0, 1)
	bootstrap, _ := bootstrapTestHandler(t, 1)
	receiver := bootstrapTestReceive(t, bootstrap, export)
	require.NoError(t, receiver.Install())
	require.NoError(t, bootstrap.CompleteBootstrap(export.Manifest().TransferID))
	require.NoError(t, bootstrap.RollSegmentAt(1))
	cutoff := original.ModTime().Add(time.Hour + time.Nanosecond)
	for name, handler := range map[string]*DiskHandler{"original": source, "incremental": incremental, "bootstrap": bootstrap} {
		info, err := os.Stat(handler.GetSegmentPath(0))
		require.NoError(t, err)
		expired := cutoff.Sub(info.ModTime()) > time.Hour
		require.Equal(t, name == "original", expired)
		t.Logf("baseline path=%s bytes=%d mtime=%s cutoff=%s expired=%t", name, info.Size(), info.ModTime().Format(time.RFC3339Nano), cutoff.Format(time.RFC3339Nano), expired)
	}
}
