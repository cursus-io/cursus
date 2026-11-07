//go:build e2e_faults

package disk

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// armDiskFault arms one storage operation fault in an isolated temporary sentinel directory.
func armDiskFault(t *testing.T, point, key string) string {
	t.Helper()
	root := t.TempDir()
	t.Setenv("CURSUS_E2E_FAULT_ROOT", root)
	t.Setenv("CURSUS_E2E_FAULTS", "1")
	path := filepath.Join(root, point, key)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	return path
}

// TestStorageOperationFailurePreservesAcknowledgedPrefix checks append and sync failures preserve the previously acknowledged prefix after reopen.
func TestStorageOperationFailurePreservesAcknowledgedPrefix(t *testing.T) {
	writers := map[string]func(*DiskHandler, types.Message) error{
		"direct": func(d *DiskHandler, message types.Message) error { return d.WriteDirect("orders", 0, message) },
		"batch": func(d *DiskHandler, message types.Message) error {
			return d.WriteBatchSync([]types.DiskMessage{diskMessageFromMessage("orders", 0, message)})
		},
		"append": func(d *DiskHandler, message types.Message) error {
			_, err := d.AppendMessageSync("orders", 0, &message)
			return err
		},
	}
	for name, write := range writers {
		for _, point := range []string{"disk-append", "disk-sync"} {
			t.Run(name+"/"+point, func(t *testing.T) {
				cfg := config.DefaultConfig()
				cfg.LogDir = t.TempDir()
				cfg.DiskFlushIntervalMS = 60000
				d, err := NewDiskHandler(cfg, "orders", 0)
				require.NoError(t, err)
				defer func() { _ = d.Close() }()
				for i := uint64(0); i < 8; i++ {
					require.NoError(t, write(d, types.Message{Offset: i, ProducerID: "producer", SeqNum: i + 1, Payload: "acknowledged"}))
				}
				path := armDiskFault(t, point, "orders-0-8")
				err = write(d, types.Message{Offset: 8, Payload: "unknown"})
				if point == "disk-append" {
					require.ErrorIs(t, err, syscall.ENOSPC)
				} else {
					require.ErrorIs(t, err, syscall.EIO)
				}
				require.FileExists(t, path+".entered")
				require.ErrorContains(t, write(d, types.Message{Offset: 9, Payload: "forbidden"}), "unavailable until restart")
				require.NoError(t, d.Close())
				reopened, err := NewDiskHandler(cfg, "orders", 0)
				require.NoError(t, err)
				defer func() { require.NoError(t, reopened.Close()) }()
				records, err := reopened.ReadMessages(0, 16)
				require.NoError(t, err)
				require.GreaterOrEqual(t, len(records), 8)
				for i := 0; i < 8; i++ {
					require.Equal(t, uint64(i), records[i].Offset)
					require.Equal(t, "acknowledged", records[i].Payload)
					require.Equal(t, uint64(i+1), records[i].SeqNum)
				}
				for _, record := range records {
					require.NotEqual(t, "forbidden", record.Payload)
				}
			})
		}
	}
}

// TestCompactionRenameFailureRecoversPublishedLog checks failed compaction publication leaves a recoverable acknowledged log.
func TestCompactionRenameFailureRecoversPublishedLog(t *testing.T) {
	cfg := compactionTestConfig(t)
	d, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { _ = d.Close() }()
	appendCompactionMessage(t, d, types.Message{Key: "a", Payload: "old"})
	appendCompactionMessage(t, d, types.Message{Key: "b", Payload: "retained-b"})
	appendCompactionMessage(t, d, types.Message{Key: "a", Payload: "retained-a"})
	rollCompactionSegment(t, d)
	path := armDiskFault(t, "disk-rename", "orders-0-index")
	_, err = d.EnforceCompaction()
	require.ErrorIs(t, err, syscall.EIO)
	require.FileExists(t, path+".entered", "must fail after log replacement and before index replacement")
	require.ErrorContains(t, d.WriteDirect("orders", 0, types.Message{Offset: 3, Payload: "forbidden"}), "unavailable until restart")
	require.NoError(t, d.Close())
	for reopen := 0; reopen < 2; reopen++ {
		restored, err := NewDiskHandler(cfg, "orders", 0)
		require.NoError(t, err)
		records, readErr := restored.ReadMessages(0, 10)
		require.NoError(t, readErr)
		require.Equal(t, []uint64{1, 2}, messageOffsets(records))
		require.Equal(t, "retained-b", records[0].Payload)
		require.Equal(t, "retained-a", records[1].Payload)
		require.Equal(t, uint64(3), restored.GetAbsoluteOffset())
		require.NoError(t, restored.Close(), "restart repair must be idempotent")
	}
}
