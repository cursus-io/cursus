package disk

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/observation"
	"github.com/stretchr/testify/require"
)

func TestRuntimeObservationDistinguishesLowHeadroomFromQueryFailure(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 100, 0
	dm := NewDiskManager(cfg)
	free := uint64(50)
	var queryErr error
	dm.headroom.query = func(string) (uint64, uint64, error) { return free, 1000, queryErr }

	// Known low capacity still blocks readiness and admission, but is observable.
	require.ErrorIs(t, dm.Ready(), errInsufficientHeadroom)
	_, err := dm.headroom.reserve(1, false)
	require.ErrorIs(t, err, errInsufficientHeadroom)
	snapshot := dm.RuntimeSnapshot()
	require.Zero(t, snapshot.StatFailures)
	require.EqualValues(t, 50, snapshot.FilesystemFreeBytes)
	require.EqualValues(t, 1000, snapshot.FilesystemTotalBytes)
	require.False(t, snapshot.FilesystemHeadroomOK)

	// Failed queries cannot certify capacity, even if the query returns values.
	queryErr = errors.New("statfs unavailable")
	require.ErrorIs(t, dm.Ready(), queryErr)
	require.Equal(t, 1, dm.RuntimeSnapshot().StatFailures)

	// Recovery restores both admission and measured healthy capacity.
	queryErr, free = nil, 500
	require.NoError(t, dm.Ready())
	_, err = dm.headroom.reserve(1, false)
	require.NoError(t, err)
	snapshot = dm.RuntimeSnapshot()
	require.Zero(t, snapshot.StatFailures)
	require.True(t, snapshot.FilesystemHeadroomOK)
	require.EqualValues(t, 500, snapshot.FilesystemFreeBytes)
}

func TestRuntimeObservationRejectsMissingSegmentMeasurements(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 0, 0
	dm := NewDiskManager(cfg)
	dm.handlers["missing"] = &DiskHandler{BaseName: filepath.Join(cfg.LogDir, "missing")}
	snapshot := dm.RuntimeSnapshot()
	require.Equal(t, 2, snapshot.StatFailures, "missing log and index must invalidate partial byte accounting")
	var unavailable *DiskManager
	require.Positive(t, unavailable.RuntimeSnapshot().StatFailures)
}

func TestDiagnosticWorkersBoundFilesystemAndDiskLockWaits(t *testing.T) {
	for _, mode := range []string{"statfs", "headroom_lock", "manager_lock", "fsync_handler_lock"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 0, 0
			dm := NewDiskManager(cfg)
			var unblock func()
			switch mode {
			case "statfs":
				release := make(chan struct{})
				dm.headroom.query = func(string) (uint64, uint64, error) { <-release; return 1024, 2048, nil }
				unblock = func() { close(release) }
			case "headroom_lock":
				dm.headroom.mu.Lock()
				unblock = dm.headroom.mu.Unlock
			case "manager_lock":
				dm.mu.Lock()
				unblock = dm.mu.Unlock
			case "fsync_handler_lock":
				handler := &DiskHandler{done: make(chan struct{})}
				dm.handlers["blocked"] = handler
				handler.mu.Lock()
				unblock = handler.mu.Unlock
			}
			w := observation.New[RuntimeSnapshot](10*time.Millisecond, 30*time.Millisecond, 50*time.Millisecond)
			t.Cleanup(w.Close)
			var calls atomic.Int32
			w.Start(func(context.Context) (RuntimeSnapshot, error) {
				calls.Add(1)
				if mode != "fsync_handler_lock" {
					if err := dm.Ready(); err != nil {
						return RuntimeSnapshot{}, err
					}
				}
				return dm.RuntimeSnapshot(), nil
			})
			require.Eventually(t, func() bool { return w.Snapshot().TimedOut }, time.Second, time.Millisecond)
			until := time.Now().Add(100 * time.Millisecond)
			for time.Now().Before(until) {
				require.False(t, w.Snapshot().Valid())
				time.Sleep(time.Millisecond)
			}
			require.EqualValues(t, 1, calls.Load())
			w.Close() // Must not join the storage lock or OS call.
			unblock()
			select {
			case <-w.Done():
			case <-time.After(time.Second):
				t.Fatal("worker did not exit after storage recovered")
			}
			require.EqualValues(t, 1, calls.Load())
		})
	}
}
