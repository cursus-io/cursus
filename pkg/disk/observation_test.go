package disk

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/observation"
	"github.com/stretchr/testify/require"
)

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
