package disk

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
)

func TestDiskManagerRuntimeSnapshot(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	manager := NewDiskManager(cfg)
	if _, err := manager.GetHandler("orders", 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.CloseAllHandlers)

	snapshot := manager.RuntimeSnapshot()
	if snapshot.Handlers != 1 || snapshot.Segments != 1 {
		t.Fatalf("runtime snapshot = %+v", snapshot)
	}
	if snapshot.Bytes < 0 || snapshot.StatFailures != 0 {
		t.Fatalf("unexpected storage accounting: %+v", snapshot)
	}
}
