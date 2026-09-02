package disk

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
)

func TestDiskManagerRuntimeSnapshot(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	manager := NewDiskManager(cfg)
	storage, err := manager.GetHandler("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	handler, ok := storage.(*DiskHandler)
	if !ok {
		t.Fatalf("handler type = %T, want *DiskHandler", storage)
	}
	t.Cleanup(manager.CloseAllHandlers)
	lease, err := handler.segmentReaders.acquire(0, handler.GetSegmentPath(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	lease, err = handler.segmentReaders.acquire(0, handler.GetSegmentPath(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot := manager.RuntimeSnapshot()
	if snapshot.Handlers != 1 || snapshot.Segments != 1 {
		t.Fatalf("runtime snapshot = %+v", snapshot)
	}
	if snapshot.Bytes < 0 || snapshot.StatFailures != 0 {
		t.Fatalf("unexpected storage accounting: %+v", snapshot)
	}
	if snapshot.SegmentCacheEntries != 1 || snapshot.SegmentCacheHits != 1 || snapshot.SegmentCacheMisses != 1 {
		t.Fatalf("unexpected segment cache accounting: %+v", snapshot)
	}
}
