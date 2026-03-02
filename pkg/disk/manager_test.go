package disk_test

import (
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func TestDiskManager_GetHandler_CreatesHandler(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		LogDir:              tmpDir,
		LingerMS:            100,
		DiskFlushBatchSize:  10,
		DiskFlushIntervalMS: 500,
		DiskWriteTimeoutMS:  500,
		SegmentRollTimeMS:   60000,
		ChannelBufferSize:   100,
	}

	dm := disk.NewDiskManager(cfg)
	topic := "testTopic"
	partition := 0

	dh, err := dm.GetHandler(topic, partition)
	if err != nil {
		t.Fatalf("GetHandler failed: %v", err)
	}
	defer func() { _ = dh.Close() }()

	if dh == nil {
		t.Fatalf("expected DiskHandler, got nil")
	}

	dh2, err := dm.GetHandler(topic, partition)
	if err != nil {
		t.Fatalf("GetHandler second call failed: %v", err)
	}
	if dh != dh2 {
		t.Fatalf("expected same DiskHandler instance, got different")
	}
}

func TestDiskManager_CloseAllHandlers(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		LogDir:              tmpDir,
		LingerMS:            50,
		DiskFlushBatchSize:  10,
		DiskFlushIntervalMS: 50,
		DiskWriteTimeoutMS:  500,
		SegmentRollTimeMS:   60000,
		ChannelBufferSize:   100,
	}

	dm := disk.NewDiskManager(cfg)
	topics := []string{"topic1", "topic2"}
	handlers := []types.StorageHandler{}

	for i, topic := range topics {
		dh, err := dm.GetHandler(topic, i)
		if err != nil {
			t.Fatalf("GetHandler failed: %v", err)
		}
		if dh == nil {
			t.Fatalf("expected DiskHandler for %s, got nil", topic)
		}
		handlers = append(handlers, dh)
	}

	defer func() {
		for _, dh := range handlers {
			if err := dh.Close(); err != nil {
				util.Warn("Failed to close DiskHandler: %v", err)
			}
		}
	}()

	dm.CloseAllHandlers()

	for i := range topics {
		filePath := handlers[i].GetSegmentPath(0)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Fatalf("expected segment file %s to exist", filePath)
		}
	}
}
