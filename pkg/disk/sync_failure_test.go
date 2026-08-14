package disk

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

func TestSyncFailureMakesHandlerTerminalUntilRestart(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 60_000

	handler, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		handler.syncFileFn = nil
		if closeErr := handler.Close(); closeErr != nil {
			t.Errorf("close handler: %v", closeErr)
		}
	})

	handler.syncFileFn = func(*os.File) error {
		return errors.New("injected fsync failure")
	}
	if _, err := handler.AppendMessageSync("orders", 0, &types.Message{Payload: "first"}); err == nil {
		t.Fatal("expected first append to report sync failure")
	}
	sizeAfterFailure := segmentSize(t, handler.GetSegmentPath(0))

	handler.syncFileFn = nil
	if _, err := handler.AppendMessageSync("orders", 0, &types.Message{Payload: "retry"}); err == nil ||
		!strings.Contains(err.Error(), "unavailable until restart") {
		t.Fatalf("sync append after failure = %v, want terminal unavailable error", err)
	}
	if _, err := handler.AppendMessage("orders", 0, &types.Message{Payload: "async-retry"}); err == nil ||
		!strings.Contains(err.Error(), "unavailable until restart") {
		t.Fatalf("async append after failure = %v, want terminal unavailable error", err)
	}
	if got := segmentSize(t, handler.GetSegmentPath(0)); got != sizeAfterFailure {
		t.Fatalf("terminal handler changed segment size: got %d want %d", got, sizeAfterFailure)
	}
}

func segmentSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
