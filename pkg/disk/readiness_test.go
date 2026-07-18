package disk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
)

func TestDiskManagerReadyChecksLogDirectory(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = filepath.Join(t.TempDir(), "missing")
	manager := NewDiskManager(cfg)
	if err := manager.Ready(); err == nil {
		t.Fatal("Ready accepted a missing log directory")
	}
	if err := os.MkdirAll(cfg.LogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := manager.Ready(); err != nil {
		t.Fatalf("Ready rejected an available log directory: %v", err)
	}
}

func TestDiskManagerReadyRejectsClosedRegisteredHandler(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	manager := NewDiskManager(cfg)
	handler, err := manager.GetHandler("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Ready(); err == nil {
		t.Fatal("Ready accepted a closed registered handler")
	}
	manager.CloseAllHandlers()
}
