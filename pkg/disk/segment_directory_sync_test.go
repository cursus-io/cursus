package disk

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

func TestApplicationSegmentDirectoryEntriesAreSyncedBeforeUse(t *testing.T) {
	originalSync := syncAuthoritativeDirectory
	t.Cleanup(func() { syncAuthoritativeDirectory = originalSync })

	var synced []string
	syncAuthoritativeDirectory = func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return nil
	}

	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.SegmentSize = 256
	cfg.DiskFlushIntervalMS = 60_000
	handler, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	topicDir := filepath.Join(cfg.LogDir, "orders")
	if len(synced) < 2 || synced[0] != topicDir || synced[1] != filepath.Clean(cfg.LogDir) {
		t.Fatalf("initial directory syncs = %v, want topic directory followed by log root", synced)
	}

	if err := handler.WriteBatchSync([]types.DiskMessage{{
		Topic: "orders", Partition: 0, Offset: 0, Payload: strings.Repeat("a", 192),
	}}); err != nil {
		t.Fatal(err)
	}
	beforeRoll := len(synced)
	if err := handler.WriteBatchSync([]types.DiskMessage{{
		Topic: "orders", Partition: 0, Offset: 1, Payload: strings.Repeat("b", 192),
	}}); err != nil {
		t.Fatal(err)
	}
	if len(synced) != beforeRoll+1 || synced[len(synced)-1] != topicDir {
		t.Fatalf("rolled directory syncs = %v, want one additional sync for %s", synced, topicDir)
	}
}

func TestSegmentDirectorySyncFailureMakesWritesUnavailable(t *testing.T) {
	originalSync := syncAuthoritativeDirectory
	t.Cleanup(func() { syncAuthoritativeDirectory = originalSync })

	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.SegmentSize = 256
	cfg.DiskFlushIntervalMS = 60_000
	handler, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	if err := handler.WriteBatchSync([]types.DiskMessage{{
		Topic: "orders", Partition: 0, Offset: 0, Payload: strings.Repeat("a", 192),
	}}); err != nil {
		t.Fatal(err)
	}
	syncAuthoritativeDirectory = func(string) error {
		return errors.New("injected directory sync failure")
	}
	err = handler.WriteBatchSync([]types.DiskMessage{{
		Topic: "orders", Partition: 0, Offset: 1, Payload: strings.Repeat("b", 192),
	}})
	if err == nil || !strings.Contains(err.Error(), "sync rotated segment directory") {
		t.Fatalf("roll error = %v, want directory sync failure", err)
	}

	syncAuthoritativeDirectory = originalSync
	err = handler.WriteDirect("orders", 0, types.Message{Offset: 1, Payload: "retry"})
	if err == nil || !strings.Contains(err.Error(), "unavailable until restart") {
		t.Fatalf("retry error = %v, want terminal write failure", err)
	}
}

func TestSegmentDirectorySyncFailureRejectsHandlerCreation(t *testing.T) {
	originalSync := syncAuthoritativeDirectory
	t.Cleanup(func() { syncAuthoritativeDirectory = originalSync })
	syncAuthoritativeDirectory = func(string) error {
		return errors.New("injected initial directory sync failure")
	}

	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskFlushIntervalMS = 60_000
	handler, err := NewDiskHandler(cfg, "orders", 0)
	if handler != nil || err == nil || !strings.Contains(err.Error(), "sync partition files directory") {
		t.Fatalf("NewDiskHandler = (%v, %v), want no handler and directory sync failure", handler, err)
	}
}
