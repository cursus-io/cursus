package stream

import (
	"testing"
	"time"
)

func TestActiveCountUsesCurrentRegistry(t *testing.T) {
	manager := NewStreamManager(10, time.Minute, time.Minute)
	if got := manager.ActiveCount(); got != 0 {
		t.Fatalf("empty ActiveCount = %d", got)
	}
	manager.mu.Lock()
	manager.streams["orders:0:workers"] = &StreamConnection{}
	manager.mu.Unlock()
	if got := manager.ActiveCount(); got != 1 {
		t.Fatalf("ActiveCount = %d, want 1", got)
	}
	var nilManager *StreamManager
	if got := nilManager.ActiveCount(); got != 0 {
		t.Fatalf("nil ActiveCount = %d", got)
	}
}
