package disk

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestCheckedFilesystemBytesRejectsOverflow(t *testing.T) {
	if _, err := checkedFilesystemBytes(math.MaxUint64, 2); err == nil {
		t.Fatal("expected overflowing filesystem capacity to be rejected")
	}
	got, err := checkedFilesystemBytes(1024, 4096)
	if err != nil {
		t.Fatalf("valid filesystem capacity failed: %v", err)
	}
	if got != 4<<20 {
		t.Fatalf("filesystem capacity = %d, want %d", got, 4<<20)
	}
}

func TestDiskHeadroomGuardReservesAgainstByteAndPercentThresholds(t *testing.T) {
	guard := &diskHeadroomGuard{
		path:           t.TempDir(),
		minFreeBytes:   100,
		minFreePercent: 20,
		checkedAt:      time.Now(),
		free:           400,
		total:          1000,
	}

	if _, err := guard.reserve(150, false); err != nil {
		t.Fatalf("first reservation failed: %v", err)
	}
	if _, err := guard.reserve(50, false); err != nil {
		t.Fatalf("reservation at threshold failed: %v", err)
	}
	if _, err := guard.reserve(1, false); err == nil || !strings.Contains(err.Error(), "insufficient filesystem headroom") {
		t.Fatalf("reservation below threshold error = %v", err)
	}
}

func TestDiskHeadroomGuardUsesStricterByteThreshold(t *testing.T) {
	guard := &diskHeadroomGuard{
		path:           t.TempDir(),
		minFreeBytes:   300,
		minFreePercent: 10,
		checkedAt:      time.Now(),
		free:           500,
		total:          1000,
	}

	if _, err := guard.reserve(201, false); err == nil {
		t.Fatal("expected byte reserve to reject write")
	}
}
