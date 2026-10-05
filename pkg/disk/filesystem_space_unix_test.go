//go:build !windows

package disk

import (
	"math"
	"testing"
)

func TestCheckedFilesystemStatValueHandlesSignedAndUnsignedFields(t *testing.T) {
	if _, err := checkedFilesystemStatValue("block count", int64(-1)); err == nil {
		t.Fatal("expected a negative signed filesystem value to be rejected")
	}

	got, err := checkedFilesystemStatValue("block count", uint64(math.MaxUint64))
	if err != nil {
		t.Fatalf("unsigned filesystem value failed: %v", err)
	}
	if got != math.MaxUint64 {
		t.Fatalf("filesystem value = %d, want %d", got, uint64(math.MaxUint64))
	}
}
