package util

import "testing"

func TestSafeIntToUint64(t *testing.T) {
	if value, ok := SafeIntToUint64(-1); ok || value != 0 {
		t.Fatalf("negative conversion = (%d, %t), want (0, false)", value, ok)
	}
	if value, ok := SafeIntToUint64(42); !ok || value != 42 {
		t.Fatalf("positive conversion = (%d, %t), want (42, true)", value, ok)
	}
}
