package controller

import (
	"math"
	"testing"
)

func TestAdvanceRecoveredProducerEpochRejectsNegativeValues(t *testing.T) {
	if _, err := advanceRecoveredProducerEpoch(7, -1); err == nil {
		t.Fatal("expected a negative recovered producer epoch to be rejected")
	}

	next, err := advanceRecoveredProducerEpoch(7, math.MaxInt64)
	if err != nil {
		t.Fatalf("maximum producer epoch failed: %v", err)
	}
	if want := uint64(math.MaxInt64) + 1; next != want {
		t.Fatalf("next producer epoch = %d, want %d", next, want)
	}
}
