package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRequestMemoryBudgetBoundsCountAndBytes(t *testing.T) {
	budget := newRequestMemoryBudget(1, 128)
	release, err := budget.reserve(context.Background(), 96)
	if err != nil {
		t.Fatalf("reserve first request: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := budget.reserve(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked reservation error = %v, want deadline exceeded", err)
	}

	release()
	release()
	secondRelease, err := budget.reserve(context.Background(), 128)
	if err != nil {
		t.Fatalf("reserve after release: %v", err)
	}
	secondRelease()
}

func TestRequestMemoryBudgetRejectsSingleOversizedReservation(t *testing.T) {
	budget := newRequestMemoryBudget(2, 64)
	if _, err := budget.reserve(context.Background(), 65); err == nil {
		t.Fatal("expected oversized reservation to fail")
	}
}
