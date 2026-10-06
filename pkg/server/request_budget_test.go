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

func TestInternalRequestBudgetIsIndependentFromClientBudget(t *testing.T) {
	clientBudget, internalBudget := newBrokerRequestBudgets(2, 128, true)
	if clientBudget.maxRequests != 1 || internalBudget.maxRequests != 1 || clientBudget.maxBytes != 64 || internalBudget.maxBytes != 64 {
		t.Fatalf("unexpected budget split: client=(%d,%d) internal=(%d,%d)", clientBudget.maxRequests, clientBudget.maxBytes, internalBudget.maxRequests, internalBudget.maxBytes)
	}
	releaseClient, err := clientBudget.reserve(context.Background(), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseClient()
	releaseInternal, err := internalBudget.reserve(context.Background(), 64)
	if err != nil {
		t.Fatalf("internal request blocked by exhausted client budget: %v", err)
	}
	releaseInternal()
}

func TestStandaloneRequestBudgetKeepsFullCapacity(t *testing.T) {
	clientBudget, internalBudget := newBrokerRequestBudgets(2, 128, false)
	if internalBudget != nil || clientBudget.maxRequests != 2 || clientBudget.maxBytes != 128 {
		t.Fatalf("unexpected standalone budgets: client=(%d,%d) internal=%v", clientBudget.maxRequests, clientBudget.maxBytes, internalBudget)
	}
}
