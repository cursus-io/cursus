package e2e_cluster

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestEventuallyReturnsWhenConditionBecomesReady(t *testing.T) {
	attempts := 0
	err := eventually(t, "test condition", time.Second, func() (bool, string, error) {
		attempts++
		return attempts == 2, "not ready", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestEventuallyIncludesLastError(t *testing.T) {
	err := eventually(t, "test condition", clusterPollInterval, func() (bool, string, error) {
		return false, "broker-1 unavailable", errors.New("connection refused")
	})
	if err == nil || !strings.Contains(err.Error(), "broker-1 unavailable") || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("unexpected timeout error: %v", err)
	}
}

func TestEventuallyStableResetsWindowAfterRegression(t *testing.T) {
	attempts := 0
	err := eventuallyStable(t, "stable test condition", 2*time.Second, 2*clusterPollInterval, func() (bool, string, error) {
		attempts++
		return attempts != 2, "transient regression", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts < 5 {
		t.Fatalf("attempts = %d, want at least 5 after stability window reset", attempts)
	}
}
