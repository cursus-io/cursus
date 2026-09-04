package e2e_cluster

import (
	"fmt"
	"testing"
	"time"
)

const (
	clusterPollInterval = 250 * time.Millisecond
	clusterReadyTimeout = 30 * time.Second
)

// eventually waits for a cluster-visible condition rather than baking an
// election or replication duration into a test. The condition detail is kept
// in timeout errors for diagnosis.
func eventually(t *testing.T, description string, timeout time.Duration, condition func() (bool, string, error)) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastDetail string
	var lastErr error
	for {
		ok, detail, err := condition()
		if ok {
			return nil
		}
		if detail != "" {
			lastDetail = detail
		}
		if err != nil {
			lastErr = err
		}
		if time.Now().Add(clusterPollInterval).After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("timed out waiting for %s: %s: %w", description, lastDetail, lastErr)
			}
			return fmt.Errorf("timed out waiting for %s: %s", description, lastDetail)
		}
		time.Sleep(clusterPollInterval)
	}
}

// eventuallyStable requires the condition to remain true for a continuous
// window. A transient regression resets the window instead of allowing a
// single ready observation to advance a stateful E2E scenario.
func eventuallyStable(t *testing.T, description string, timeout, stableFor time.Duration, condition func() (bool, string, error)) error {
	t.Helper()
	if stableFor <= 0 {
		return eventually(t, description, timeout, condition)
	}

	deadline := time.Now().Add(timeout)
	var readySince time.Time
	var lastDetail string
	var lastErr error
	for {
		ok, detail, err := condition()
		now := time.Now()
		if detail != "" {
			lastDetail = detail
		}
		if err != nil {
			lastErr = err
		}
		if ok {
			if readySince.IsZero() {
				readySince = now
			}
			if now.Sub(readySince) >= stableFor {
				return nil
			}
		} else {
			readySince = time.Time{}
		}
		if now.Add(clusterPollInterval).After(deadline) {
			windowDetail := fmt.Sprintf("%s (required stable window %s)", lastDetail, stableFor)
			if lastErr != nil {
				return fmt.Errorf("timed out waiting for %s: %s: %w", description, windowDetail, lastErr)
			}
			return fmt.Errorf("timed out waiting for %s: %s", description, windowDetail)
		}
		time.Sleep(clusterPollInterval)
	}
}
