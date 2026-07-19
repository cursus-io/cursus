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
