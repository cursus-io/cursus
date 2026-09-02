package e2e

import (
	"fmt"
	"net/http"
	"time"
)

const (
	healthCheckRetries  = 30
	healthCheckInterval = 1 * time.Second
)

// CheckBrokerHealth verifies broker is ready (assumes already started by Makefile)
func CheckBrokerHealth(healthCheckURLs []string) error {
	for _, url := range healthCheckURLs {
		ready := false
		for i := 0; i < healthCheckRetries; i++ {
			// #nosec G107 -- URLs are local broker endpoints supplied by the E2E fixture.
			resp, err := http.Get(url)
			if err == nil && resp.StatusCode == http.StatusOK {
				_ = resp.Body.Close()
				ready = true
				break
			}
			if resp != nil {
				_ = resp.Body.Close()
			}
			time.Sleep(healthCheckInterval)
		}
		if !ready {
			return fmt.Errorf("node at %s not ready after %d attempts", url, healthCheckRetries)
		}
	}
	return nil
}

func (bc *BrokerClient) getPrimaryAddr() (string, error) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if len(bc.addrs) == 0 {
		return "", fmt.Errorf("broker address list is empty")
	}
	return bc.addrs[0], nil
}
