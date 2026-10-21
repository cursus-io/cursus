//go:build e2e_faults

package topic

import (
	"errors"
	"os"
)

func injectedConsumerCleanupReadError() error {
	if os.Getenv("CURSUS_E2E_FAULTS") == "1" {
		if _, err := os.Stat("/run/cursus-e2e-faults/consumer-cleanup-read-error"); err == nil {
			return errors.New("injected consumer cleanup read failure")
		}
	}
	return nil
}
