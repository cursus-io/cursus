//go:build e2e_faults

package topic

import (
	"fmt"
	"os"

	"github.com/cursus-io/cursus/pkg/types"
)

// Fail before local page I/O, after a previous page established a cursor.
// No durable log files are altered. Production builds exclude the control.
func consumerCleanupReadFault(cursor types.ReplayCursor) error {
	if os.Getenv("CURSUS_E2E_FAULTS") != "1" || !cursor.Initialized {
		return nil
	}
	// Large-record scenarios place the interruption after the exceptional
	// record has been validated and checkpointed, not after its small prefix.
	if _, err := os.Stat("/run/cursus-e2e-faults/consumer-cleanup-after-large"); err == nil && cursor.Position <= 1<<20 {
		return nil
	}
	_, err := os.Stat("/run/cursus-e2e-faults/consumer-cleanup-read")
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("injected transient consumer cleanup page read failure")
}
