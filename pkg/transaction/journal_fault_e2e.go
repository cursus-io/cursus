//go:build e2e_faults

package transaction

import (
	"os"
	"path/filepath"
)

// Only the dedicated fault build accepts a one-shot marker in its own storage
// directory. The parent test arms it after seeding, so startup cannot consume it.
func injectedJournalPhase(path, phase string) {
	if os.Getenv("CURSUS_E2E_FAULTS") != "1" {
		return
	}
	if os.Remove(filepath.Join(filepath.Dir(path), ".truncate-fault-"+phase)) == nil {
		os.Exit(86)
	}
}
