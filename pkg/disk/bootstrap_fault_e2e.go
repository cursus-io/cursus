//go:build e2e_faults

package disk

import (
	"os"
	"path/filepath"
)

// The dedicated fault image can exit after durable intent and a replacement,
// before derived-state rebuild or HWM publication. Production excludes this.
func bootstrapProcessCrashFault(base string) {
	recoveryProcessCrashFault(base, "bootstrap-install-crash", 86)
}

func truncateProcessCrashFault(base string) {
	recoveryProcessCrashFault(base, "replica-truncate-crash", 87)
}

func recoveryProcessCrashFault(base, operation string, code int) {
	if os.Getenv("CURSUS_E2E_FAULTS") != "1" {
		return
	}
	name := filepath.Base(filepath.Dir(base)) + "-" + filepath.Base(base)
	path := filepath.Join("/run/cursus-e2e-faults", operation, name)
	if os.Remove(path) == nil {
		os.Exit(code)
	}
}
