//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly && !windows

package disk

import (
	"fmt"
	"os"
	"runtime"
)

func tryLockStorageFile(*os.File) error {
	return fmt.Errorf("exclusive storage locks are unsupported on %s", runtime.GOOS)
}
