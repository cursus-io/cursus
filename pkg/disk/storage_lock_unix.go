//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package disk

import (
	"os"

	"golang.org/x/sys/unix"
)

func tryLockStorageFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}
