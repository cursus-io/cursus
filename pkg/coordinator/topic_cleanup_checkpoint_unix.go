//go:build !windows

package coordinator

import "os"

func replaceCleanupCheckpoint(source, destination string) error {
	return os.Rename(source, destination)
}
