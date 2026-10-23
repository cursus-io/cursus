//go:build !windows

package coordinator

import (
	"errors"
	"os"
	"path/filepath"
)

func replaceCleanupCheckpoint(source, destination string) error {
	if err := os.Rename(source, destination); err != nil {
		return err
	}
	// #nosec G304 -- destination is the broker-owned cleanup cache path.
	dir, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
