//go:build !windows

package eventsource

import (
	"errors"
	"fmt"
	"os"
)

func syncSnapshotDirectory(path string) error {
	// #nosec G304 -- path belongs to the broker-managed event snapshot tree.
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open snapshot directory for sync: %w", err)
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		syncErr = fmt.Errorf("sync snapshot directory: %w", syncErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close snapshot directory: %w", closeErr)
	}
	return errors.Join(syncErr, closeErr)
}
