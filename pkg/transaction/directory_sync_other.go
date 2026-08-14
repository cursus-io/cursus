//go:build !windows

package transaction

import (
	"errors"
	"fmt"
	"os"
)

func syncJournalDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open transaction journal directory for sync: %w", err)
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		syncErr = fmt.Errorf("sync transaction journal directory: %w", syncErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close transaction journal directory: %w", closeErr)
	}
	return errors.Join(syncErr, closeErr)
}
