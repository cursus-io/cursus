//go:build windows

package transaction

import (
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

func syncJournalDirectory(path string) error {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return fmt.Errorf("encode transaction journal directory path: %w", err)
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return fmt.Errorf("open transaction journal directory for sync: %w", err)
	}
	flushErr := windows.FlushFileBuffers(handle)
	closeErr := windows.CloseHandle(handle)
	if flushErr != nil {
		flushErr = fmt.Errorf("sync transaction journal directory: %w", flushErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close transaction journal directory: %w", closeErr)
	}
	return errors.Join(flushErr, closeErr)
}
