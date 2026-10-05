//go:build windows

package eventsource

import (
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

func syncSnapshotDirectory(path string) error {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return fmt.Errorf("encode snapshot directory path: %w", err)
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
		return fmt.Errorf("open snapshot directory for sync: %w", err)
	}
	flushErr := windows.FlushFileBuffers(handle)
	closeErr := windows.CloseHandle(handle)
	if flushErr != nil {
		flushErr = fmt.Errorf("sync snapshot directory: %w", flushErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close snapshot directory: %w", closeErr)
	}
	return errors.Join(flushErr, closeErr)
}
