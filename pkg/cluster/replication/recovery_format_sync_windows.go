//go:build windows

package replication

import (
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

func syncRaftDirectory(dataDir string) error {
	path, err := windows.UTF16PtrFromString(dataDir)
	if err != nil {
		return fmt.Errorf("encode Raft directory path: %w", err)
	}
	handle, err := windows.CreateFile(
		path,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return fmt.Errorf("open Raft directory for sync: %w", err)
	}
	flushErr := windows.FlushFileBuffers(handle)
	closeErr := windows.CloseHandle(handle)
	return errors.Join(flushErr, closeErr)
}
