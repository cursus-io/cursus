//go:build windows

package topic

import (
	"errors"
	"fmt"
	"golang.org/x/sys/windows"
)

// Match the existing event-source durable-directory boundary. Do not silently
// accept unsupported/access-denied directory flushes for an authoritative swap.
func syncPartitionDirectory(path string) error {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	h, err := windows.CreateFile(p, windows.GENERIC_READ|windows.GENERIC_WRITE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return fmt.Errorf("open bootstrap directory for sync: %w", err)
	}
	return errors.Join(windows.FlushFileBuffers(h), windows.CloseHandle(h))
}
