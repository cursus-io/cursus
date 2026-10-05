//go:build windows

package disk

import "golang.org/x/sys/windows"

func queryFilesystemSpace(path string) (uint64, uint64, error) {
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}
	var free uint64
	var total uint64
	if err := windows.GetDiskFreeSpaceEx(pathPointer, &free, &total, nil); err != nil {
		return 0, 0, err
	}
	return free, total, nil
}
