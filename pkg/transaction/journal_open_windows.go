//go:build windows

package transaction

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func openJournalForInspection(path string) (*os.File, os.FileInfo, bool, error) {
	handle, err := windows.CreateFile(windows.StringToUTF16Ptr(path), windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if os.IsNotExist(err) {
		return nil, nil, true, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	file := os.NewFile(uintptr(handle), path)
	info, err := file.Stat()
	if err != nil {
		return nil, nil, false, errors.Join(err, file.Close())
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, nil, false, errors.Join(fmt.Errorf("transaction journal must be a regular file"), file.Close())
	}
	return file, info, false, nil
}
