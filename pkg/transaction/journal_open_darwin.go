//go:build darwin

package transaction

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func openJournalForInspection(path string) (*os.File, os.FileInfo, bool, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0) // #nosec G304 -- O_NOFOLLOW binds inspection to the opened path entry.
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, true, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, nil, false, errors.Join(err, file.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, nil, false, errors.Join(fmt.Errorf("transaction journal must be a regular file"), file.Close())
	}
	return file, info, false, nil
}
