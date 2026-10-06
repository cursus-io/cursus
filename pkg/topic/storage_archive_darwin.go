//go:build darwin

package topic

import "golang.org/x/sys/unix"

func archiveTopicDirectoryExclusive(source, destination string) error {
	return unix.RenameatxNp(unix.AT_FDCWD, source, unix.AT_FDCWD, destination, unix.RENAME_EXCL)
}
