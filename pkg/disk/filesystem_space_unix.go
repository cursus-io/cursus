//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package disk

import "golang.org/x/sys/unix"

func queryFilesystemSpace(path string) (uint64, uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), uint64(stat.Blocks) * uint64(stat.Bsize), nil
}
