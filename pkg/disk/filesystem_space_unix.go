//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package disk

import (
	"fmt"

	"golang.org/x/sys/unix"
)

type filesystemStatValue interface {
	~int32 | ~int64 | ~uint32 | ~uint64
}

func checkedFilesystemStatValue[T filesystemStatValue](name string, value T) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("filesystem returned negative %s: %d", name, value)
	}
	// #nosec G115 -- signed variants are checked for negative values above.
	return uint64(value), nil
}

func queryFilesystemSpace(path string) (uint64, uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	availableBlocks, err := checkedFilesystemStatValue("available block count", stat.Bavail)
	if err != nil {
		return 0, 0, err
	}
	totalBlocks, err := checkedFilesystemStatValue("total block count", stat.Blocks)
	if err != nil {
		return 0, 0, err
	}
	blockSize, err := checkedFilesystemStatValue("block size", stat.Bsize)
	if err != nil {
		return 0, 0, err
	}
	if blockSize == 0 {
		return 0, 0, fmt.Errorf("filesystem returned zero block size")
	}
	free, err := checkedFilesystemBytes(availableBlocks, blockSize)
	if err != nil {
		return 0, 0, fmt.Errorf("available filesystem capacity: %w", err)
	}
	total, err := checkedFilesystemBytes(totalBlocks, blockSize)
	if err != nil {
		return 0, 0, fmt.Errorf("total filesystem capacity: %w", err)
	}
	return free, total, nil
}
