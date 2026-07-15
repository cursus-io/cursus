//go:build !windows

package disk

import "os"

func syncDirectory(path string) error {
	// #nosec G304 -- path is the broker-owned segment directory.
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
