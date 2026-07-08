//go:build !windows

package topic

import "os"

func replaceCheckpointFile(tmp, path string) error {
	return os.Rename(tmp, path)
}
