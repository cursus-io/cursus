//go:build !windows

package topic

import "os"

func installCheckpointFileExclusive(tmp, path string) error {
	return os.Link(tmp, path)
}
