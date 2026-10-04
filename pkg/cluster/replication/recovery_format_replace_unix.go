//go:build !windows

package replication

import "os"

func replaceRaftFormatMarker(source, destination string) error {
	return os.Rename(source, destination)
}
