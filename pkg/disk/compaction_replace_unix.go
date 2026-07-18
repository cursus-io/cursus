//go:build !windows

package disk

import "os"

func replaceCompactedFile(source, destination string) error {
	return os.Rename(source, destination)
}
