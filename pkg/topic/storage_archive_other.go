//go:build !linux && !windows

package topic

import "fmt"

func archiveTopicDirectoryExclusive(_, _ string) error {
	return fmt.Errorf("exclusive orphan archive is unsupported on this platform")
}
