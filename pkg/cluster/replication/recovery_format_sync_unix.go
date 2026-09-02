//go:build !windows

package replication

import (
	"errors"
	"os"
)

func syncRaftDirectory(dataDir string) error {
	directory, err := os.Open(dataDir)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
