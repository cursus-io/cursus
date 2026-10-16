//go:build !windows

package topic

import (
	"errors"
	"os"
)

func syncPartitionDirectory(path string) error {
	// #nosec G304 -- writePartitionCheckpoint supplies the parent of an internally constructed partition checkpoint path.
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
