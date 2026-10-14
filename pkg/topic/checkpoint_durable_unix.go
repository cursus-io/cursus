//go:build !windows

package topic

import (
	"errors"
	"os"
)

func syncPartitionDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
