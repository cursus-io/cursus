package topic

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func writePartitionCheckpoint(path string, value any, strict bool, syncDir func(string) error) error {
	if path == "" {
		return fmt.Errorf("partition checkpoint path is empty")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(data, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = replaceCheckpointFile(tmp, path); err != nil {
		return err
	}
	if strict {
		if syncDir == nil {
			syncDir = syncPartitionDirectory
		}
		return syncDir(filepath.Dir(path))
	}
	syncParentDir(filepath.Dir(path))
	return nil
}
