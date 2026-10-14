package topic

import (
	"encoding/json"
	"fmt"
)

// Recovery publication retains the strict directory barrier on every platform.
func (p *Partition) writePartitionCheckpoint(path string, value any, strict bool) error {
	if path == "" {
		return fmt.Errorf("partition checkpoint path is empty")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var syncDir func(string) error
	if strict {
		syncDir = p.checkpointDirectorySync
		if syncDir == nil {
			syncDir = syncPartitionDirectory
		}
	}
	return p.writeCheckpointWithDirectorySync(path, append(data, '\n'), syncDir)
}
