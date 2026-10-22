//go:build e2e_faults

package coordinator

import (
	"fmt"
	"os"
	"path/filepath"
)

// Only the dedicated fault image can hide a successfully committed tombstone
// acknowledgement. The marker also detects any repeated successful append.
func consumerCleanupAcknowledgementFault(record ConsumerMetadataRecord) error {
	if os.Getenv("CURSUS_E2E_FAULTS") != "1" || record.Type != ConsumerMetadataRecordTombstone {
		return nil
	}
	// Topic is an already validated single-topic lifecycle identity, but keep
	// sentinel paths confined even if a test supplies an invalid record.
	if record.Topic == "" || filepath.Base(record.Topic) != record.Topic || record.Topic == "." || record.Topic == ".." {
		return nil
	}
	path := filepath.Join("/run/cursus-e2e-faults", "consumer-cleanup-ack", record.Topic)
	if err := os.Rename(path, path+".fired"); err == nil {
		return fmt.Errorf("injected lost consumer tombstone acknowledgement after commit")
	}
	if _, err := os.Stat(path + ".fired"); err == nil {
		if err := os.WriteFile(path+".duplicate", nil, 0o600); err != nil {
			return err
		}
	}
	return nil
}
