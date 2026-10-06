package buildinfo

import (
	"fmt"
	"os"
)

var (
	Version  = "0.2.0"
	Revision = "unknown"
)

const (
	WireProtocolVersion   = "2"
	BrokerProtocolVersion = "5"
	SnapshotFormatVersion = "10"
	RecordFormatVersion   = "CDM5"
)

func VerifyDeploymentContract() error {
	expected := map[string]string{
		"EXPECTED_IMAGE_REVISION":  Revision,
		"EXPECTED_WIRE_PROTOCOL":   WireProtocolVersion,
		"EXPECTED_BROKER_PROTOCOL": BrokerProtocolVersion,
		"EXPECTED_SNAPSHOT_FORMAT": SnapshotFormatVersion,
		"EXPECTED_RECORD_FORMAT":   RecordFormatVersion,
	}
	for name, actual := range expected {
		wanted := os.Getenv(name)
		if wanted == "" {
			return fmt.Errorf("deployment contract is missing %s", name)
		}
		if wanted != actual {
			return fmt.Errorf("deployment contract mismatch for %s: image=%q expected=%q", name, actual, wanted)
		}
	}
	return nil
}
