//go:build e2e_faults

package topic

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/cursus-io/cursus/pkg/coordinator"
)

// Corrupt only a returned test page, never the committed log or its HWM.
// The fixed-path control is absent from production and requires the runtime gate.
func consumerCleanupEvidenceFault(page coordinator.ConsumerCleanupPage, end uint64) (coordinator.ConsumerCleanupPage, error) {
	if os.Getenv("CURSUS_E2E_FAULTS") != "1" {
		return page, nil
	}
	const path = "/run/cursus-e2e-faults/consumer-cleanup-evidence.json"
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return page, nil
	}
	if err != nil {
		return page, err
	}
	var control struct {
		Group, Mode string
		Epoch       uint64
	}
	if err := json.Unmarshal(data, &control); err != nil {
		return page, err
	}
	for i, message := range page.Messages {
		var record coordinator.ConsumerMetadataRecord
		if json.Unmarshal([]byte(message.Payload), &record) != nil || record.Group != control.Group || record.Epoch != control.Epoch || record.Type != coordinator.ConsumerMetadataRecordTombstone {
			continue
		}
		page.Messages = append(page.Messages[:0:0], page.Messages...)
		switch control.Mode {
		case "missing":
			page.Messages = append(page.Messages[:i], page.Messages[i+1:]...)
		case "malformed":
			page.Messages[i].Payload = `{"version":`
		case "conflicting":
			conflict := record
			conflict.Type, conflict.PartitionCount = coordinator.ConsumerMetadataRecordRegistration, 1
			payload, key, encodeErr := coordinator.EncodeConsumerMetadataRecord(conflict)
			if encodeErr != nil {
				return page, encodeErr
			}
			message.Payload, message.Key = string(payload), key
			page.Messages = append(page.Messages, message)
		case "uncommitted":
			page.Messages[i].Offset = end // A leaking reader cannot authorize this record.
		case "obsolete":
			if record.Epoch <= 2 {
				return page, fmt.Errorf("obsolete evidence fixture requires a prior deletion")
			}
			record.Epoch -= 2
			payload, key, encodeErr := coordinator.EncodeConsumerMetadataRecord(record)
			if encodeErr != nil {
				return page, encodeErr
			}
			page.Messages[i].Payload, page.Messages[i].Key = string(payload), key
		case "transactional":
			page.Messages[i].TransactionalID = "uncommitted-cleanup-fixture"
		default:
			return page, fmt.Errorf("unknown cleanup evidence fault")
		}
		if _, err := os.Stat(path + ".observed"); os.IsNotExist(err) {
			proof, err := json.Marshal(map[string]any{"mode": control.Mode, "group": record.Group, "target_epoch": control.Epoch, "offset": message.Offset, "committed_end": end})
			if err != nil {
				return page, err
			}
			if err := os.WriteFile(path+".observed", proof, 0600); err != nil {
				return page, err
			}
		}
		return page, nil
	}
	return page, nil
}
