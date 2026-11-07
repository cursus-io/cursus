package e2e_cluster

import (
	"crypto/sha256"
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
)

// This oracle is for one retained, nontransactional, uncompacted partition.
// Broker errors can follow an append: neither errors nor unknown outcomes prove
// absence. Only a successful response creates a mandatory readback obligation.
type writeAttempt struct {
	ID          string        `json:"id"`
	Target      string        `json:"target"`
	Message     types.Message `json:"message"`
	PayloadHash string        `json:"payload_hash"`
	Outcome     string        `json:"outcome"`
	AckOffset   uint64        `json:"ack_offset"`
	Error       string        `json:"error,omitempty"`
}

func writeIdentity(message types.Message) string {
	return fmt.Sprintf("%s/%d/%d", message.ProducerID, message.Epoch, message.SeqNum)
}

func writePayloadHash(payload string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(payload)))
}

func checkWriteHistory(attempts []writeAttempt, records []types.Message) error {
	if len(attempts) == 0 {
		return fmt.Errorf("empty write history")
	}
	intents := make(map[string]writeAttempt)
	acknowledged := make(map[string]uint64)
	for _, attempt := range attempts {
		id := writeIdentity(attempt.Message)
		if attempt.ID != id || attempt.PayloadHash != writePayloadHash(attempt.Message.Payload) {
			return fmt.Errorf("invalid journal identity/hash for %s", attempt.ID)
		}
		if previous, exists := intents[id]; exists && previous.PayloadHash != attempt.PayloadHash {
			return fmt.Errorf("retry changed payload for %s", id)
		}
		intents[id] = attempt
		switch attempt.Outcome {
		case "acknowledged":
			if offset, exists := acknowledged[id]; exists && offset != attempt.AckOffset {
				return fmt.Errorf("retry changed acknowledged offset for %s", id)
			}
			acknowledged[id] = attempt.AckOffset
		case "unknown", "broker_error":
		default:
			return fmt.Errorf("invalid outcome %q for %s", attempt.Outcome, id)
		}
	}
	if len(acknowledged) == 0 {
		return fmt.Errorf("history contains no acknowledged writes")
	}
	seen := make(map[string]bool)
	sequences := make(map[string]uint64)
	for index, record := range records {
		id := writeIdentity(record)
		intent, exists := intents[id]
		if !exists {
			return fmt.Errorf("unexpected record %s at offset %d", id, record.Offset)
		}
		if seen[id] {
			return fmt.Errorf("duplicate record %s", id)
		}
		seen[id] = true
		if record.Offset != uint64(index) {
			return fmt.Errorf("offset collision, gap or reordering: got %d want %d", record.Offset, index)
		}
		if writePayloadHash(record.Payload) != intent.PayloadHash {
			return fmt.Errorf("payload changed for %s", id)
		}
		if offset, acked := acknowledged[id]; acked && offset != record.Offset {
			return fmt.Errorf("acknowledged offset changed for %s: got %d want %d", id, record.Offset, offset)
		}
		producer := fmt.Sprintf("%s/%d", record.ProducerID, record.Epoch)
		if last, exists := sequences[producer]; exists && record.SeqNum <= last {
			return fmt.Errorf("producer sequence reordered for %s", id)
		}
		sequences[producer] = record.SeqNum
	}
	for id := range acknowledged {
		if !seen[id] {
			return fmt.Errorf("acknowledged record missing: %s", id)
		}
	}
	return nil
}
