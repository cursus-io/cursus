// Package aggregate provides fail-closed retained-log proof operations.
package aggregate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/cursus-io/cursus/pkg/topic"
)

type Status string

const (
	StatusComplete     Status = "complete"
	StatusNotFound     Status = "not_found"
	StatusRetentionGap Status = "retention_gap"
	StatusSequenceGap  Status = "sequence_gap"
	StatusConflict     Status = "identity_conflict"
	StatusUnavailable  Status = "proof_unavailable"
)

type Proof struct {
	Topic                            string `json:"topic"`
	Partition                        int    `json:"partition"`
	MatchID                          string `json:"match_id"`
	EarliestRetainedSequence         uint64 `json:"earliest_retained_sequence,omitempty"`
	LatestRetainedSequence           uint64 `json:"latest_retained_sequence,omitempty"`
	FirstOffset                      uint64 `json:"first_offset,omitempty"`
	LastOffset                       uint64 `json:"last_offset,omitempty"`
	EventCount                       uint64 `json:"event_count"`
	FirstEventID                     string `json:"first_event_id,omitempty"`
	LastEventID                      string `json:"last_event_id,omitempty"`
	SequenceComplete                 bool   `json:"sequence_complete"`
	RetentionCompleteFromSequenceOne bool   `json:"retention_complete_from_sequence_one"`
	ProofHWM                         uint64 `json:"proof_hwm"`
	Status                           Status `json:"status"`
	Diagnostic                       string `json:"diagnostic,omitempty"`
}

type Event struct {
	Offset        uint64 `json:"offset"`
	MatchID       string `json:"match_id"`
	EventSequence uint64 `json:"event_sequence"`
	EventID       string `json:"event_id"`
	EventType     string `json:"event_type"`
	Payload       string `json:"payload"`
}

func Digest(payload string) string {
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// Inspect scans only the committed retained range captured by the initial HWM.
// The log is authoritative: no sidecar can turn a gap into a successful proof.
func Inspect(t *topic.Topic, partition int, matchID string, expectedLast *uint64) (Proof, []Event) {
	proof := Proof{Topic: t.Name, Partition: partition, MatchID: matchID, Status: StatusUnavailable}
	p, err := t.GetPartition(partition)
	if err != nil {
		proof.Diagnostic = err.Error()
		return proof, nil
	}
	// A proof is a read barrier: it only certifies records that have crossed
	// the storage flush boundary and are visible to committed readers.
	p.FlushDisk()
	r := p.OffsetRange()
	proof.ProofHWM = p.LastStableOffset()
	seenIDs := make(map[string]uint64)
	var events []Event
	for offset := r.Earliest; offset < proof.ProofHWM; {
		messages, readErr := p.ReadCommitted(offset, 256)
		if readErr != nil || len(messages) == 0 {
			if readErr != nil {
				proof.Diagnostic = readErr.Error()
			} else {
				proof.Diagnostic = "committed log scan made no progress"
			}
			return proof, nil
		}
		for _, msg := range messages {
			if msg.Offset >= proof.ProofHWM {
				break
			}
			if msg.Key != matchID {
				continue
			}
			if msg.AggregateVersion == 0 || msg.EventID == "" || msg.PayloadDigest == "" || msg.PayloadDigest != Digest(msg.Payload) {
				proof.Status, proof.Diagnostic = StatusConflict, "missing or invalid aggregate replay identity"
				return proof, nil
			}
			if prior, exists := seenIDs[msg.EventID]; exists {
				proof.Status, proof.Diagnostic = StatusConflict, fmt.Sprintf("event_id reused at sequences %d and %d", prior, msg.AggregateVersion)
				return proof, nil
			}
			seenIDs[msg.EventID] = msg.AggregateVersion
			if proof.EventCount == 0 {
				proof.EarliestRetainedSequence, proof.FirstOffset, proof.FirstEventID = msg.AggregateVersion, msg.Offset, msg.EventID
			} else if msg.AggregateVersion != proof.LatestRetainedSequence+1 {
				proof.Status, proof.Diagnostic = StatusSequenceGap, fmt.Sprintf("expected sequence %d, got %d", proof.LatestRetainedSequence+1, msg.AggregateVersion)
				return proof, nil
			}
			proof.EventCount++
			proof.LatestRetainedSequence, proof.LastOffset, proof.LastEventID = msg.AggregateVersion, msg.Offset, msg.EventID
			events = append(events, Event{Offset: msg.Offset, MatchID: matchID, EventSequence: msg.AggregateVersion, EventID: msg.EventID, EventType: msg.EventType, Payload: msg.Payload})
		}
		next := messages[len(messages)-1].Offset + 1
		if next <= offset {
			proof.Diagnostic = "committed log scan did not advance"
			return proof, nil
		}
		offset = next
	}
	if proof.EventCount == 0 {
		proof.Status = StatusNotFound
		return proof, nil
	}
	proof.SequenceComplete = proof.EarliestRetainedSequence == 1
	proof.RetentionCompleteFromSequenceOne = proof.SequenceComplete
	if !proof.SequenceComplete {
		proof.Status, proof.Diagnostic = StatusRetentionGap, fmt.Sprintf("first retained sequence is %d", proof.EarliestRetainedSequence)
		return proof, nil
	}
	if expectedLast != nil && proof.LatestRetainedSequence != *expectedLast {
		proof.Status, proof.Diagnostic = StatusUnavailable, fmt.Sprintf("expected last sequence %d, got %d", *expectedLast, proof.LatestRetainedSequence)
		return proof, nil
	}
	proof.Status = StatusComplete
	return proof, events
}
