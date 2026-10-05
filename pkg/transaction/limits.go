package transaction

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
)

const (
	defaultMaxTransactions       = 100000
	defaultMaxTransactionRecords = 10000
	defaultMaxTransactionBytes   = int64(64 * 1024 * 1024)
	defaultMaxTransactionOffsets = 10000
)

// Limits bounds retained transaction identities and the dynamic state held by
// one transaction. Fixed struct overhead is bounded by the count limits.
type Limits struct {
	MaxTransactions int
	MaxRecords      int
	MaxBytes        int64
	MaxOffsets      int
}

func DefaultLimits() Limits {
	return Limits{
		MaxTransactions: defaultMaxTransactions,
		MaxRecords:      defaultMaxTransactionRecords,
		MaxBytes:        defaultMaxTransactionBytes,
		MaxOffsets:      defaultMaxTransactionOffsets,
	}
}

func normalizeLimits(limits Limits) Limits {
	defaults := DefaultLimits()
	if limits.MaxTransactions <= 0 {
		limits.MaxTransactions = defaults.MaxTransactions
	}
	if limits.MaxRecords <= 0 {
		limits.MaxRecords = defaults.MaxRecords
	}
	if limits.MaxBytes <= 0 {
		limits.MaxBytes = defaults.MaxBytes
	}
	if limits.MaxOffsets <= 0 {
		limits.MaxOffsets = defaults.MaxOffsets
	}
	return limits
}

func (m *Manager) validateTransactionLimits(tx *Transaction) error {
	if tx == nil {
		return nil
	}
	records := transactionRecordCount(len(tx.Messages), len(tx.Streams), len(tx.RequestAssignments))
	if records > m.limits.MaxRecords {
		return fmt.Errorf("transaction capacity exceeded: records=%d max_transaction_records=%d", records, m.limits.MaxRecords)
	}
	if len(tx.Offsets) > m.limits.MaxOffsets {
		return fmt.Errorf("transaction capacity exceeded: offsets=%d max_transaction_offsets=%d", len(tx.Offsets), m.limits.MaxOffsets)
	}
	if bytes := transactionDynamicBytes(tx.Messages, tx.Streams, tx.Offsets, tx.Participants, tx.RequestAssignments); bytes > m.limits.MaxBytes {
		return fmt.Errorf("transaction capacity exceeded: bytes=%d max_transaction_bytes=%d", bytes, m.limits.MaxBytes)
	}
	return nil
}

func (m *Manager) validateSnapshotLimits(snap *Snapshot) error {
	if snap == nil {
		return nil
	}
	tx := transactionFromSnapshot(snap)
	return m.validateTransactionLimits(tx)
}

func transactionRecordCount(messages, streams, assignments int) int {
	if streams > assignments {
		assignments = streams
	}
	return messages + assignments
}

func transactionDynamicBytes(messages []MessageOperation, streams []StreamOperation, offsets []OffsetOperation, participants []Participant, assignments map[string]RequestAssignment) int64 {
	var total int64
	for _, op := range messages {
		total += int64(len(op.Topic)) + messageDynamicBytes(op.Message)
	}
	for _, op := range streams {
		total += int64(len(op.Topic)+len(op.Key)) + messageDynamicBytes(op.Message)
	}
	for _, op := range offsets {
		total += int64(len(op.Topic) + len(op.Group) + len(op.Member))
	}
	for _, participant := range participants {
		total += int64(len(participant.Topic))
	}
	for key, assignment := range assignments {
		total += int64(len(key) + len(assignment.Topic) + len(assignment.Fingerprint))
	}
	return total
}

func messageDynamicBytes(message types.Message) int64 {
	return int64(
		len(message.Topic) + len(message.ProducerID) + len(message.Payload) + len(message.Key) +
			len(message.EventType) + len(message.Metadata) + len(message.EventID) + len(message.PayloadDigest) +
			len(message.TransactionalID) + len(message.TransactionState) + len(message.TransactionMarker) +
			len(message.ControlBatchType) + len(message.ControlBatchKey) + len(message.ControlBatchValue),
	)
}
