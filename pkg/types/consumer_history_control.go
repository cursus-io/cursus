package types

import (
	"bytes"
	"fmt"
)

// IsEpochSequencedConsumerControl identifies the protocol-11 control format.
// Its sequence is a fenced coordinator epoch, not a dense message sequence.
// Callers must additionally require the internal consumer topic and, for new
// publication, the durable cluster gate and current transaction route.
func IsEpochSequencedConsumerControl(m Message) bool {
	if m.TransactionalID == "" || m.ControlBatchCoordinatorEpoch <= 0 || m.Epoch < 0 ||
		m.ProducerID != fmt.Sprintf("txn-history:%s:%s", m.TransactionalID, m.TransactionMarker) ||
		m.SeqNum != uint64(m.ControlBatchCoordinatorEpoch) || m.Payload != TransactionControlMarkerPayload ||
		m.Key != "" || m.ControlBatchType != ControlBatchTransaction || m.ControlBatchVersion != ControlBatchVersionCursusV2 {
		return false
	}
	state := TransactionStateCommitted
	if m.TransactionMarker == TransactionMarkerAbort {
		state = TransactionStateAborted
	}
	if m.TransactionState != state {
		return false
	}
	key, value, err := TransactionMarkerControlBytes(m.TransactionMarker, m.ControlBatchCoordinatorEpoch)
	return err == nil && bytes.Equal(key, m.ControlBatchKey) && bytes.Equal(value, m.ControlBatchValue)
}
