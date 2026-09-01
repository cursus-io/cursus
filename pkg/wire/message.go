package wire

import "fmt"

const (
	TransactionStateNone      = ""
	TransactionStateOpen      = "open"
	TransactionStateCommitted = "committed"
	TransactionStateAborted   = "aborted"

	TransactionMarkerNone   = ""
	TransactionMarkerCommit = "commit"
	TransactionMarkerAbort  = "abort"

	ControlBatchNone            = ""
	ControlBatchTransaction     = "transaction"
	ControlBatchVersionCursusV2 = 2
)

// Message is the canonical broker and Go SDK record schema.
type Message struct {
	Topic     string
	Partition int
	Offset    uint64
	Timestamp int64

	ProducerID string
	SeqNum     uint64
	Payload    string
	Key        string
	Epoch      int64

	EventType        string
	SchemaVersion    uint32
	AggregateVersion uint64
	Metadata         string

	TransactionalID              string
	TransactionState             string
	TransactionMarker            string
	ControlBatchType             string
	ControlBatchVersion          int16
	ControlBatchCoordinatorEpoch int64
	ControlBatchKey              []byte
	ControlBatchValue            []byte

	RetryCount int
	Retry      bool
}

func (m Message) String() string {
	return fmt.Sprintf("Message { ID: %s-%d, Payload:%s, Offset:%d, Key:%s, Epoch:%d, RetryCount:%d, EventType:%s, AggregateVersion:%d }",
		m.ProducerID, m.SeqNum, m.Payload, m.Offset, m.Key, m.Epoch, m.RetryCount, m.EventType, m.AggregateVersion)
}

type Batch struct {
	Topic        string
	Partition    int
	BatchStart   uint64
	BatchEnd     uint64
	Acks         string
	IsIdempotent bool
	Messages     []Message
}
