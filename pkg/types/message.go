package types

import "github.com/cursus-io/cursus/pkg/wire"

const (
	TransactionStateNone      = wire.TransactionStateNone
	TransactionStateOpen      = wire.TransactionStateOpen
	TransactionStateCommitted = wire.TransactionStateCommitted
	TransactionStateAborted   = wire.TransactionStateAborted

	TransactionMarkerNone   = wire.TransactionMarkerNone
	TransactionMarkerCommit = wire.TransactionMarkerCommit
	TransactionMarkerAbort  = wire.TransactionMarkerAbort

	ControlBatchNone            = wire.ControlBatchNone
	ControlBatchTransaction     = wire.ControlBatchTransaction
	ControlBatchVersionCursusV2 = wire.ControlBatchVersionCursusV2
)

type Message = wire.Message

type Batch = wire.Batch

// DiskMessage represents a message stored on disk with full metadata
type DiskMessage struct {
	Topic      string
	Partition  int32
	Offset     uint64
	ProducerID string
	SeqNum     uint64
	Epoch      int64
	Payload    string
	Key        string

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
}

// AppendResult represents the result of appending a message to storage
type AppendResult struct {
	SegmentIndex int
	Offset       int
}

type AckResponse struct {
	Status        string `json:"status"`
	LastOffset    uint64 `json:"last_offset"`
	ProducerEpoch int64  `json:"producer_epoch"`
	ProducerID    string `json:"producer_id"`
	SeqStart      uint64 `json:"seq_start"`
	SeqEnd        uint64 `json:"seq_end"`
	Leader        string `json:"leader,omitempty"`
	ErrorMsg      string `json:"error,omitempty"`
}
