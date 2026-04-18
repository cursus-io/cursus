package types

import "fmt"

// Message represents a single message
type Message struct {
	Offset     uint64
	ProducerID string
	SeqNum     uint64
	Payload    string
	Key        string // optional: partition routing key
	Epoch      int64

	EventType        string
	SchemaVersion    uint32
	AggregateVersion uint64
	Metadata         string

	RetryCount int
	Retry      bool
}

func (m Message) String() string {
	return fmt.Sprintf("Message { ID: %s-%d, Payload:%s, Offset:%d, Key:%s, Epoch:%d, RetryCount:%d }",
		m.ProducerID, m.SeqNum, m.Payload, m.Offset, m.Key, m.Epoch, m.RetryCount)
}

type Batch struct {
	Topic        string
	Partition    int
	BatchStart   uint64
	BatchEnd     uint64
	Acks         string // "0", "1", "-1(=all)"
	IsIdempotent bool
	Messages     []Message
}

// DiskMessage represents a message stored on disk with full metadata
type DiskMessage struct {
	Topic      string
	Partition  int32
	Offset     uint64
	ProducerID string
	SeqNum     uint64
	Epoch      int64
	Payload    string

	EventType        string
	SchemaVersion    uint32
	AggregateVersion uint64
	Metadata         string
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
