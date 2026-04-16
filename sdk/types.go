package sdk

import "fmt"

// Message represents a single message
type Message struct {
	Offset     uint64
	ProducerID string
	SeqNum     uint64
	Payload    string
	Key        string // optional: partition routing key
	Epoch      int64

	RetryCount int
	Retry      bool
}

func (m Message) String() string {
	return fmt.Sprintf("Message { ID: %s-%d, Payload:%s, Offset:%d, Key:%s, Epoch:%d, RetryCount:%d }",
		m.ProducerID, m.SeqNum, m.Payload, m.Offset, m.Key, m.Epoch, m.RetryCount)
}

// AckResponse represents the broker's response to a produce request
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
