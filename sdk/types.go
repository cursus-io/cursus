package sdk

import (
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

type Message = wire.Message

// PartitionOffsetRange describes broker-reported offsets for one partition.
type PartitionOffsetRange struct {
	Partition int
	Earliest  uint64
	Latest    uint64
	LEO       uint64
	HWM       uint64
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

// PartitionStat holds per-partition benchmark statistics for a producer.
type PartitionStat struct {
	PartitionID int
	BatchCount  int
	AvgDuration time.Duration
}
