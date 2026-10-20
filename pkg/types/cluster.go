package types

// ConsumerMetadataRoute is captured by the originating coordinator. Forwarding
// and replication preserve it unchanged through the durable commit boundary.
type ConsumerMetadataRoute struct {
	Group          string                    `json:"group"`
	Owner          string                    `json:"owner"`
	LeaderEpoch    int                       `json:"leader_epoch"`
	LifecycleEpoch uint64                    `json:"lifecycle_epoch"`
	PartitionCount int                       `json:"partition_count"`
	Partition      int                       `json:"partition"`
	Transaction    *ConsumerTransactionRoute `json:"transaction,omitempty"`
}

type ConsumerTransactionRoute struct {
	ID               string `json:"id"`
	Owner            string `json:"owner"`
	CoordinatorEpoch int64  `json:"coordinator_epoch"`
	ProducerEpoch    int64  `json:"producer_epoch"`
	Revision         uint64 `json:"revision"`
}

type MessageCommand struct {
	ConsumerRoute               *ConsumerMetadataRoute `json:"consumer_route,omitempty"`
	Topic                       string                 `json:"topic"`
	Partition                   int                    `json:"partition"`
	Messages                    []Message              `json:"messages"`
	Acks                        string                 `json:"acks"`
	IsIdempotent                bool                   `json:"is_idempotent"`
	SequenceScope               string                 `json:"sequence_scope,omitempty"` // "global" or "partition"
	LeaderID                    string                 `json:"leader_id,omitempty"`
	LeaderEpoch                 int                    `json:"leader_epoch,omitempty"`
	LifecycleEpoch              uint64                 `json:"lifecycle_epoch,omitempty"`
	RequiredTransactionRevision uint64                 `json:"required_transaction_revision,omitempty"`
	CommitHWM                   *uint64                `json:"commit_hwm,omitempty"`
}

type BatchMessageCommand struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Messages  []Message `json:"messages"`
	Acks      string    `json:"acks"`
}
