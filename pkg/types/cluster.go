package types

type MessageCommand struct {
	Topic         string    `json:"topic"`
	Partition     int       `json:"partition"`
	Messages      []Message `json:"messages"`
	Acks          string    `json:"acks"`
	IsIdempotent  bool      `json:"is_idempotent"`
	SequenceScope string    `json:"sequence_scope,omitempty"` // "global" or "partition"
	LeaderID      string    `json:"leader_id,omitempty"`
	LeaderEpoch   int       `json:"leader_epoch,omitempty"`
	CommitHWM     *uint64   `json:"commit_hwm,omitempty"`
}

type BatchMessageCommand struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Messages  []Message `json:"messages"`
	Acks      string    `json:"acks"`
}
