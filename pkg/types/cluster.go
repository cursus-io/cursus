package types

type MessageCommand struct {
	Topic         string    `json:"topic"`
	Partition     int       `json:"partition"`
	Messages      []Message `json:"messages"`
	Acks          string    `json:"acks"`
	IsIdempotent  bool      `json:"is_idempotent"`
	SequenceScope string    `json:"sequence_scope,omitempty"` // "global" or "partition"
}

type BatchMessageCommand struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Messages  []Message `json:"messages"`
	Acks      string    `json:"acks"`
}
