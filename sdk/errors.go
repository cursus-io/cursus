package sdk

import "errors"

var (
	ErrProducerClosed   = errors.New("producer closed")
	ErrConsumerClosed   = errors.New("consumer closed")
	ErrTopicNotFound    = errors.New("topic not found")
	ErrInvalidPartition = errors.New("invalid partition")
	ErrNotLeader        = errors.New("not leader")
)
