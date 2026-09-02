package util

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
)

func EncodeBatchMessages(topic string, partition int, acks string, isIdempotent bool, messages []types.Message) ([]byte, error) {
	return wire.EncodeBatch(wire.Batch{
		Topic: topic, Partition: partition, Acks: acks, IsIdempotent: isIdempotent, Messages: messages,
	})
}

func DecodeBatchMessages(data []byte) (*types.Batch, error) {
	batch, err := wire.DecodeBatch(data)
	if err != nil {
		return nil, fmt.Errorf("decode Wire v2 batch: %w", err)
	}
	return batch, nil
}
