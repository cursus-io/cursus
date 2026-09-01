package util

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
)

// EncodeMessage is retained only for cluster-internal call sites while their
// transport is moved to Wire v2 frames. Client traffic must use pkg/wire.
func EncodeMessage(topic string, payload string) []byte {
	topicBytes := []byte(topic)
	if len(topicBytes) > math.MaxUint16 {
		return nil
	}
	payloadBytes := []byte(payload)
	if len(topicBytes) > math.MaxInt-2 || len(payloadBytes) > math.MaxInt-2-len(topicBytes) {
		return nil
	}
	data := make([]byte, 2+len(topicBytes)+len(payloadBytes))
	binary.BigEndian.PutUint16(data[:2], uint16(len(topicBytes)))
	copy(data[2:2+len(topicBytes)], topicBytes)
	copy(data[2+len(topicBytes):], payloadBytes)
	return data
}

func DecodeMessage(data []byte) (string, string, error) {
	if len(data) < 2 {
		return "", "", errors.New("data too short")
	}
	topicLength := int(binary.BigEndian.Uint16(data[:2]))
	if topicLength > len(data)-2 {
		return "", "", errors.New("invalid topic length")
	}
	return string(data[2 : 2+topicLength]), string(data[2+topicLength:]), nil
}

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
