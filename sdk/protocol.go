package sdk

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"

	"github.com/cursus-io/cursus/pkg/wire"
)

const MaxMessageSize = wire.MaxFramePayload

// EncodeMessage is a transitional helper for SDK paths not yet moved to the
// shared Wire v2 transport. It is removed with the transport migration.
func EncodeMessage(topic string, payload string) []byte {
	if len(topic) > math.MaxUint16 || len(topic) > MaxMessageSize-2 || len(payload) > MaxMessageSize-2-len(topic) {
		return nil
	}
	encoded := make([]byte, 2+len(topic)+len(payload))
	binary.BigEndian.PutUint16(encoded[:2], uint16(len(topic)))
	copy(encoded[2:], topic)
	copy(encoded[2+len(topic):], payload)
	return encoded
}

func EncodeBatchMessages(topic string, partition int, acks string, isIdempotent bool, messages []Message) ([]byte, error) {
	return wire.EncodeBatch(wire.Batch{
		Topic: topic, Partition: partition, Acks: acks, IsIdempotent: isIdempotent, Messages: messages,
	})
}

func DecodeBatchMessages(data []byte) ([]Message, string, int, error) {
	batch, err := wire.DecodeBatch(data)
	if err != nil {
		return nil, "", 0, fmt.Errorf("decode Wire v2 batch: %w", err)
	}
	return batch.Messages, batch.Topic, batch.Partition, nil
}

func WriteWithLength(conn net.Conn, data []byte) error {
	return wire.WriteLengthPrefixed(conn, data)
}

func ReadWithLength(conn net.Conn) ([]byte, error) {
	return wire.ReadLengthPrefixed(conn)
}

func CompressMessage(data []byte, compressionType string) ([]byte, error) {
	compression, err := wire.ParseCompression(compressionType)
	if err != nil {
		return nil, err
	}
	return wire.Compress(data, compression)
}

func DecompressMessage(data []byte, compressionType string) ([]byte, error) {
	compression, err := wire.ParseCompression(compressionType)
	if err != nil {
		return nil, err
	}
	return wire.DecompressBounded(data, compression)
}
