package sdk

import (
	"fmt"
	"net"

	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/sdk/internal/transport"
)

const MaxMessageSize = wire.MaxFramePayload

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
	if framed, ok := conn.(*transport.Conn); ok {
		return framed.Send(data)
	}
	return wire.WriteLengthPrefixed(conn, data)
}

func ReadWithLength(conn net.Conn) ([]byte, error) {
	if framed, ok := conn.(*transport.Conn); ok {
		return framed.Receive()
	}
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
