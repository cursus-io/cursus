package sdk

import (
	"errors"
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
	return fmt.Errorf("SDK payload writes require a negotiated Wire v2 connection")
}

func ReadWithLength(conn net.Conn) ([]byte, error) {
	if framed, ok := conn.(*transport.Conn); ok {
		payload, err := framed.Receive()
		if err == nil {
			return payload, nil
		}
		var brokerErr *wire.BrokerError
		if errors.As(err, &brokerErr) {
			return nil, brokerErrorFromWire(brokerErr)
		}
		return nil, err
	}
	return nil, fmt.Errorf("SDK payload reads require a negotiated Wire v2 connection")
}
