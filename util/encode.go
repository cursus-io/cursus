package util

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cursus-io/cursus/pkg/types"
)

// EncodeMessage serializes topic and payload into bytes.
func EncodeMessage(topic string, payload string) []byte {
	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)
	data := make([]byte, 2+len(topicBytes)+len(payloadBytes))
	binary.BigEndian.PutUint16(data[:2], uint16(len(topicBytes)))
	copy(data[2:2+len(topicBytes)], topicBytes)
	copy(data[2+len(topicBytes):], payloadBytes)
	return data
}

// DecodeMessage deserializes bytes into topic and payload.
func DecodeMessage(data []byte) (string, string, error) {
	if len(data) < 2 {
		return "", "", errors.New("data too short")
	}
	topicLen := binary.BigEndian.Uint16(data[:2])
	if int(topicLen)+2 > len(data) {
		return "", "", errors.New("invalid topic length")
	}
	topic := string(data[2 : 2+topicLen])
	payload := string(data[2+int(topicLen):])
	return topic, payload, nil
}

func EncodeBatchMessages(topic string, partition int, acks string, msgs []types.Message) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write([]byte{0xBA, 0x7C})

	write := func(v any) error {
		if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
			return fmt.Errorf("encode value failed: %w", err)
		}
		return nil
	}

	topicBytes := []byte(topic)
	if len(topicBytes) > 0xFFFF {
		return nil, fmt.Errorf("topic too long: %d bytes", len(topicBytes))
	}
	if err := write(uint16(len(topicBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(topicBytes); err != nil {
		return nil, fmt.Errorf("write topic bytes failed: %w", err)
	}

	if err := write(int32(partition)); err != nil {
		return nil, err
	}

	acksBytes := []byte(acks)
	if len(acksBytes) > 0xFF {
		return nil, fmt.Errorf("acks value too long: %d bytes", len(acksBytes))
	}
	if err := write(uint8(len(acksBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(acksBytes); err != nil {
		return nil, fmt.Errorf("write acks bytes failed: %w", err)
	}

	var batchStart, batchEnd uint64
	if len(msgs) > 0 {
		batchStart = msgs[0].SeqNum
		batchEnd = msgs[len(msgs)-1].SeqNum
	}
	if err := write(batchStart); err != nil {
		return nil, err
	}
	if err := write(batchEnd); err != nil {
		return nil, err
	}

	if err := write(int32(len(msgs))); err != nil {
		return nil, err
	}

	for _, m := range msgs {
		if err := write(m.Offset); err != nil {
			return nil, err
		}

		if err := write(m.SeqNum); err != nil {
			return nil, err
		}

		producerIDBytes := []byte(m.ProducerID)
		if len(producerIDBytes) > 0xFFFF {
			return nil, fmt.Errorf("producerID too long: %d bytes", len(producerIDBytes))
		}
		if err := write(uint16(len(producerIDBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(producerIDBytes); err != nil {
			return nil, err
		}

		keyBytes := []byte(m.Key)
		if len(keyBytes) > 0xFFFF {
			return nil, fmt.Errorf("key too long: %d bytes", len(keyBytes))
		}
		if err := write(uint16(len(keyBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return nil, err
		}

		if err := write(m.Epoch); err != nil {
			return nil, err
		}

		payloadBytes := []byte(m.Payload)
		if err := write(uint32(len(payloadBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(payloadBytes); err != nil {
			return nil, fmt.Errorf("write payload bytes failed: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// DecodeBatchMessages decodes a batch encoded by EncodeBatchMessages
func DecodeBatchMessages(data []byte) (*types.Batch, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short")
	}

	reader := bytes.NewReader(data)

	var magic uint16
	if err := binary.Read(reader, binary.BigEndian, &magic); err != nil {
		return nil, fmt.Errorf("failed to read magic number: %w", err)
	}
	if magic != 0xBA7C {
		return nil, fmt.Errorf("invalid magic number: %x", magic)
	}

	var topicLen uint16
	if err := binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %w", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(reader, topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic bytes: %w", err)
	}

	var partition int32
	if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
		return nil, fmt.Errorf("failed to read partition: %w", err)
	}

	var acksLen uint8
	if err := binary.Read(reader, binary.BigEndian, &acksLen); err != nil {
		return nil, fmt.Errorf("failed to read acks length: %w", err)
	}
	acksBytes := make([]byte, acksLen)
	if _, err := io.ReadFull(reader, acksBytes); err != nil {
		return nil, fmt.Errorf("failed to read acks bytes: %w", err)
	}

	var batchStart, batchEnd uint64
	if err := binary.Read(reader, binary.BigEndian, &batchStart); err != nil {
		return nil, fmt.Errorf("failed to read batch start: %w", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &batchEnd); err != nil {
		return nil, fmt.Errorf("failed to read batch end: %w", err)
	}

	var msgCount int32
	if err := binary.Read(reader, binary.BigEndian, &msgCount); err != nil {
		return nil, fmt.Errorf("failed to read message count: %w", err)
	}

	batch := &types.Batch{
		Topic:     string(topicBytes),
		Partition: int(partition),
		Acks:      string(acksBytes),
		Messages:  make([]types.Message, 0, msgCount),
	}

	for i := 0; i < int(msgCount); i++ {
		var m types.Message

		if err := binary.Read(reader, binary.BigEndian, &m.Offset); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] offset: %w", i, err)
		}
		if err := binary.Read(reader, binary.BigEndian, &m.SeqNum); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] seqNum: %w", i, err)
		}

		var pIdLen uint16
		if err := binary.Read(reader, binary.BigEndian, &pIdLen); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] producerID length: %w", i, err)
		}
		pIdBytes := make([]byte, pIdLen)
		if _, err := io.ReadFull(reader, pIdBytes); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] producerID bytes: %w", i, err)
		}
		m.ProducerID = string(pIdBytes)

		var keyLen uint16
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] key length: %w", i, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] key bytes: %w", i, err)
		}
		m.Key = string(keyBytes)

		if err := binary.Read(reader, binary.BigEndian, &m.Epoch); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] epoch: %w", i, err)
		}

		var payloadLen uint32
		if err := binary.Read(reader, binary.BigEndian, &payloadLen); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] payload length: %w", i, err)
		}
		payloadBytes := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payloadBytes); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] payload bytes: %w", i, err)
		}
		m.Payload = string(payloadBytes)

		batch.Messages = append(batch.Messages, m)
	}

	return batch, nil
}
