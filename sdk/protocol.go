package sdk

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/pierrec/lz4/v4"
	snappy "github.com/segmentio/kafka-go/compress/snappy/go-xerial-snappy"
)

const MaxMessageSize = 64 * 1024 * 1024 // 64MB

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

func EncodeBatchMessages(topic string, partition int, acks string, isIdempotent bool, msgs []Message) ([]byte, error) {
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

	if err := write(isIdempotent); err != nil {
		return nil, err
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

// WriteWithLength writes data with a 4-byte length prefix.
func WriteWithLength(conn net.Conn, data []byte) error {
	if len(data) > MaxMessageSize {
		return fmt.Errorf("data size %d exceeds maximum %d", len(data), MaxMessageSize)
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	buf := append(lenBuf, data...)
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	return nil
}

// ReadWithLength reads data with a 4-byte length prefix.
func ReadWithLength(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)
	if length > MaxMessageSize {
		return nil, fmt.Errorf("message size %d exceeds maximum %d", length, MaxMessageSize)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return buf, nil
}

// CompressMessage compresses a message if enabled
func CompressMessage(data []byte, compressionType string) ([]byte, error) {
	switch compressionType {
	case "gzip":
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		if _, err := gw.Write(data); err != nil {
			return nil, err
		}
		if err := gw.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	case "snappy":
		return snappy.Encode(data), nil

	case "lz4":
		var buf bytes.Buffer
		zw := lz4.NewWriter(&buf)
		if _, err := zw.Write(data); err != nil {
			return nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	case "none", "":
		return data, nil

	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// DecompressMessage decompresses a message if enabled
func DecompressMessage(data []byte, compressionType string) ([]byte, error) {
	switch compressionType {
	case "gzip":
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		res, err := io.ReadAll(gr)
		if err != nil {
			_ = gr.Close()
			return nil, err
		}
		if err := gr.Close(); err != nil {
			return nil, err
		}
		return res, nil

	case "snappy":
		return snappy.Decode(data)

	case "lz4":
		reader := lz4.NewReader(bytes.NewReader(data))
		res, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("lz4 read all: %w", err)
		}
		return res, nil

	case "none", "":
		return data, nil

	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// DecodeBatchMessages decodes a batch encoded by EncodeBatchMessages
func DecodeBatchMessages(data []byte) ([]Message, string, int, error) {
	if len(data) < 2 {
		return nil, "", 0, fmt.Errorf("data too short")
	}

	reader := bytes.NewReader(data)

	var magic uint16
	if err := binary.Read(reader, binary.BigEndian, &magic); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read magic number: %w", err)
	}
	if magic != 0xBA7C {
		return nil, "", 0, fmt.Errorf("invalid magic number: %x", magic)
	}

	var topicLen uint16
	if err := binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read topic length: %w", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(reader, topicBytes); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read topic bytes: %w", err)
	}

	var partition int32
	if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read partition: %w", err)
	}

	var acksLen uint8
	if err := binary.Read(reader, binary.BigEndian, &acksLen); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read acks length: %w", err)
	}
	acksBytes := make([]byte, acksLen)
	if _, err := io.ReadFull(reader, acksBytes); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read acks bytes: %w", err)
	}

	var isIdempotent bool
	if err := binary.Read(reader, binary.BigEndian, &isIdempotent); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read isIdempotent: %w", err)
	}

	var batchStart, batchEnd uint64
	if err := binary.Read(reader, binary.BigEndian, &batchStart); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read batch start: %w", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &batchEnd); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read batch end: %w", err)
	}

	var msgCount int32
	if err := binary.Read(reader, binary.BigEndian, &msgCount); err != nil {
		return nil, "", 0, fmt.Errorf("failed to read message count: %w", err)
	}

	messages := make([]Message, 0, msgCount)
	for i := 0; i < int(msgCount); i++ {
		var m Message
		if err := binary.Read(reader, binary.BigEndian, &m.Offset); err != nil {
			return nil, "", 0, fmt.Errorf("read offset[%d]: %w", i, err)
		}
		if err := binary.Read(reader, binary.BigEndian, &m.SeqNum); err != nil {
			return nil, "", 0, fmt.Errorf("read seqnum[%d]: %w", i, err)
		}

		var pIdLen uint16
		if err := binary.Read(reader, binary.BigEndian, &pIdLen); err != nil {
			return nil, "", 0, fmt.Errorf("read producerID length[%d]: %w", i, err)
		}
		pIdBytes := make([]byte, pIdLen)
		if _, err := io.ReadFull(reader, pIdBytes); err != nil {
			return nil, "", 0, fmt.Errorf("read producerID[%d]: %w", i, err)
		}
		m.ProducerID = string(pIdBytes)

		var keyLen uint16
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return nil, "", 0, fmt.Errorf("read key length[%d]: %w", i, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, "", 0, fmt.Errorf("read key[%d]: %w", i, err)
		}
		m.Key = string(keyBytes)

		if err := binary.Read(reader, binary.BigEndian, &m.Epoch); err != nil {
			return nil, "", 0, fmt.Errorf("read epoch[%d]: %w", i, err)
		}

		var payloadLen uint32
		if err := binary.Read(reader, binary.BigEndian, &payloadLen); err != nil {
			return nil, "", 0, fmt.Errorf("read payload length[%d]: %w", i, err)
		}
		payloadBytes := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payloadBytes); err != nil {
			return nil, "", 0, fmt.Errorf("read payload[%d]: %w", i, err)
		}
		m.Payload = string(payloadBytes)

		messages = append(messages, m)
	}

	return messages, string(topicBytes), int(partition), nil
}


