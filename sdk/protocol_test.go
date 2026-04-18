package sdk

import (
	"bytes"
	"encoding/binary"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// EncodeMessage
// ---------------------------------------------------------------------------

func TestEncodeMessage_Basic(t *testing.T) {
	data := EncodeMessage("my-topic", "hello world")
	// First 2 bytes = topic length (big-endian uint16)
	topicLen := binary.BigEndian.Uint16(data[:2])
	assert.Equal(t, uint16(8), topicLen)
	assert.Equal(t, "my-topic", string(data[2:2+topicLen]))
	assert.Equal(t, "hello world", string(data[2+topicLen:]))
}

func TestEncodeMessage_EmptyTopic(t *testing.T) {
	data := EncodeMessage("", "payload")
	topicLen := binary.BigEndian.Uint16(data[:2])
	assert.Equal(t, uint16(0), topicLen)
	assert.Equal(t, "payload", string(data[2:]))
}

func TestEncodeMessage_EmptyPayload(t *testing.T) {
	data := EncodeMessage("t", "")
	topicLen := binary.BigEndian.Uint16(data[:2])
	assert.Equal(t, uint16(1), topicLen)
	assert.Equal(t, "t", string(data[2:3]))
	assert.Equal(t, 3, len(data))
}

func TestEncodeMessage_BothEmpty(t *testing.T) {
	data := EncodeMessage("", "")
	assert.Equal(t, 2, len(data))
	assert.Equal(t, uint16(0), binary.BigEndian.Uint16(data[:2]))
}

// ---------------------------------------------------------------------------
// EncodeBatchMessages + DecodeBatchMessages round-trip
// ---------------------------------------------------------------------------

func sampleMessages() []Message {
	return []Message{
		{
			Offset:           10,
			SeqNum:           1,
			ProducerID:       "prod-1",
			Key:              "order-42",
			Epoch:            1700000000,
			Payload:          `{"amount":100}`,
			EventType:        "OrderCreated",
			SchemaVersion:    1,
			AggregateVersion: 1,
			Metadata:         `{"userId":"u-1"}`,
		},
		{
			Offset:           11,
			SeqNum:           2,
			ProducerID:       "prod-1",
			Key:              "order-42",
			Epoch:            1700000001,
			Payload:          `{"status":"shipped"}`,
			EventType:        "OrderShipped",
			SchemaVersion:    2,
			AggregateVersion: 2,
			Metadata:         "",
		},
	}
}

func TestEncodeDecode_RoundTrip(t *testing.T) {
	msgs := sampleMessages()
	encoded, err := EncodeBatchMessages("orders", 3, "all", true, msgs)
	require.NoError(t, err)

	decoded, topic, partition, err := DecodeBatchMessages(encoded)
	require.NoError(t, err)

	assert.Equal(t, "orders", topic)
	assert.Equal(t, 3, partition)
	require.Len(t, decoded, 2)

	for i, m := range msgs {
		d := decoded[i]
		assert.Equal(t, m.Offset, d.Offset, "offset mismatch at %d", i)
		assert.Equal(t, m.SeqNum, d.SeqNum, "seqnum mismatch at %d", i)
		assert.Equal(t, m.ProducerID, d.ProducerID, "producerID mismatch at %d", i)
		assert.Equal(t, m.Key, d.Key, "key mismatch at %d", i)
		assert.Equal(t, m.Epoch, d.Epoch, "epoch mismatch at %d", i)
		assert.Equal(t, m.Payload, d.Payload, "payload mismatch at %d", i)
		assert.Equal(t, m.EventType, d.EventType, "eventType mismatch at %d", i)
		assert.Equal(t, m.SchemaVersion, d.SchemaVersion, "schemaVersion mismatch at %d", i)
		assert.Equal(t, m.AggregateVersion, d.AggregateVersion, "aggregateVersion mismatch at %d", i)
		assert.Equal(t, m.Metadata, d.Metadata, "metadata mismatch at %d", i)
	}
}

func TestEncodeDecode_EmptyMessages(t *testing.T) {
	encoded, err := EncodeBatchMessages("t", 0, "1", false, nil)
	require.NoError(t, err)

	decoded, topic, partition, err := DecodeBatchMessages(encoded)
	require.NoError(t, err)
	assert.Equal(t, "t", topic)
	assert.Equal(t, 0, partition)
	assert.Empty(t, decoded)
}

func TestEncodeDecode_EmptyStringFields(t *testing.T) {
	msgs := []Message{{
		Offset:           0,
		SeqNum:           0,
		ProducerID:       "",
		Key:              "",
		Epoch:            0,
		Payload:          "",
		EventType:        "",
		SchemaVersion:    0,
		AggregateVersion: 0,
		Metadata:         "",
	}}
	encoded, err := EncodeBatchMessages("", 0, "", false, msgs)
	require.NoError(t, err)

	decoded, topic, partition, err := DecodeBatchMessages(encoded)
	require.NoError(t, err)
	assert.Equal(t, "", topic)
	assert.Equal(t, 0, partition)
	require.Len(t, decoded, 1)
	assert.Equal(t, msgs[0].Payload, decoded[0].Payload)
}

func TestEncodeDecode_SingleMessage(t *testing.T) {
	msgs := []Message{{
		Offset:           99,
		SeqNum:           7,
		ProducerID:       "p",
		Key:              "k",
		Epoch:            42,
		Payload:          "data",
		EventType:        "Evt",
		SchemaVersion:    3,
		AggregateVersion: 5,
		Metadata:         "{}",
	}}
	encoded, err := EncodeBatchMessages("topic", 1, "none", false, msgs)
	require.NoError(t, err)

	decoded, topic, partition, err := DecodeBatchMessages(encoded)
	require.NoError(t, err)
	assert.Equal(t, "topic", topic)
	assert.Equal(t, 1, partition)
	require.Len(t, decoded, 1)
	assert.Equal(t, uint64(99), decoded[0].Offset)
	assert.Equal(t, uint64(7), decoded[0].SeqNum)
	assert.Equal(t, "p", decoded[0].ProducerID)
	assert.Equal(t, "k", decoded[0].Key)
	assert.Equal(t, int64(42), decoded[0].Epoch)
	assert.Equal(t, "data", decoded[0].Payload)
	assert.Equal(t, "Evt", decoded[0].EventType)
	assert.Equal(t, uint32(3), decoded[0].SchemaVersion)
	assert.Equal(t, uint64(5), decoded[0].AggregateVersion)
	assert.Equal(t, "{}", decoded[0].Metadata)
}

func TestEncodeBatch_TopicTooLong(t *testing.T) {
	longTopic := strings.Repeat("x", 0x10000)
	_, err := EncodeBatchMessages(longTopic, 0, "", false, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topic too long")
}

func TestEncodeBatch_AcksTooLong(t *testing.T) {
	longAcks := strings.Repeat("a", 256)
	_, err := EncodeBatchMessages("t", 0, longAcks, false, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "acks value too long")
}

func TestEncodeBatch_ProducerIDTooLong(t *testing.T) {
	msgs := []Message{{ProducerID: strings.Repeat("p", 0x10000)}}
	_, err := EncodeBatchMessages("t", 0, "", false, msgs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "producerID too long")
}

func TestEncodeBatch_KeyTooLong(t *testing.T) {
	msgs := []Message{{Key: strings.Repeat("k", 0x10000)}}
	_, err := EncodeBatchMessages("t", 0, "", false, msgs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key too long")
}

func TestEncodeBatch_EventTypeTooLong(t *testing.T) {
	msgs := []Message{{EventType: strings.Repeat("e", 0x10000)}}
	_, err := EncodeBatchMessages("t", 0, "", false, msgs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "eventType too long")
}

func TestEncodeBatch_MetadataTooLong(t *testing.T) {
	msgs := []Message{{Metadata: strings.Repeat("m", 0x10000)}}
	_, err := EncodeBatchMessages("t", 0, "", false, msgs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata too long")
}

// ---------------------------------------------------------------------------
// DecodeBatchMessages error paths
// ---------------------------------------------------------------------------

func TestDecode_TooShort(t *testing.T) {
	_, _, _, err := DecodeBatchMessages([]byte{0xBA})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data too short")
}

func TestDecode_InvalidMagic(t *testing.T) {
	_, _, _, err := DecodeBatchMessages([]byte{0xFF, 0xFF})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid magic number")
}

func TestDecode_NegativeMessageCount(t *testing.T) {
	// Build a minimal valid header with a negative message count.
	var buf bytes.Buffer
	buf.Write([]byte{0xBA, 0x7C})                                       // magic
	_ = binary.Write(&buf, binary.BigEndian, uint16(1))                   // topic len
	buf.WriteByte('t')                                                   // topic
	_ = binary.Write(&buf, binary.BigEndian, int32(0))                   // partition
	_ = binary.Write(&buf, binary.BigEndian, uint8(0))                   // acks len
	_ = binary.Write(&buf, binary.BigEndian, false)                      // isIdempotent
	_ = binary.Write(&buf, binary.BigEndian, uint64(0))                  // batchStart
	_ = binary.Write(&buf, binary.BigEndian, uint64(0))                  // batchEnd
	_ = binary.Write(&buf, binary.BigEndian, int32(-1))                  // msgCount = -1
	_, _, _, err := DecodeBatchMessages(buf.Bytes())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid negative message count")
}

func TestDecode_ExcessiveMessageCount(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0xBA, 0x7C})
	_ = binary.Write(&buf, binary.BigEndian, uint16(1))
	buf.WriteByte('t')
	_ = binary.Write(&buf, binary.BigEndian, int32(0))
	_ = binary.Write(&buf, binary.BigEndian, uint8(0))
	_ = binary.Write(&buf, binary.BigEndian, false)
	_ = binary.Write(&buf, binary.BigEndian, uint64(0))
	_ = binary.Write(&buf, binary.BigEndian, uint64(0))
	_ = binary.Write(&buf, binary.BigEndian, int32(100001))
	_, _, _, err := DecodeBatchMessages(buf.Bytes())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "message count too high")
}

func TestDecode_TruncatedBody(t *testing.T) {
	// Valid header but claims 1 message, body is empty.
	var buf bytes.Buffer
	buf.Write([]byte{0xBA, 0x7C})
	_ = binary.Write(&buf, binary.BigEndian, uint16(1))
	buf.WriteByte('t')
	_ = binary.Write(&buf, binary.BigEndian, int32(0))
	_ = binary.Write(&buf, binary.BigEndian, uint8(0))
	_ = binary.Write(&buf, binary.BigEndian, false)
	_ = binary.Write(&buf, binary.BigEndian, uint64(0))
	_ = binary.Write(&buf, binary.BigEndian, uint64(0))
	_ = binary.Write(&buf, binary.BigEndian, int32(1))
	_, _, _, err := DecodeBatchMessages(buf.Bytes())
	assert.Error(t, err) // should fail reading the first message's offset
}

// ---------------------------------------------------------------------------
// WriteWithLength + ReadWithLength round-trip
// ---------------------------------------------------------------------------

func TestWriteReadWithLength_RoundTrip(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	payload := []byte("hello, cursus")
	errCh := make(chan error, 1)
	go func() {
		errCh <- WriteWithLength(client, payload)
	}()

	got, err := ReadWithLength(server)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
	require.NoError(t, <-errCh)
}

func TestWriteReadWithLength_EmptyPayload(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 1)
	go func() {
		errCh <- WriteWithLength(client, []byte{})
	}()

	got, err := ReadWithLength(server)
	require.NoError(t, err)
	assert.Empty(t, got)
	require.NoError(t, <-errCh)
}

func TestWriteReadWithLength_LargePayload(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	payload := bytes.Repeat([]byte("A"), 1024*1024) // 1 MB
	errCh := make(chan error, 1)
	go func() {
		errCh <- WriteWithLength(client, payload)
	}()

	got, err := ReadWithLength(server)
	require.NoError(t, err)
	assert.Equal(t, len(payload), len(got))
	require.NoError(t, <-errCh)
}

func TestWriteWithLength_OversizedData(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	oversized := make([]byte, MaxMessageSize+1)
	err := WriteWithLength(client, oversized)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

func TestReadWithLength_TruncatedLength(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()

	// Write only 2 bytes instead of 4
	go func() {
		_, _ = server.Write([]byte{0x00, 0x01})
		_ = server.Close()
	}()

	_, err := ReadWithLength(client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read length")
}

func TestReadWithLength_TruncatedBody(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		// Write length=10 but only 3 bytes of body, then close
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, 10)
		_, _ = server.Write(lenBuf)
		_, _ = server.Write([]byte{1, 2, 3})
		_ = server.Close()
	}()

	_, err := ReadWithLength(client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read body")
}

func TestReadWithLength_OversizedLength(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(MaxMessageSize+1))
		_, _ = server.Write(lenBuf)
		_ = server.Close()
	}()

	_, err := ReadWithLength(client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

// ---------------------------------------------------------------------------
// CompressMessage + DecompressMessage round-trip
// ---------------------------------------------------------------------------

func TestCompressDecompress_Gzip(t *testing.T) {
	original := []byte("The quick brown fox jumps over the lazy dog")
	compressed, err := CompressMessage(original, "gzip")
	require.NoError(t, err)
	assert.NotEqual(t, original, compressed)

	decompressed, err := DecompressMessage(compressed, "gzip")
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestCompressDecompress_Snappy(t *testing.T) {
	original := []byte("snappy test data 1234567890")
	compressed, err := CompressMessage(original, "snappy")
	require.NoError(t, err)

	decompressed, err := DecompressMessage(compressed, "snappy")
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestCompressDecompress_LZ4(t *testing.T) {
	original := []byte("lz4 round-trip test payload with repeated content repeated content repeated content")
	compressed, err := CompressMessage(original, "lz4")
	require.NoError(t, err)

	decompressed, err := DecompressMessage(compressed, "lz4")
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestCompressDecompress_None(t *testing.T) {
	original := []byte("no compression")
	compressed, err := CompressMessage(original, "none")
	require.NoError(t, err)
	assert.Equal(t, original, compressed)

	decompressed, err := DecompressMessage(compressed, "none")
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestCompressDecompress_Empty(t *testing.T) {
	original := []byte("empty type means none")
	compressed, err := CompressMessage(original, "")
	require.NoError(t, err)
	assert.Equal(t, original, compressed)

	decompressed, err := DecompressMessage(compressed, "")
	require.NoError(t, err)
	assert.Equal(t, original, decompressed)
}

func TestCompress_UnsupportedType(t *testing.T) {
	_, err := CompressMessage([]byte("data"), "zstd")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported compression type")
}

func TestDecompress_UnsupportedType(t *testing.T) {
	_, err := DecompressMessage([]byte("data"), "zstd")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported compression type")
}

func TestCompressDecompress_EmptyData(t *testing.T) {
	// snappy (xerial framing) does not support empty input, so we skip it here.
	for _, ct := range []string{"gzip", "lz4", "none", ""} {
		t.Run(ct, func(t *testing.T) {
			compressed, err := CompressMessage([]byte{}, ct)
			require.NoError(t, err)

			decompressed, err := DecompressMessage(compressed, ct)
			require.NoError(t, err)
			assert.Empty(t, decompressed)
		})
	}
}

func TestCompressDecompress_LargeData(t *testing.T) {
	original := bytes.Repeat([]byte("ABCDEFGHIJ"), 10000) // 100 KB
	for _, ct := range []string{"gzip", "snappy", "lz4"} {
		t.Run(ct, func(t *testing.T) {
			compressed, err := CompressMessage(original, ct)
			require.NoError(t, err)
			// Repeated data should compress well
			assert.Less(t, len(compressed), len(original))

			decompressed, err := DecompressMessage(compressed, ct)
			require.NoError(t, err)
			assert.Equal(t, original, decompressed)
		})
	}
}

func TestDecompress_InvalidGzipData(t *testing.T) {
	_, err := DecompressMessage([]byte("not gzip"), "gzip")
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// EncodeBatchMessages + compression integration
// ---------------------------------------------------------------------------

func TestEncodeBatch_CompressDecompressRoundTrip(t *testing.T) {
	msgs := sampleMessages()
	encoded, err := EncodeBatchMessages("events", 0, "all", true, msgs)
	require.NoError(t, err)

	for _, ct := range []string{"gzip", "snappy", "lz4", "none"} {
		t.Run(ct, func(t *testing.T) {
			compressed, err := CompressMessage(encoded, ct)
			require.NoError(t, err)

			decompressed, err := DecompressMessage(compressed, ct)
			require.NoError(t, err)

			decoded, topic, partition, err := DecodeBatchMessages(decompressed)
			require.NoError(t, err)
			assert.Equal(t, "events", topic)
			assert.Equal(t, 0, partition)
			require.Len(t, decoded, 2)
			assert.Equal(t, msgs[0].EventType, decoded[0].EventType)
			assert.Equal(t, msgs[1].Payload, decoded[1].Payload)
		})
	}
}

// ---------------------------------------------------------------------------
// WriteWithLength + ReadWithLength with batch messages
// ---------------------------------------------------------------------------

func TestWriteReadBatch_Integration(t *testing.T) {
	msgs := sampleMessages()
	encoded, err := EncodeBatchMessages("test-topic", 1, "1", false, msgs)
	require.NoError(t, err)

	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 1)
	go func() {
		errCh <- WriteWithLength(client, encoded)
	}()

	received, err := ReadWithLength(server)
	require.NoError(t, err)
	require.NoError(t, <-errCh)

	decoded, topic, partition, err := DecodeBatchMessages(received)
	require.NoError(t, err)
	assert.Equal(t, "test-topic", topic)
	assert.Equal(t, 1, partition)
	require.Len(t, decoded, 2)
	assert.Equal(t, msgs[0].ProducerID, decoded[0].ProducerID)
}
