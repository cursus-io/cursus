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

func TestEncodeBatch_UsesUint32FieldLengths(t *testing.T) {
	longTopic := strings.Repeat("x", 0x10000)
	encoded, err := EncodeBatchMessages(longTopic, 0, "1", false, []Message{{
		ProducerID: strings.Repeat("p", 0x10000),
		Key:        strings.Repeat("k", 0x10000),
		EventType:  strings.Repeat("e", 0x10000),
		Metadata:   strings.Repeat("m", 0x10000),
	}})
	require.NoError(t, err)
	decoded, topicName, _, err := DecodeBatchMessages(encoded)
	require.NoError(t, err)
	require.Equal(t, longTopic, topicName)
	require.Len(t, decoded, 1)
	require.Len(t, decoded[0].ProducerID, 0x10000)
	require.Len(t, decoded[0].Key, 0x10000)
}

// ---------------------------------------------------------------------------
// DecodeBatchMessages error paths
// ---------------------------------------------------------------------------

func TestDecode_TooShort(t *testing.T) {
	_, _, _, err := DecodeBatchMessages([]byte{0xBA})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "truncated")
}

func TestDecode_InvalidMagic(t *testing.T) {
	_, _, _, err := DecodeBatchMessages([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 2, 0, 0})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid Wire v2 batch magic")
}

func TestDecode_RejectsLegacyBatchMagic(t *testing.T) {
	_, _, _, err := DecodeBatchMessages([]byte{0xBA, 0x7C, 0, 0, 0, 0})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid Wire v2 batch magic")
}

func TestDecode_ExcessiveMessageCount(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(0x43425632))
	_ = binary.Write(&buf, binary.BigEndian, uint16(2))
	_ = binary.Write(&buf, binary.BigEndian, uint16(0))
	_ = binary.Write(&buf, binary.BigEndian, uint32(1))
	buf.WriteByte('t')
	_ = binary.Write(&buf, binary.BigEndian, int32(0))
	_ = binary.Write(&buf, binary.BigEndian, uint32(0))
	_ = binary.Write(&buf, binary.BigEndian, uint64(0))
	_ = binary.Write(&buf, binary.BigEndian, uint64(0))
	_ = binary.Write(&buf, binary.BigEndian, uint32(100001))
	_, _, _, err := DecodeBatchMessages(buf.Bytes())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "message count 100001 exceeds maximum")
}

func TestDecode_TruncatedBody(t *testing.T) {
	encoded, err := EncodeBatchMessages("t", 0, "1", false, []Message{{Payload: "value"}})
	require.NoError(t, err)
	_, _, _, err = DecodeBatchMessages(encoded[:len(encoded)-1])
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field length")
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
	assert.Contains(t, err.Error(), "read payload length")
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
	assert.Contains(t, err.Error(), "read payload")
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
