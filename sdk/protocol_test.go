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
	encoded, err := EncodeBatchMessages("topic", 1, "1", false, msgs)
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

func TestPayloadIORejectsUnnegotiatedConnections(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	err := WriteWithLength(client, []byte("legacy"))
	require.ErrorContains(t, err, "negotiated Wire v2")
	_, err = ReadWithLength(client)
	require.ErrorContains(t, err, "negotiated Wire v2")
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

	serverDone := make(chan error, 1)
	go func() {
		connection, request, _, err := acceptWireTestRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		serverDone <- writeWireTestResponse(connection, request, string(request.Payload))
	}()

	framed, err := openWireConnection(client, 1000, "none")
	require.NoError(t, err)
	require.NoError(t, WriteWithLength(framed, encoded))
	received, err := ReadWithLength(framed)
	require.NoError(t, err)
	require.NoError(t, <-serverDone)

	decoded, topic, partition, err := DecodeBatchMessages(received)
	require.NoError(t, err)
	assert.Equal(t, "test-topic", topic)
	assert.Equal(t, 1, partition)
	require.Len(t, decoded, 2)
	assert.Equal(t, msgs[0].ProducerID, decoded[0].ProducerID)
}
