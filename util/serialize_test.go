package util

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestMessageSerialization(t *testing.T) {
	msg := types.Message{
		ProducerID: "prod-1",
		SeqNum:     12345,
		Payload:    "test-payload",
		Key:        "test-key",
		Epoch:      100,
	}

	data, err := SerializeMessage(msg)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := DeserializeMessage(data)
	assert.NoError(t, err)
	assert.Equal(t, msg.ProducerID, decoded.ProducerID)
	assert.Equal(t, msg.SeqNum, decoded.SeqNum)
	assert.Equal(t, msg.Payload, decoded.Payload)
	assert.Equal(t, msg.Key, decoded.Key)
	assert.Equal(t, msg.Epoch, decoded.Epoch)
}

func TestDiskMessageSerialization(t *testing.T) {
	msg := types.DiskMessage{
		Topic:      "test-topic",
		Partition:  2,
		Offset:     500,
		ProducerID: "p1",
		SeqNum:     10,
		Epoch:      5,
		Payload:    "hello world",
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := DeserializeDiskMessage(data)
	assert.NoError(t, err)
	assert.Equal(t, msg, decoded)
}

func TestDeserializeMessage_ErrorCases(t *testing.T) {
	// Too short for initial fields
	_, err := DeserializeMessage([]byte{0})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "producer length")

	_, err = DeserializeDiskMessage([]byte{0})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topic length")

	// Malformed length-prefixes (claims to have more data than provided)
	// DeserializeMessage: [2 bytes producer length] [producer ID...]
	_, err = DeserializeMessage([]byte{0, 10}) // claims 10 bytes, but 0 provided
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "producer ID")

	// DeserializeDiskMessage: [2 bytes topic length] [topic...] [4 bytes partition] ...
	_, err = DeserializeDiskMessage([]byte{0, 5, 't', 'e', 's', 't', '1'}) // claims 5 bytes topic, OK, but missing partition
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "partition")

	// Large claims that exceed buffer
	_, err = DeserializeMessage([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255})
	assert.Error(t, err)
}

func TestDiskMessageSerialization_WithEventSourcingFields(t *testing.T) {
	msg := types.DiskMessage{
		Topic:            "orders",
		Partition:        2,
		Offset:           100,
		ProducerID:       "prod-1",
		SeqNum:           10,
		Epoch:            999,
		Payload:          `{"item":"widget"}`,
		EventType:        "OrderPlaced",
		SchemaVersion:    3,
		AggregateVersion: 7,
		Metadata:         `{"trace_id":"abc123"}`,
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)

	got, err := DeserializeDiskMessage(data)
	assert.NoError(t, err)
	assert.Equal(t, msg.EventType, got.EventType)
	assert.Equal(t, msg.SchemaVersion, got.SchemaVersion)
	assert.Equal(t, msg.AggregateVersion, got.AggregateVersion)
	assert.Equal(t, msg.Metadata, got.Metadata)
}

func TestDeserializeDiskMessage_TruncatedEventSourcingFields(t *testing.T) {
	msg := types.DiskMessage{
		Topic:            "t",
		Partition:        0,
		Offset:           1,
		ProducerID:       "p",
		SeqNum:           1,
		Epoch:            1,
		Payload:          "x",
		EventType:        "SomeEvent",
		SchemaVersion:    1,
		AggregateVersion: 1,
		Metadata:         "meta",
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)

	// Truncate so event-sourcing fields are incomplete
	truncated := data[:len(data)-10]
	_, err = DeserializeDiskMessage(truncated)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete event-sourcing fields")
}

func TestDeserializeDiskMessage_OldFormatNoEventSourcing(t *testing.T) {
	// Serialize with empty event-sourcing fields, then strip the ES trailer
	msg := types.DiskMessage{
		Topic:      "legacy-topic",
		Partition:  1,
		Offset:     50,
		ProducerID: "old-prod",
		SeqNum:     5,
		Epoch:      500,
		Payload:    "old-payload",
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)

	// ES trailer for empty fields: 2 (eventType len) + 4 (schema) + 8 (aggVer) + 2 (metadata len) = 16 bytes
	oldData := data[:len(data)-16]

	got, err := DeserializeDiskMessage(oldData)
	assert.NoError(t, err)
	assert.Equal(t, msg.Topic, got.Topic)
	assert.Equal(t, msg.Payload, got.Payload)
	assert.Equal(t, msg.ProducerID, got.ProducerID)
	// Event-sourcing fields should be zero-valued
	assert.Equal(t, "", got.EventType)
	assert.Equal(t, uint32(0), got.SchemaVersion)
	assert.Equal(t, uint64(0), got.AggregateVersion)
	assert.Equal(t, "", got.Metadata)
}

func TestDiskMessageSerialization_EmptyFields(t *testing.T) {
	msg := types.DiskMessage{
		Topic:      "empty-test",
		Partition:  0,
		Offset:     0,
		ProducerID: "",
		SeqNum:     0,
		Epoch:      0,
		Payload:    "",
	}

	data, err := SerializeDiskMessage(msg)
	assert.NoError(t, err)

	got, err := DeserializeDiskMessage(data)
	assert.NoError(t, err)
	assert.Equal(t, "empty-test", got.Topic)
	assert.Equal(t, "", got.ProducerID)
	assert.Equal(t, "", got.Payload)
	assert.Equal(t, "", got.EventType)
	assert.Equal(t, "", got.Metadata)
}
