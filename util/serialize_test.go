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
