package util

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
)

// serializeMessage converts types.Message to serialized bytes
func SerializeMessage(msg types.Message) ([]byte, error) {
	var buf bytes.Buffer
	var err error

	// ProducerID (length + string)
	producerBytes := []byte(msg.ProducerID)
	if err = binary.Write(&buf, binary.BigEndian, uint16(len(producerBytes))); err != nil {
		return nil, fmt.Errorf("write producer length: %w", err)
	}
	if _, err = buf.Write(producerBytes); err != nil {
		return nil, fmt.Errorf("write producer bytes: %w", err)
	}

	// SeqNum (8 bytes)
	if err = binary.Write(&buf, binary.BigEndian, msg.SeqNum); err != nil {
		return nil, fmt.Errorf("write sequence number: %w", err)
	}

	// Payload (length + string)
	payloadBytes := []byte(msg.Payload)
	if err = binary.Write(&buf, binary.BigEndian, uint32(len(payloadBytes))); err != nil {
		return nil, fmt.Errorf("write payload length: %w", err)
	}
	if _, err = buf.Write(payloadBytes); err != nil {
		return nil, fmt.Errorf("write payload bytes: %w", err)
	}

	// Key (length + string)
	keyBytes := []byte(msg.Key)
	if err = binary.Write(&buf, binary.BigEndian, uint16(len(keyBytes))); err != nil {
		return nil, fmt.Errorf("write key length: %w", err)
	}
	if _, err = buf.Write(keyBytes); err != nil {
		return nil, fmt.Errorf("write key bytes: %w", err)
	}

	// Epoch (8 bytes)
	if err = binary.Write(&buf, binary.BigEndian, msg.Epoch); err != nil {
		return nil, fmt.Errorf("write epoch: %w", err)
	}

	return buf.Bytes(), nil
}

// deserializeMessage converts bytes back to types.Message
func DeserializeMessage(data []byte) (types.Message, error) {
	var msg types.Message
	offset := 0

	if len(data) < 2 {
		return msg, fmt.Errorf("data too short for producer length")
	}

	// ProducerID
	producerLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if offset+producerLen > len(data) {
		return msg, fmt.Errorf("data too short for producer ID")
	}

	msg.ProducerID = string(data[offset : offset+producerLen])
	offset += producerLen

	if offset+8 > len(data) {
		return msg, fmt.Errorf("data too short for sequence number")
	}

	// SeqNum (8 bytes)
	msg.SeqNum = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	if offset+4 > len(data) {
		return msg, fmt.Errorf("data too short for payload length")
	}

	// Payload
	payloadLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+payloadLen > len(data) {
		return msg, fmt.Errorf("data too short for payload")
	}

	msg.Payload = string(data[offset : offset+payloadLen])
	offset += payloadLen

	if offset+2 > len(data) {
		return msg, fmt.Errorf("data too short for key length")
	}

	// Key
	keyLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if offset+keyLen > len(data) {
		return msg, fmt.Errorf("data too short for key")
	}

	msg.Key = string(data[offset : offset+keyLen])
	offset += keyLen

	if offset+8 > len(data) {
		return msg, fmt.Errorf("data too short for epoch")
	}

	// Epoch (8 bytes)
	msg.Epoch = int64(binary.BigEndian.Uint64(data[offset : offset+8]))

	return msg, nil
}

// SerializeDiskMessage serializes a DiskMessage for disk storage
func SerializeDiskMessage(msg types.DiskMessage) ([]byte, error) {
	var buf bytes.Buffer

	// Topic (length + string)
	topicBytes := []byte(msg.Topic)
	if err := binary.Write(&buf, binary.BigEndian, uint16(len(topicBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(topicBytes); err != nil {
		return nil, err
	}

	// Partition (4 bytes)
	if err := binary.Write(&buf, binary.BigEndian, msg.Partition); err != nil {
		return nil, err
	}

	// Offset (8 bytes)
	if err := binary.Write(&buf, binary.BigEndian, msg.Offset); err != nil {
		return nil, err
	}

	// ProducerID
	if err := binary.Write(&buf, binary.BigEndian, uint16(len(msg.ProducerID))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(msg.ProducerID); err != nil {
		return nil, err
	}

	// SeqNum
	if err := binary.Write(&buf, binary.BigEndian, msg.SeqNum); err != nil {
		return nil, err
	}

	// Epoch
	if err := binary.Write(&buf, binary.BigEndian, msg.Epoch); err != nil {
		return nil, err
	}

	// Payload (length + string)
	payloadBytes := []byte(msg.Payload)
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(payloadBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(payloadBytes); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeDiskMessage deserializes bytes back to DiskMessage
func DeserializeDiskMessage(data []byte) (types.DiskMessage, error) {
	var msg types.DiskMessage
	offset := 0

	// Topic
	if offset+2 > len(data) {
		return msg, errors.New("data too short for topic length")
	}
	topicLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	if offset+topicLen > len(data) {
		return msg, errors.New("data too short for topic")
	}
	msg.Topic = string(data[offset : offset+topicLen])
	offset += topicLen

	// Partition
	if offset+4 > len(data) {
		return msg, errors.New("data too short for partition")
	}
	msg.Partition = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Offset
	if offset+8 > len(data) {
		return msg, errors.New("data too short for offset")
	}
	msg.Offset = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// ProducerID
	if offset+2 > len(data) {
		return msg, errors.New("data too short for producer ID length")
	}
	prodLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if offset+prodLen > len(data) {
		return msg, errors.New("data too short for producer ID")
	}
	msg.ProducerID = string(data[offset : offset+prodLen])
	offset += prodLen

	// SeqNum
	if offset+8 > len(data) {
		return msg, errors.New("data too short for sequence number")
	}
	msg.SeqNum = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Epoch
	if offset+8 > len(data) {
		return msg, errors.New("data too short for epoch")
	}
	msg.Epoch = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// Payload
	if offset+4 > len(data) {
		return msg, errors.New("data too short for payload length")
	}
	payloadLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	if offset+payloadLen > len(data) {
		return msg, errors.New("data too short for payload")
	}
	msg.Payload = string(data[offset : offset+payloadLen])

	return msg, nil
}
