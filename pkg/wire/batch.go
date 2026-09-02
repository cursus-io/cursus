package wire

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	batchMagic          uint32 = 0x43425632 // CBV2
	batchVersion        uint16 = 2
	batchFlagIdempotent        = 1 << 0
)

func IsBatch(data []byte) bool {
	return len(data) >= 6 && binary.BigEndian.Uint32(data[:4]) == batchMagic && binary.BigEndian.Uint16(data[4:6]) == batchVersion
}

func EncodeBatch(batch Batch) ([]byte, error) {
	if batch.Partition < math.MinInt32 || batch.Partition > math.MaxInt32 {
		return nil, fmt.Errorf("partition %d is outside int32 range", batch.Partition)
	}
	if len(batch.Messages) > MaxBatchRecords {
		return nil, fmt.Errorf("message count %d exceeds maximum %d", len(batch.Messages), MaxBatchRecords)
	}
	if len(batch.Messages) > math.MaxUint32 {
		return nil, fmt.Errorf("message count %d exceeds uint32", len(batch.Messages))
	}
	if err := validateAcks(batch.Acks); err != nil {
		return nil, err
	}
	if len(batch.Messages) > 0 {
		if batch.BatchStart == 0 {
			batch.BatchStart = batch.Messages[0].SeqNum
		}
		if batch.BatchEnd == 0 {
			batch.BatchEnd = batch.Messages[len(batch.Messages)-1].SeqNum
		}
	}

	encoder := newBinaryEncoder(MaxFramePayload)
	encoder.uint32(batchMagic)
	encoder.uint16(batchVersion)
	flags := uint16(0)
	if batch.IsIdempotent {
		flags |= batchFlagIdempotent
	}
	encoder.uint16(flags)
	encoder.string(batch.Topic)
	encoder.int32(int32(batch.Partition))
	encoder.string(batch.Acks)
	encoder.uint64(batch.BatchStart)
	encoder.uint64(batch.BatchEnd)
	// #nosec G115 -- the message count is checked against math.MaxUint32 above.
	encoder.uint32(uint32(len(batch.Messages)))
	for index, message := range batch.Messages {
		message.Topic = batch.Topic
		message.Partition = batch.Partition
		record, err := EncodeRecord(message)
		if err != nil {
			return nil, fmt.Errorf("encode message %d: %w", index, err)
		}
		encoder.bytes(record)
	}
	return encoder.result()
}

func DecodeBatch(data []byte) (*Batch, error) {
	decoder := newBinaryDecoder(data)
	magic := decoder.uint32()
	if decoder.err != nil {
		return nil, decoder.err
	}
	if magic != batchMagic {
		return nil, fmt.Errorf("invalid Wire v2 batch magic")
	}
	if version := decoder.uint16(); version != batchVersion {
		return nil, fmt.Errorf("unsupported batch version %d", version)
	}
	flags := decoder.uint16()
	if flags&^uint16(batchFlagIdempotent) != 0 {
		return nil, fmt.Errorf("batch contains unknown flags %x", flags)
	}
	batch := &Batch{
		Topic: decoder.string(), Partition: int(decoder.int32()), Acks: decoder.string(),
		BatchStart: decoder.uint64(), BatchEnd: decoder.uint64(), IsIdempotent: flags&batchFlagIdempotent != 0,
	}
	if err := validateAcks(batch.Acks); err != nil {
		return nil, err
	}
	count := decoder.uint32()
	if count > MaxBatchRecords {
		return nil, fmt.Errorf("message count %d exceeds maximum %d", count, MaxBatchRecords)
	}
	batch.Messages = make([]Message, 0, count)
	for index := uint32(0); index < count; index++ {
		recordData := decoder.bytes()
		if decoder.err != nil {
			return nil, decoder.err
		}
		message, err := DecodeRecord(recordData)
		if err != nil {
			return nil, fmt.Errorf("decode message %d: %w", index, err)
		}
		if message.Topic != batch.Topic || message.Partition != batch.Partition {
			return nil, fmt.Errorf("message %d routing conflicts with batch", index)
		}
		batch.Messages = append(batch.Messages, message)
	}
	if err := decoder.finish(); err != nil {
		return nil, err
	}
	return batch, nil
}

func validateAcks(acks string) error {
	switch acks {
	case "", "0", "none", "1", "-1", "all":
		return nil
	default:
		return fmt.Errorf("invalid acknowledgements %q", acks)
	}
}
