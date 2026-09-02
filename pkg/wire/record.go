package wire

import (
	"encoding/binary"
	"fmt"
	"math"
)

const recordVersion uint16 = 2

const (
	recordTimestamp uint64 = 1 << iota
	recordProducer
	recordKey
	recordEventType
	recordSchemaVersion
	recordAggregateVersion
	recordMetadata
	recordTransactionalID
	recordTransactionState
	recordTransactionMarker
	recordControlBatchType
	recordControlBatchVersion
	recordControlCoordinatorEpoch
	recordControlKey
	recordControlValue
)

const recordKnownMask = (recordControlValue << 1) - 1

func EncodeRecord(message Message) ([]byte, error) {
	if message.Partition < math.MinInt32 || message.Partition > math.MaxInt32 {
		return nil, fmt.Errorf("partition %d is outside int32 range", message.Partition)
	}
	if err := validateTransactionFields(message); err != nil {
		return nil, err
	}

	presence := recordPresence(message)
	encoder := newBinaryEncoder(MaxFramePayload)
	encoder.uint16(recordVersion)
	encoder.uint64(presence)
	encoder.string(message.Topic)
	encoder.int32(int32(message.Partition))
	encoder.uint64(message.Offset)
	encoder.string(message.Payload)
	if presence&recordTimestamp != 0 {
		encoder.int64(message.Timestamp)
	}
	if presence&recordProducer != 0 {
		encoder.string(message.ProducerID)
		encoder.uint64(message.SeqNum)
		encoder.int64(message.Epoch)
	}
	if presence&recordKey != 0 {
		encoder.string(message.Key)
	}
	if presence&recordEventType != 0 {
		encoder.string(message.EventType)
	}
	if presence&recordSchemaVersion != 0 {
		encoder.uint32(message.SchemaVersion)
	}
	if presence&recordAggregateVersion != 0 {
		encoder.uint64(message.AggregateVersion)
	}
	if presence&recordMetadata != 0 {
		encoder.string(message.Metadata)
	}
	if presence&recordTransactionalID != 0 {
		encoder.string(message.TransactionalID)
	}
	if presence&recordTransactionState != 0 {
		encoder.string(message.TransactionState)
	}
	if presence&recordTransactionMarker != 0 {
		encoder.string(message.TransactionMarker)
	}
	if presence&recordControlBatchType != 0 {
		encoder.string(message.ControlBatchType)
	}
	if presence&recordControlBatchVersion != 0 {
		encoder.int16(message.ControlBatchVersion)
	}
	if presence&recordControlCoordinatorEpoch != 0 {
		encoder.int64(message.ControlBatchCoordinatorEpoch)
	}
	if presence&recordControlKey != 0 {
		encoder.bytes(message.ControlBatchKey)
	}
	if presence&recordControlValue != 0 {
		encoder.bytes(message.ControlBatchValue)
	}
	return encoder.result()
}

func DecodeRecord(data []byte) (Message, error) {
	decoder := newBinaryDecoder(data)
	version := decoder.uint16()
	if decoder.err != nil {
		return Message{}, decoder.err
	}
	if version != recordVersion {
		return Message{}, fmt.Errorf("unsupported record version %d", version)
	}
	presence := decoder.uint64()
	if presence&^recordKnownMask != 0 {
		return Message{}, fmt.Errorf("record contains unknown presence bits %x", presence)
	}
	message := Message{
		Topic: decoder.string(), Partition: int(decoder.int32()), Offset: decoder.uint64(), Payload: decoder.string(),
	}
	if presence&recordTimestamp != 0 {
		message.Timestamp = decoder.int64()
	}
	if presence&recordProducer != 0 {
		message.ProducerID = decoder.string()
		message.SeqNum = decoder.uint64()
		message.Epoch = decoder.int64()
	}
	if presence&recordKey != 0 {
		message.Key = decoder.string()
	}
	if presence&recordEventType != 0 {
		message.EventType = decoder.string()
	}
	if presence&recordSchemaVersion != 0 {
		message.SchemaVersion = decoder.uint32()
	}
	if presence&recordAggregateVersion != 0 {
		message.AggregateVersion = decoder.uint64()
	}
	if presence&recordMetadata != 0 {
		message.Metadata = decoder.string()
	}
	if presence&recordTransactionalID != 0 {
		message.TransactionalID = decoder.string()
	}
	if presence&recordTransactionState != 0 {
		message.TransactionState = decoder.string()
	}
	if presence&recordTransactionMarker != 0 {
		message.TransactionMarker = decoder.string()
	}
	if presence&recordControlBatchType != 0 {
		message.ControlBatchType = decoder.string()
	}
	if presence&recordControlBatchVersion != 0 {
		message.ControlBatchVersion = decoder.int16()
	}
	if presence&recordControlCoordinatorEpoch != 0 {
		message.ControlBatchCoordinatorEpoch = decoder.int64()
	}
	if presence&recordControlKey != 0 {
		message.ControlBatchKey = decoder.bytes()
	}
	if presence&recordControlValue != 0 {
		message.ControlBatchValue = decoder.bytes()
	}
	if err := decoder.finish(); err != nil {
		return Message{}, err
	}
	if err := validateTransactionFields(message); err != nil {
		return Message{}, err
	}
	return message, nil
}

func recordPresence(message Message) uint64 {
	var result uint64
	if message.Timestamp != 0 {
		result |= recordTimestamp
	}
	if message.ProducerID != "" || message.SeqNum != 0 || message.Epoch != 0 {
		result |= recordProducer
	}
	if message.Key != "" {
		result |= recordKey
	}
	if message.EventType != "" {
		result |= recordEventType
	}
	if message.SchemaVersion != 0 {
		result |= recordSchemaVersion
	}
	if message.AggregateVersion != 0 {
		result |= recordAggregateVersion
	}
	if message.Metadata != "" {
		result |= recordMetadata
	}
	if message.TransactionalID != "" {
		result |= recordTransactionalID
	}
	if message.TransactionState != "" {
		result |= recordTransactionState
	}
	if message.TransactionMarker != "" {
		result |= recordTransactionMarker
	}
	if message.ControlBatchType != "" {
		result |= recordControlBatchType
	}
	if message.ControlBatchVersion != 0 {
		result |= recordControlBatchVersion
	}
	if message.ControlBatchCoordinatorEpoch != 0 {
		result |= recordControlCoordinatorEpoch
	}
	if message.ControlBatchKey != nil {
		result |= recordControlKey
	}
	if message.ControlBatchValue != nil {
		result |= recordControlValue
	}
	return result
}

func validateTransactionFields(message Message) error {
	switch message.TransactionState {
	case TransactionStateNone, TransactionStateOpen, TransactionStateCommitted, TransactionStateAborted:
	default:
		return fmt.Errorf("invalid transaction state %q", message.TransactionState)
	}
	switch message.TransactionMarker {
	case TransactionMarkerNone, TransactionMarkerCommit, TransactionMarkerAbort:
	default:
		return fmt.Errorf("invalid transaction marker %q", message.TransactionMarker)
	}
	switch message.ControlBatchType {
	case ControlBatchNone, ControlBatchTransaction:
	default:
		return fmt.Errorf("invalid control batch type %q", message.ControlBatchType)
	}
	return nil
}

type binaryEncoder struct {
	data  []byte
	limit int
	err   error
}

func newBinaryEncoder(limit int) *binaryEncoder { return &binaryEncoder{limit: limit} }

func (e *binaryEncoder) append(size int) []byte {
	if e.err != nil {
		return nil
	}
	if size < 0 || size > e.limit-len(e.data) {
		e.err = fmt.Errorf("encoded payload exceeds maximum %d", e.limit)
		return nil
	}
	start := len(e.data)
	e.data = append(e.data, make([]byte, size)...)
	return e.data[start:]
}

func (e *binaryEncoder) uint16(value uint16) {
	field := e.append(2)
	if len(field) == 2 {
		binary.BigEndian.PutUint16(field, value)
	}
}

// Signed values use their two's-complement bit pattern on the wire.
func (e *binaryEncoder) int16(value int16) { e.uint16(uint16(value)) } // #nosec G115 -- intentional bit-preserving conversion.
func (e *binaryEncoder) uint32(value uint32) {
	field := e.append(4)
	if len(field) == 4 {
		binary.BigEndian.PutUint32(field, value)
	}
}
func (e *binaryEncoder) int32(value int32) { e.uint32(uint32(value)) } // #nosec G115 -- intentional bit-preserving conversion.
func (e *binaryEncoder) uint64(value uint64) {
	field := e.append(8)
	if len(field) == 8 {
		binary.BigEndian.PutUint64(field, value)
	}
}
func (e *binaryEncoder) int64(value int64)   { e.uint64(uint64(value)) } // #nosec G115 -- intentional bit-preserving conversion.
func (e *binaryEncoder) string(value string) { e.bytes([]byte(value)) }
func (e *binaryEncoder) bytes(value []byte) {
	if len(value) > math.MaxUint32 {
		e.err = fmt.Errorf("field length %d exceeds uint32", len(value))
		return
	}
	// #nosec G115 -- the field length is checked against math.MaxUint32 above.
	e.uint32(uint32(len(value)))
	copy(e.append(len(value)), value)
}
func (e *binaryEncoder) result() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.data, nil
}

type binaryDecoder struct {
	data []byte
	pos  int
	err  error
}

func newBinaryDecoder(data []byte) *binaryDecoder {
	if len(data) > MaxFramePayload {
		return &binaryDecoder{err: fmt.Errorf("payload size %d exceeds maximum %d", len(data), MaxFramePayload)}
	}
	return &binaryDecoder{data: data}
}

func (d *binaryDecoder) take(size int) []byte {
	if d.err != nil {
		return nil
	}
	if size < 0 || size > len(d.data)-d.pos {
		d.err = fmt.Errorf("truncated binary field at offset %d", d.pos)
		return nil
	}
	result := d.data[d.pos : d.pos+size]
	d.pos += size
	return result
}
func (d *binaryDecoder) uint16() uint16 {
	field := d.take(2)
	if len(field) != 2 {
		return 0
	}
	return binary.BigEndian.Uint16(field)
}
func (d *binaryDecoder) int16() int16 { return int16(d.uint16()) } // #nosec G115 -- decode the preserved two's-complement bits.
func (d *binaryDecoder) uint32() uint32 {
	field := d.take(4)
	if len(field) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(field)
}
func (d *binaryDecoder) int32() int32 { return int32(d.uint32()) } // #nosec G115 -- decode the preserved two's-complement bits.
func (d *binaryDecoder) uint64() uint64 {
	field := d.take(8)
	if len(field) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(field)
}
func (d *binaryDecoder) int64() int64   { return int64(d.uint64()) } // #nosec G115 -- decode the preserved two's-complement bits.
func (d *binaryDecoder) string() string { return string(d.bytes()) }
func (d *binaryDecoder) bytes() []byte {
	length := d.uint32()
	if d.err != nil {
		return nil
	}
	// #nosec G115 -- length is widened losslessly and remaining bytes are non-negative.
	if uint64(length) > uint64(len(d.data)-d.pos) {
		if d.err == nil {
			d.err = fmt.Errorf("field length %d exceeds remaining payload %d", length, len(d.data)-d.pos)
		}
		return nil
	}
	source := d.take(int(length))
	result := make([]byte, int(length))
	copy(result, source)
	return result
}
func (d *binaryDecoder) finish() error {
	if d.err != nil {
		return d.err
	}
	if d.pos != len(d.data) {
		return fmt.Errorf("binary payload has %d trailing bytes", len(d.data)-d.pos)
	}
	return nil
}
