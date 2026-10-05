package types

import "fmt"

type StorageHandler interface {
	ReadMessages(offset uint64, max int) ([]Message, error)
	// GetFirstOffset returns the earliest retained offset.
	GetFirstOffset() uint64
	GetAbsoluteOffset() uint64
	GetFlushedOffset() uint64
	// GetLatestOffset returns the next offset that can be assigned, not the last record offset.
	GetLatestOffset() uint64
	GetSegmentPath(baseOffset uint64) string

	AppendMessage(topic string, partition int, msg *Message) (uint64, error)
	AppendMessageSync(topic string, partition int, msg *Message) (uint64, error)
	AppendMessageWithOffset(topic string, partition int, msg *Message) error
	WriteBatch(batch []DiskMessage) error
	TruncateTo(nextOffset uint64) error

	Flush()
	Close() error
}

// BoundedStorageReader lets a storage implementation stop decoding once a
// fetch reaches its byte budget. DecodedBytes describes storage bytes retained
// by the returned records; production disk storage implements this interface.
type BoundedStorageReader interface {
	ReadMessagesBounded(offset uint64, maxRecords, maxBytes int, allowOversizedFirst bool) (messages []Message, decodedBytes int, err error)
}

// DurableBatchStorage extends StorageHandler with a batch append that does not
// return until the batch has crossed the filesystem sync boundary. Production
// disk handlers implement this contract; lightweight test or alternate storage
// implementations may continue to use StorageHandler alone.
type DurableBatchStorage interface {
	StorageHandler
	WriteBatchSync(batch []DiskMessage) error
}

// CompactedRangeStorage durably materializes a logical replica range whose
// superseded offsets may be absent from the physical record stream.
type CompactedRangeStorage interface {
	StorageHandler
	WriteCompactedReplicaRange(startOffset, endOffset uint64, batch []DiskMessage) error
}

// OffsetOutOfRangeError indicates that the requested offset is outside the retained committed log range.
type OffsetOutOfRangeError struct {
	Requested uint64
	Earliest  uint64
	Latest    uint64
}

func (e *OffsetOutOfRangeError) Error() string {
	return fmt.Sprintf("offset %d is out of range, retained range is [%d,%d)", e.Requested, e.Earliest, e.Latest)
}
