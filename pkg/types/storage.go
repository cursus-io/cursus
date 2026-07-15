package types

import "fmt"

type StorageHandler interface {
	ReadMessages(offset uint64, max int) ([]Message, error)
	GetFirstOffset() uint64
	GetAbsoluteOffset() uint64
	GetFlushedOffset() uint64
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

// OffsetOutOfRangeError indicates that the requested offset is outside the retained committed log range.
type OffsetOutOfRangeError struct {
	Requested uint64
	Earliest  uint64
	Latest    uint64
}

func (e *OffsetOutOfRangeError) Error() string {
	return fmt.Sprintf("offset %d is out of range, retained range is [%d,%d)", e.Requested, e.Earliest, e.Latest)
}
