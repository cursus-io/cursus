package types

type StorageHandler interface {
	ReadMessages(offset uint64, max int) ([]Message, error)
	GetAbsoluteOffset() uint64
	GetFlushedOffset() uint64
	GetLatestOffset() uint64
	GetSegmentPath(baseOffset uint64) string

	AppendMessage(topic string, partition int, msg *Message) (uint64, error)
	AppendMessageSync(topic string, partition int, msg *Message) (uint64, error)
	AppendMessageWithOffset(topic string, partition int, msg *Message) error
	WriteBatch(batch []DiskMessage) error

	Flush()
	Close() error
}
