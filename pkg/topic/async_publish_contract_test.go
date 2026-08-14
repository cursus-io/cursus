package topic

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
)

type rejectingAsyncStorage struct{}

func (rejectingAsyncStorage) ReadMessages(uint64, int) ([]types.Message, error) { return nil, nil }
func (rejectingAsyncStorage) GetFirstOffset() uint64                            { return 0 }
func (rejectingAsyncStorage) GetAbsoluteOffset() uint64                         { return 0 }
func (rejectingAsyncStorage) GetFlushedOffset() uint64                          { return 0 }
func (rejectingAsyncStorage) GetLatestOffset() uint64                           { return 0 }
func (rejectingAsyncStorage) GetSegmentPath(uint64) string                      { return "" }
func (rejectingAsyncStorage) AppendMessage(string, int, *types.Message) (uint64, error) {
	return 0, errors.New("write queue full")
}
func (rejectingAsyncStorage) AppendMessageSync(string, int, *types.Message) (uint64, error) {
	return 0, nil
}
func (rejectingAsyncStorage) AppendMessageWithOffset(string, int, *types.Message) error {
	return nil
}
func (rejectingAsyncStorage) WriteBatch([]types.DiskMessage) error { return nil }
func (rejectingAsyncStorage) TruncateTo(uint64) error              { return nil }
func (rejectingAsyncStorage) Flush()                               {}
func (rejectingAsyncStorage) Close() error                         { return nil }

func TestAsyncPublishReturnsStorageEnqueueFailure(t *testing.T) {
	partition := NewPartition(0, "events", rejectingAsyncStorage{}, nil, &config.Config{})
	t.Cleanup(partition.Close)
	topic := &Topic{Name: "events", Partitions: []*Partition{partition}}

	err := topic.PublishToPartition(0, types.Message{Payload: "payload"})
	if err == nil || err.Error() != "write queue full" {
		t.Fatalf("expected write queue failure, got %v", err)
	}
}
