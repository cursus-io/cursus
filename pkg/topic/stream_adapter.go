package topic

import (
	"time"

	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/types"
)

type StreamManagerAdapter struct {
	sm *stream.StreamManager
}

func NewStreamManagerAdapter(sm *stream.StreamManager) *StreamManagerAdapter {
	if sm == nil {
		panic("stream manager cannot be nil")
	}
	return &StreamManagerAdapter{sm: sm}
}

func (a *StreamManagerAdapter) AddStream(key string, streamConn *stream.StreamConnection, readFn func(offset uint64, max int) ([]types.Message, error), commitInterval time.Duration) error {
	return a.sm.AddStream(key, streamConn, readFn, commitInterval)
}

func (a *StreamManagerAdapter) RemoveStream(key string) {
	a.sm.RemoveStream(key)
}

func (a *StreamManagerAdapter) GetStreamsForPartition(topic string, partition int) []*stream.StreamConnection {
	return a.sm.GetStreamsForPartition(topic, partition)
}

func (a *StreamManagerAdapter) StopStream(key string) {
	a.sm.StopStream(key)
}
