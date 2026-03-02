package stream

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

type StreamManager struct {
	streams   map[string]*StreamConnection // key: "topic:partition:group"
	mu        sync.RWMutex
	maxConn   int
	timeout   time.Duration
	heartbeat time.Duration
}

func NewStreamManager(maxConn int, timeout, heartbeat time.Duration) *StreamManager {
	return &StreamManager{
		streams:   make(map[string]*StreamConnection),
		maxConn:   maxConn,
		timeout:   timeout,
		heartbeat: heartbeat,
	}
}

func (sm *StreamManager) AddStream(key string, stream *StreamConnection,
	readFn func(offset uint64, max int) ([]types.Message, error),
	commitInterval time.Duration,
) error {

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(sm.streams) >= sm.maxConn {
		return fmt.Errorf("maximum connections (%d) reached", sm.maxConn)
	}

	sm.streams[key] = stream
	go stream.Run(readFn, commitInterval)
	go sm.monitorConnection(key, stream)

	return nil
}

func (sm *StreamManager) RemoveStream(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if stream, ok := sm.streams[key]; ok {
		delete(sm.streams, key)
		stream.Stop()
	}
}

func (sm *StreamManager) monitorConnection(key string, stream *StreamConnection) {
	ticker := time.NewTicker(sm.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-stream.stopCh:
			stream.closeConn()
			return
		case <-ticker.C:
			if time.Since(stream.LastActive()) > sm.timeout {
				sm.RemoveStream(key)
				return
			}
		}
	}
}

func (sc *StreamConnection) Conn() net.Conn {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.conn
}

func (sm *StreamManager) StopStream(key string) {
	sm.mu.RLock()
	stream, ok := sm.streams[key]
	sm.mu.RUnlock()
	if !ok {
		return
	}

	stream.Stop()
}

func (sm *StreamManager) GetStreamsForPartition(topic string, partitionID int) []*StreamConnection {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var streams []*StreamConnection
	for _, stream := range sm.streams {
		if stream.topic == topic && stream.partition == partitionID {
			streams = append(streams, stream)
		}
	}
	return streams
}
