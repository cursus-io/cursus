package stream

import (
	"net"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

type StreamConnection struct {
	conn      net.Conn
	topic     string
	partition int
	group     string

	mu         sync.RWMutex
	offset     uint64
	lastActive time.Time

	stopCh   chan struct{}
	stopOnce sync.Once

	batchSize int
	interval  time.Duration

	coordinator *coordinator.Coordinator
}

// NewStreamConnection creates a new stream connection
func NewStreamConnection(conn net.Conn, topic string, partition int, group string, offset uint64) *StreamConnection {
	sc := &StreamConnection{
		conn:       conn,
		topic:      topic,
		partition:  partition,
		group:      group,
		offset:     offset,
		lastActive: time.Now(),
		stopCh:     make(chan struct{}),
		batchSize:  10,
		interval:   100 * time.Millisecond,
	}
	return sc
}

func (sc *StreamConnection) SetBatchSize(size int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.batchSize = size
}

func (sc *StreamConnection) SetInterval(interval time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.interval = interval
}

func (sc *StreamConnection) SetCoordinator(coord *coordinator.Coordinator) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.coordinator = coord
}

func (sc *StreamConnection) Run(
	readFn func(offset uint64, max int) ([]types.Message, error),
	commitInterval time.Duration,
) {
	defer func() {
		if sc.coordinator != nil {
			_ = sc.coordinator.CommitOffset(sc.group, sc.topic, sc.partition, sc.Offset())
		}
	}()

	ticker := time.NewTicker(sc.interval)
	commitTicker := time.NewTicker(commitInterval)
	defer ticker.Stop()
	defer commitTicker.Stop()

	for {
		select {
		case <-sc.stopCh:
			return
		case <-commitTicker.C:
			if sc.coordinator != nil {
				_ = sc.coordinator.CommitOffset(sc.group, sc.topic, sc.partition, sc.Offset())
				util.Debug("Periodically committed offset %d for %s/%d", sc.Offset(), sc.topic, sc.partition)
			}
		case <-ticker.C:
			sc.mu.Lock()
			conn := sc.conn
			if conn == nil {
				sc.mu.Unlock()
				util.Debug("Stream connection closed, stopping stream for %s partition %d", sc.topic, sc.partition)
				return
			}
			sc.mu.Unlock()
			msgs, err := readFn(sc.Offset(), sc.batchSize)
			if err != nil {
				util.Error("Stream read error for %s/%d: %v", sc.topic, sc.partition, err)
				sc.Stop()
				return
			}

			if len(msgs) == 0 {
				if _, err := conn.Write([]byte{0, 0, 0, 0}); err != nil {
					util.Debug("Keepalive write error in stream: %v", err)
					sc.closeConn()
					sc.Stop()
					return
				}
				sc.SetLastActive(time.Now())
				continue
			}

			batchData, err := util.EncodeBatchMessages(sc.topic, sc.partition, "1", msgs)
			if err != nil {
				util.Error("Failed to encode batch messages: %v", err)
				sc.Stop()
				return
			}

			if err := util.WriteWithLength(conn, batchData); err != nil {
				util.Debug("Batch write error in stream, closing connection: %v", err)
				sc.closeConn()
				sc.Stop()
				return
			}

			lastOffset := msgs[len(msgs)-1].Offset
			sc.SetOffset(lastOffset + 1)
			sc.SetLastActive(time.Now())

			util.Debug("Stream sent batch of %d messages, new offset: %d", len(msgs), sc.Offset())
		}
	}
}

func (sc *StreamConnection) Topic() string  { return sc.topic }
func (sc *StreamConnection) Partition() int { return sc.partition }
func (sc *StreamConnection) Group() string  { return sc.group }

func (sc *StreamConnection) Offset() uint64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.offset
}

func (sc *StreamConnection) SetOffset(o uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.offset = o
}

func (sc *StreamConnection) IncrementOffset() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.offset++
}

func (sc *StreamConnection) SetLastActive(t time.Time) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastActive = t
}

func (sc *StreamConnection) LastActive() time.Time {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.lastActive
}

func (sc *StreamConnection) StopCh() <-chan struct{} { return sc.stopCh }

func (sc *StreamConnection) Stop() {
	sc.stopOnce.Do(func() {
		close(sc.stopCh)
	})
}

func (sc *StreamConnection) closeConn() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.conn != nil {
		_ = sc.conn.Close()
		sc.conn = nil
	}
}
