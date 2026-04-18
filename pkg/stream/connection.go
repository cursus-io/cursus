package stream

import (
	"net"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// OffsetCommitter abstracts offset commit operations to break the
// circular dependency between stream and coordinator packages.
type OffsetCommitter interface {
	CommitOffset(group, topic string, partition int, offset uint64) error
}

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

	batchSize         int
	interval          time.Duration
	keepaliveInterval time.Duration
	newMessageCh      <-chan struct{} // signal from partition when new messages arrive

	committer OffsetCommitter
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
		batchSize:         10,
		interval:          100 * time.Millisecond,
		keepaliveInterval: 5 * time.Second,
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

func (sc *StreamConnection) SetCommitter(c OffsetCommitter) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.committer = c
}

func (sc *StreamConnection) SetKeepaliveInterval(d time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.keepaliveInterval = d
}

func (sc *StreamConnection) SetNewMessageCh(ch <-chan struct{}) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.newMessageCh = ch
}

func (sc *StreamConnection) Run(
	readFn func(offset uint64, max int) ([]types.Message, error),
	commitInterval time.Duration,
) {
	defer func() {
		if sc.committer != nil {
			_ = sc.committer.CommitOffset(sc.group, sc.topic, sc.partition, sc.Offset())
		}
	}()

	pollTicker := time.NewTicker(sc.interval)
	commitTicker := time.NewTicker(commitInterval)
	keepaliveTicker := time.NewTicker(sc.keepaliveInterval)
	defer pollTicker.Stop()
	defer commitTicker.Stop()
	defer keepaliveTicker.Stop()

	// sendMessages reads and sends available messages. Returns false on fatal error.
	sendMessages := func() bool {
		sc.mu.RLock()
		conn := sc.conn
		bs := sc.batchSize
		sc.mu.RUnlock()

		if conn == nil {
			return false
		}

		currentOffset := sc.Offset()
		msgs, err := readFn(currentOffset, bs)
		if err != nil {
			util.Error("Stream read error for %s/%d: %v", sc.topic, sc.partition, err)
			return false
		}

		if len(msgs) == 0 {
			return true
		}

		// Offset gap detection
		firstOffset := msgs[0].Offset
		if currentOffset > 0 && firstOffset > currentOffset {
			util.Warn("Stream %s/%d: offset gap detected, expected %d but got %d (missing %d messages)",
				sc.topic, sc.partition, currentOffset, firstOffset, firstOffset-currentOffset)
		}

		batchData, err := util.EncodeBatchMessages(sc.topic, sc.partition, "1", false, msgs)
		if err != nil {
			util.Error("Failed to encode batch messages: %v", err)
			return false
		}

		if err := util.WriteWithLength(conn, batchData); err != nil {
			util.Debug("Batch write error in stream: %v", err)
			sc.closeConn()
			return false
		}

		lastOffset := msgs[len(msgs)-1].Offset
		sc.SetOffset(lastOffset + 1)
		sc.SetLastActive(time.Now())
		return true
	}

	for {
		select {
		case <-sc.stopCh:
			return

		case <-commitTicker.C:
			if sc.committer != nil {
				_ = sc.committer.CommitOffset(sc.group, sc.topic, sc.partition, sc.Offset())
			}

		case <-sc.newMessageCh:
			if !sendMessages() {
				sc.Stop()
				return
			}

		case <-pollTicker.C:
			if !sendMessages() {
				sc.Stop()
				return
			}

		case <-keepaliveTicker.C:
			sc.mu.RLock()
			conn := sc.conn
			sc.mu.RUnlock()
			if conn != nil {
				if _, err := conn.Write([]byte{0, 0, 0, 0}); err != nil {
					util.Debug("Keepalive write error: %v", err)
					sc.closeConn()
					sc.Stop()
					return
				}
				sc.SetLastActive(time.Now())
			}
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
