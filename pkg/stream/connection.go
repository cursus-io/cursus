package stream

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const (
	StreamControlPrefix                 = "STREAM_CONTROL"
	StreamControlTypeClose              = "CLOSE"
	StreamControlReasonStopped          = "stopped"
	StreamControlReasonRemoved          = "removed"
	StreamControlReasonTimeout          = "timeout"
	StreamControlReasonError            = "error"
	StreamControlReasonOffsetOutOfRange = "offset_out_of_range"
)

// OffsetCommitter is retained for source compatibility.
// Deprecated: stream delivery never commits consumer offsets.
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

	stopCh             chan struct{}
	stopOnce           sync.Once
	stopReason         string
	stopRequested      uint64
	stopEarliest       uint64
	stopLatest         uint64
	stopHasOffsetRange bool

	batchSize         int
	interval          time.Duration
	keepaliveInterval time.Duration
	newMessageCh      <-chan struct{} // signal from partition when new messages arrive

}

// NewStreamConnection creates a new stream connection
func NewStreamConnection(conn net.Conn, topic string, partition int, group string, offset uint64) *StreamConnection {
	sc := &StreamConnection{
		conn:              conn,
		topic:             topic,
		partition:         partition,
		group:             group,
		offset:            offset,
		lastActive:        time.Now(),
		stopCh:            make(chan struct{}),
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

// SetCommitter is retained for source compatibility.
// Deprecated: stream delivery never commits consumer offsets.
func (sc *StreamConnection) SetCommitter(OffsetCommitter) {}

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

// Run accepts the legacy commit interval for source compatibility but ignores
// it because delivery is not a processing acknowledgement.
func (sc *StreamConnection) Run(
	readFn func(offset uint64, max int) ([]types.Message, error), _ time.Duration,
) {
	defer func() {
		sc.sendCloseControlFrame()
		sc.closeConn()
	}()

	pollTicker := time.NewTicker(sc.interval)
	keepaliveTicker := time.NewTicker(sc.keepaliveInterval)
	defer pollTicker.Stop()
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
			var offsetErr *types.OffsetOutOfRangeError
			if errors.As(err, &offsetErr) {
				sc.setOffsetOutOfRange(offsetErr.Requested, offsetErr.Earliest, offsetErr.Latest)
			} else {
				sc.setStopReason(StreamControlReasonError)
			}
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
			sc.setStopReason(StreamControlReasonError)
			return false
		}

		if err := util.WriteWithLength(conn, batchData); err != nil {
			util.Debug("Batch write error in stream: %v", err)
			sc.setStopReason(StreamControlReasonError)
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

		case <-sc.newMessageCh:
			if !sendMessages() {
				sc.StopWithReason(StreamControlReasonError)
				return
			}

		case <-pollTicker.C:
			if !sendMessages() {
				sc.StopWithReason(StreamControlReasonError)
				return
			}

		case <-keepaliveTicker.C:
			sc.mu.RLock()
			conn := sc.conn
			sc.mu.RUnlock()
			if conn != nil {
				if _, err := conn.Write([]byte{0, 0, 0, 0}); err != nil {
					util.Debug("Keepalive write error: %v", err)
					sc.StopWithReason(StreamControlReasonError)
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
	sc.StopWithReason(StreamControlReasonStopped)
}

func (sc *StreamConnection) StopWithReason(reason string) {
	if reason == "" {
		reason = StreamControlReasonStopped
	}
	sc.setStopReason(reason)
	sc.stopOnce.Do(func() {
		close(sc.stopCh)
	})
}

func (sc *StreamConnection) setStopReason(reason string) {
	if reason == "" {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.stopReason == "" {
		sc.stopReason = reason
	}
}

func (sc *StreamConnection) setOffsetOutOfRange(requested, earliest, latest uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.stopReason == "" {
		sc.stopReason = StreamControlReasonOffsetOutOfRange
	}
	sc.stopRequested = requested
	sc.stopEarliest = earliest
	sc.stopLatest = latest
	sc.stopHasOffsetRange = true
}
func (sc *StreamConnection) sendCloseControlFrame() {
	sc.mu.RLock()
	conn := sc.conn
	reason := sc.stopReason
	offset := sc.offset
	requested := sc.stopRequested
	earliest := sc.stopEarliest
	latest := sc.stopLatest
	hasOffsetRange := sc.stopHasOffsetRange
	sc.mu.RUnlock()

	if conn == nil {
		return
	}
	if reason == "" {
		reason = StreamControlReasonStopped
	}

	frame := fmt.Sprintf("%s type=%s reason=%s offset=%d", StreamControlPrefix, StreamControlTypeClose, reason, offset)
	if hasOffsetRange {
		frame = fmt.Sprintf("%s requested=%d earliest=%d latest=%d", frame, requested, earliest, latest)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
	if err := util.WriteWithLength(conn, []byte(frame)); err != nil {
		util.Debug("Stream close control write error: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Time{})
}

func (sc *StreamConnection) closeConn() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.conn != nil {
		_ = sc.conn.Close()
		sc.conn = nil
	}
}
