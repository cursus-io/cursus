package subscriber

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func (pc *PartitionConsumer) ensureConnection() error {
	if pc.consumer.mainCtx.Err() != nil {
		return fmt.Errorf("consumer shutting down")
	}

	bo := pc.getBackoff()

	pc.mu.Lock()
	if pc.conn != nil {
		pc.mu.Unlock()
		return nil
	}
	if pc.closed {
		pc.mu.Unlock()
		return fmt.Errorf("partition consumer closed")
	}
	pc.mu.Unlock()

	var err error
	for attempt := 0; attempt < pc.consumer.config.MaxConnectRetries; attempt++ {
		pc.mu.Lock()
		if pc.closed {
			pc.mu.Unlock()
			return fmt.Errorf("partition consumer closed during connection attempts")
		}
		pc.mu.Unlock()

		conn, _, connectErr := pc.consumer.client.ConnectWithFailover()
		if connectErr == nil {
			pc.mu.Lock()
			if pc.closed {
				_ = conn.Close()
				pc.mu.Unlock()
				return fmt.Errorf("partition consumer closed")
			}
			pc.conn = conn
			pc.mu.Unlock()
			return nil
		}

		err = connectErr
		waitDur := bo.duration()
		util.Warn("Partition [%d] connect fail (attempt %d): %v. Retrying in %v", pc.partitionID, attempt+1, err, waitDur)

		if !pc.waitDuration(waitDur) {
			return fmt.Errorf("connection aborted by shutdown")
		}
	}
	return fmt.Errorf("failed to connect after retries: %w", err)
}

type offsetOutOfRangeFrame struct {
	Requested uint64
	Earliest  uint64
	Latest    uint64
}

func parseOffsetOutOfRangeFrame(respStr string) (offsetOutOfRangeFrame, bool) {
	if !strings.Contains(respStr, "OFFSET_OUT_OF_RANGE") {
		return offsetOutOfRangeFrame{}, false
	}
	frame := offsetOutOfRangeFrame{}
	hasEarliest := false
	hasLatest := false
	for _, field := range strings.Fields(respStr) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "requested":
			frame.Requested = parsed
		case "earliest":
			frame.Earliest = parsed
			hasEarliest = true
		case "latest":
			frame.Latest = parsed
			hasLatest = true
		}
	}
	return frame, hasEarliest && hasLatest
}

type streamControlFrame struct {
	Type           string
	Reason         string
	Offset         uint64
	HasOffset      bool
	Requested      uint64
	Earliest       uint64
	Latest         uint64
	HasOffsetRange bool
}

func parseStreamControlFrame(data []byte) (streamControlFrame, bool) {
	respStr := string(data)
	if !strings.HasPrefix(respStr, "STREAM_CONTROL") {
		return streamControlFrame{}, false
	}

	frame := streamControlFrame{}
	for _, field := range strings.Fields(respStr) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "type":
			frame.Type = value
		case "reason":
			frame.Reason = value
		case "offset":
			offset, err := strconv.ParseUint(value, 10, 64)
			if err == nil {
				frame.Offset = offset
				frame.HasOffset = true
			}
		case "requested":
			if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
				frame.Requested = parsed
			}
		case "earliest":
			if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
				frame.Earliest = parsed
				frame.HasOffsetRange = true
			}
		case "latest":
			if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
				frame.Latest = parsed
			}
		}
	}
	return frame, true
}

func (pc *PartitionConsumer) handleOffsetOutOfRange(frame offsetOutOfRangeFrame) bool {
	policy := strings.ToLower(pc.consumer.config.AutoOffsetReset)
	if policy == "" {
		policy = "earliest"
	}

	var next uint64
	switch policy {
	case "earliest":
		next = frame.Earliest
	case "latest":
		next = frame.Latest
	case "error":
		util.Error("Partition [%d] offset out of range requested=%d earliest=%d latest=%d", pc.partitionID, frame.Requested, frame.Earliest, frame.Latest)
		pc.consumer.mainCancel()
		pc.closeConnection()
		return true
	default:
		util.Warn("Partition [%d] unknown auto_offset_reset=%q, defaulting to earliest", pc.partitionID, policy)
		next = frame.Earliest
	}

	atomic.StoreUint64(&pc.fetchOffset, next)
	pc.consumer.mu.Lock()
	pc.consumer.offsets[pc.partitionID] = next
	pc.consumer.mu.Unlock()
	util.Warn("Partition [%d] offset out of range requested=%d earliest=%d latest=%d; reset fetch offset to %d (%s)", pc.partitionID, frame.Requested, frame.Earliest, frame.Latest, next, policy)
	pc.closeConnection()
	return true
}
func (pc *PartitionConsumer) handleStreamControl(data []byte) bool {
	frame, ok := parseStreamControlFrame(data)
	if !ok {
		return false
	}

	switch frame.Type {
	case "CLOSE":
		if frame.Reason == "offset_out_of_range" && frame.HasOffsetRange {
			return pc.handleOffsetOutOfRange(offsetOutOfRangeFrame{Requested: frame.Requested, Earliest: frame.Earliest, Latest: frame.Latest})
		}
		if frame.HasOffset {
			atomic.StoreUint64(&pc.fetchOffset, frame.Offset)
		}
		util.Info("Partition [%d] stream closed by broker reason=%s offset=%d", pc.partitionID, frame.Reason, frame.Offset)
		pc.closeConnection()
		return true
	default:
		util.Warn("Partition [%d] unknown stream control frame: %s", pc.partitionID, string(data))
		return true
	}
}
func (pc *PartitionConsumer) handleBrokerError(data []byte) bool {
	respStr := string(data)
	if !strings.HasPrefix(respStr, "ERROR:") {
		return false
	}

	util.Warn("Partition [%d] broker error: %s", pc.partitionID, respStr)

	if frame, ok := parseOffsetOutOfRangeFrame(respStr); ok {
		return pc.handleOffsetOutOfRange(frame)
	}

	if strings.Contains(respStr, "NOT_LEADER") {
		pc.consumer.handleLeaderRedirection(respStr)
	}

	if strings.Contains(respStr, "GEN_MISMATCH") || strings.Contains(respStr, "REBALANCE_REQUIRED") || strings.Contains(respStr, "NOT_OWNER") {
		pc.close()
		pc.consumer.handleRebalanceSignal()
		return true
	}

	pc.closeConnection()
	return true
}

func (pc *PartitionConsumer) commitOffsetWithRetry(offset uint64) error {
	maxRetries := pc.consumer.config.MaxCommitRetries
	var lastErr error

	minBackoff := pc.consumer.config.CommitRetryBackoff
	maxBackoff := pc.consumer.config.CommitRetryMaxBackoff
	bo := newBackoff(minBackoff, maxBackoff)

	for attempt := 0; attempt < maxRetries; attempt++ {
		if pc.consumer.mainCtx.Err() != nil {
			return fmt.Errorf("stopping commit: consumer context cancelled")
		}

		resultCh := make(chan error, 1)
		err := func() error {
			select {
			case pc.consumer.commitCh <- commitEntry{
				partition: pc.partitionID,
				offset:    offset,
				respCh:    resultCh,
			}:

				timer := time.NewTimer(5 * time.Second)
				defer timer.Stop()
				select {
				case err := <-resultCh:
					return err
				case <-pc.consumer.mainCtx.Done():
					return fmt.Errorf("commit cancelled during wait")
				case <-timer.C:
					return fmt.Errorf("commit timeout")
				}

			default:
				util.Warn("Partition %d commitCh full, attempting directCommit", pc.partitionID)
				return pc.consumer.directCommit(pc.partitionID, offset)
			}
		}()

		if err == nil {
			util.Debug("Partition %d batch commit success for offset %d", pc.partitionID, offset)
			return nil
		}

		lastErr = err
		util.Error("Partition %d commit attempt %d failed: %v", pc.partitionID, attempt+1, err)

		if !pc.waitWithBackoff(bo) {
			return fmt.Errorf("commit aborted by shutdown")
		}
	}

	return fmt.Errorf("commit failed after %d attempts: %w", maxRetries, lastErr)
}

func (pc *PartitionConsumer) getBackoff() *backoff {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.backoff == nil {
		min := time.Duration(pc.consumer.config.ConnectRetryBackoffMS) * time.Millisecond
		if min < 200*time.Millisecond {
			min = 200 * time.Millisecond
		}

		max := 30 * time.Second
		pc.backoff = newBackoff(min, max)
	}
	return pc.backoff
}

func (pc *PartitionConsumer) waitWithBackoff(bo *backoff) bool {
	waitDur := bo.duration()
	select {
	case <-pc.consumer.mainCtx.Done():
		return false
	case <-pc.consumer.doneCh:
		return false
	case <-time.After(waitDur):
		return true
	}
}

func (pc *PartitionConsumer) waitDuration(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-pc.consumer.mainCtx.Done():
		return false
	case <-pc.consumer.doneCh:
		return false
	case <-t.C:
		return true
	}
}

func (pc *PartitionConsumer) printConsumedMessage(batch *types.Batch) {
	if len(batch.Messages) == 0 {
		return
	}

	util.Info("📥 Partition [%d] Batch Received: Topic='%s', TotalMessages=%d", pc.partitionID, batch.Topic, len(batch.Messages))
	util.Info("   ├─ Message Details (First 5 messages):")

	limit := 5
	if len(batch.Messages) < limit {
		limit = len(batch.Messages)
	}

	for i := 0; i < limit; i++ {
		msg := batch.Messages[i]

		payload := msg.Payload
		if len(payload) > 50 {
			payload = payload[:50] + "..."
		}

		if msg.Key == "" {
			util.Info("   │  └─ Msg %d: Payload='%s'", i, payload)
		} else {
			util.Info("   │  └─ Msg %d: Key=%s, Payload='%s'", i, msg.Key, payload)
		}
	}

	if len(batch.Messages) > 5 {
		util.Info("   └─ ... and %d more messages.", len(batch.Messages)-5)
	} else {
		util.Info("   └─ All messages listed above.")
	}
}

func (pc *PartitionConsumer) close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		return
	}

	pc.closed = true
	if pc.conn != nil {
		if err := pc.conn.Close(); err != nil {
			util.Debug("failed to close connection: %v", err)
		}
		pc.conn = nil
	}
	pc.closeDataCh()
}

func (pc *PartitionConsumer) closeConnection() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn != nil {
		if err := pc.conn.Close(); err != nil {
			util.Debug("failed to close connection: %v", err)
		}
		pc.conn = nil
	}
}
