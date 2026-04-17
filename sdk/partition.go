package sdk

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// messageBatch groups messages from a single poll/stream response.
type messageBatch struct {
	topic    string
	messages []Message
}

// PartitionConsumer handles consuming and committing for a single partition.
type PartitionConsumer struct {
	partitionID  int
	consumer     *Consumer
	fetchOffset  uint64
	commitOffset uint64

	conn   net.Conn
	mu     sync.Mutex
	closed bool
	bo     *backoff

	dataCh    chan *messageBatch
	once      sync.Once
	closeOnce sync.Once
}

// initWorker lazily starts the per-partition worker goroutine (once).
func (pc *PartitionConsumer) initWorker() {
	pc.once.Do(func() {
		channelSize := pc.consumer.config.WorkerChannelSize
		if channelSize <= 0 {
			channelSize = 1000
		}
		pc.dataCh = make(chan *messageBatch, channelSize)
		pc.consumer.wg.Add(1)
		go pc.runWorker()
	})
}

// closeDataCh closes the data channel exactly once.
func (pc *PartitionConsumer) closeDataCh() {
	pc.closeOnce.Do(func() {
		if pc.dataCh != nil {
			close(pc.dataCh)
		}
	})
}

// runWorker processes batches from dataCh, calls the message handler, and commits offsets.
func (pc *PartitionConsumer) runWorker() {
	defer pc.consumer.wg.Done()

	for batch := range pc.dataCh {
		select {
		case <-pc.consumer.mainCtx.Done():
			// Roll back fetchOffset to last committed so the next consumer picks up correctly.
			pc.consumer.mu.RLock()
			committed := pc.consumer.offsets[pc.partitionID]
			pc.consumer.mu.RUnlock()
			atomic.StoreUint64(&pc.fetchOffset, committed)
			LogWarn("Partition [%d] worker stopping: context cancelled, rolled back to offset %d", pc.partitionID, committed)
			continue
		default:
		}

		if len(batch.messages) == 0 {
			continue
		}

		// Deliver messages to user handler.
		handler := pc.consumer.MessageHandler
		if handler != nil {
			for _, msg := range batch.messages {
				if err := handler(msg); err != nil {
					LogError("Partition [%d] handler error at offset %d: %v", pc.partitionID, msg.Offset, err)
				}
			}
		}

		if pc.consumer.mainCtx.Err() != nil {
			// Ownership lost after handler — skip commit, roll back.
			pc.consumer.mu.RLock()
			committed := pc.consumer.offsets[pc.partitionID]
			pc.consumer.mu.RUnlock()
			atomic.StoreUint64(&pc.fetchOffset, committed)
			continue
		}

		lastOffset := batch.messages[len(batch.messages)-1].Offset
		commitOffset := lastOffset + 1

		if err := pc.commitOffsetWithRetry(commitOffset); err != nil {
			LogError("Partition [%d] failed to commit offset %d: %v", pc.partitionID, commitOffset, err)
		} else {
			atomic.StoreUint64(&pc.commitOffset, commitOffset)

			pc.consumer.mu.Lock()
			pc.consumer.offsets[pc.partitionID] = commitOffset
			pc.consumer.mu.Unlock()
		}
	}
}

// pollAndProcess sends one CONSUME command and pushes the resulting batch to dataCh.
func (pc *PartitionConsumer) pollAndProcess() {
	select {
	case <-pc.consumer.mainCtx.Done():
		return
	default:
	}

	pc.initWorker()

	if err := pc.ensureConnection(); err != nil {
		LogWarn("Partition [%d] cannot poll: %v", pc.partitionID, err)
		return
	}

	pc.mu.Lock()
	conn := pc.conn
	currentOffset := atomic.LoadUint64(&pc.fetchOffset)
	pc.mu.Unlock()

	c := pc.consumer
	c.mu.RLock()
	memberID, generation := c.memberID, c.generation
	c.mu.RUnlock()

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s generation=%d member=%s",
		c.config.Topic, pc.partitionID, currentOffset, c.config.GroupID, generation, memberID)

	if err := WriteWithLength(conn, EncodeMessage(c.config.Topic, consumeCmd)); err != nil {
		LogError("Partition [%d] send CONSUME failed: %v", pc.partitionID, err)
		pc.closeConnection()
		return
	}

	idleTimeout := time.Duration(c.config.StreamingReadDeadlineMS) * time.Millisecond
	if idleTimeout <= 0 {
		idleTimeout = 5 * time.Second
	}
	if err := conn.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
		pc.closeConnection()
		return
	}

	bo := pc.getBackoff()
	batchData, err := ReadWithLength(conn)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		LogError("Partition [%d] read batch error: %v", pc.partitionID, err)
		pc.closeConnection()
		pc.waitWithBackoff(bo)
		return
	}

	if pc.handleBrokerError(batchData) || len(batchData) == 0 {
		pc.waitWithBackoff(bo)
		return
	}

	messages, topic, _, err := DecodeBatchMessages(batchData)
	if err != nil {
		LogError("Partition [%d] decode error: %v", pc.partitionID, err)
		return
	}

	if len(messages) == 0 {
		return
	}

	// Offset gap detection
	firstOffset := messages[0].Offset
	expectedOffset := atomic.LoadUint64(&pc.fetchOffset)
	if expectedOffset > 0 && firstOffset > expectedOffset {
		LogError("Partition [%d] offset gap: expected %d, received %d (missing %d messages)",
			pc.partitionID, expectedOffset, firstOffset, firstOffset-expectedOffset)
	}

	newOffset := messages[len(messages)-1].Offset + 1
	atomic.StoreUint64(&pc.fetchOffset, newOffset)
	bo.reset()

	select {
	case pc.dataCh <- &messageBatch{topic: topic, messages: messages}:
	case <-c.doneCh:
		pc.closeDataCh()
	}
}

// startStreamLoop sends a STREAM command and continuously reads batches until rebalance or close.
func (pc *PartitionConsumer) startStreamLoop() {
	pc.initWorker()
	pid := pc.partitionID
	c := pc.consumer
	bo := pc.getBackoff()
	defer pc.closeDataCh()

	for {
		select {
		case <-c.doneCh:
			pc.closeConnection()
			return
		default:
		}

		if atomic.LoadInt32(&c.rebalancing) == 1 {
			pc.closeConnection()
			if !pc.waitWithBackoff(bo) {
				return
			}
			continue
		}

		if err := pc.ensureConnection(); err != nil {
			LogWarn("Partition [%d] stream connection failed, retrying: %v", pid, err)
			if !pc.waitWithBackoff(bo) {
				return
			}
			continue
		}

		// On reconnect, roll back to the last committed offset to avoid gaps.
		c.mu.RLock()
		committed, ok := c.offsets[pid]
		c.mu.RUnlock()
		if ok {
			atomic.StoreUint64(&pc.fetchOffset, committed)
			LogInfo("Partition [%d] reconnected, rolling back to committed offset %d", pid, committed)
		}

		pc.mu.Lock()
		conn := pc.conn
		currentOffset := atomic.LoadUint64(&pc.fetchOffset)
		pc.mu.Unlock()

		c.mu.RLock()
		memberID, generation := c.memberID, c.generation
		c.mu.RUnlock()

		streamCmd := fmt.Sprintf("STREAM topic=%s partition=%d group=%s offset=%d generation=%d member=%s",
			c.config.Topic, pid, c.config.GroupID, currentOffset, generation, memberID)

		if err := WriteWithLength(conn, EncodeMessage("", streamCmd)); err != nil {
			LogError("Partition [%d] STREAM send failed: %v", pid, err)
			pc.closeConnection()
			if !pc.waitWithBackoff(bo) {
				return
			}
			continue
		}

		LogInfo("Partition [%d] streaming from offset %d", pid, currentOffset)

		idleTimeout := time.Duration(c.config.StreamingReadDeadlineMS) * time.Millisecond
		if idleTimeout <= 0 {
			idleTimeout = 5 * time.Minute
		}

		for atomic.LoadInt32(&c.rebalancing) != 1 {
			if err := conn.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
				pc.closeConnection()
				break
			}

			batchData, err := ReadWithLength(conn)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue // idle timeout — retry read
				}
				LogError("Partition [%d] stream read error: %v", pid, err)
				pc.closeConnection()
				if !pc.waitWithBackoff(bo) {
					return
				}
				break
			}

			if len(batchData) == 0 || pc.handleBrokerError(batchData) {
				if !pc.waitWithBackoff(bo) {
					return
				}
				continue
			}

			messages, topic, _, err := DecodeBatchMessages(batchData)
			if err != nil {
				LogError("Partition [%d] stream decode error: %v", pid, err)
				if !pc.waitWithBackoff(bo) {
					return
				}
				continue
			}

			if len(messages) == 0 {
				bo.reset()
				select {
				case <-time.After(100 * time.Millisecond):
				case <-c.doneCh:
					return
				case <-c.mainCtx.Done():
					return
				}
				continue
			}

			lastOffset := messages[len(messages)-1].Offset
			atomic.StoreUint64(&pc.fetchOffset, lastOffset+1)
			bo.reset()

			select {
			case pc.dataCh <- &messageBatch{topic: topic, messages: messages}:
			case <-c.doneCh:
				return
			}
		}
	}
}

// ensureConnection establishes a connection to the broker with retries and backoff.
func (pc *PartitionConsumer) ensureConnection() error {
	if pc.consumer.mainCtx.Err() != nil {
		return fmt.Errorf("consumer shutting down")
	}

	pc.mu.Lock()
	if pc.conn != nil {
		pc.mu.Unlock()
		return nil
	}
	if pc.closed {
		pc.mu.Unlock()
		return fmt.Errorf("%w", ErrConsumerClosed)
	}
	pc.mu.Unlock()

	bo := pc.getBackoff()
	maxRetries := pc.consumer.config.MaxConnectRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		pc.mu.Lock()
		if pc.closed {
			pc.mu.Unlock()
			return fmt.Errorf("%w", ErrConsumerClosed)
		}
		pc.mu.Unlock()

		conn, _, connectErr := pc.consumer.client.ConnectWithFailover()
		if connectErr == nil {
			pc.mu.Lock()
			if pc.closed {
				_ = conn.Close()
				pc.mu.Unlock()
				return fmt.Errorf("%w", ErrConsumerClosed)
			}
			pc.conn = conn
			pc.mu.Unlock()
			return nil
		}

		lastErr = connectErr
		waitDur := bo.duration()
		LogWarn("Partition [%d] connect failed (attempt %d/%d): %v, retrying in %v",
			pc.partitionID, attempt+1, maxRetries, connectErr, waitDur)

		if !pc.waitDuration(waitDur) {
			return fmt.Errorf("connection aborted by shutdown")
		}
	}
	return fmt.Errorf("partition [%d] failed to connect after %d retries: %w", pc.partitionID, maxRetries, lastErr)
}

// handleBrokerError returns true if data is a recognised broker error string.
func (pc *PartitionConsumer) handleBrokerError(data []byte) bool {
	respStr := string(data)
	if !strings.HasPrefix(respStr, "ERROR") {
		return false
	}

	LogWarn("Partition [%d] broker error: %s", pc.partitionID, respStr)

	if strings.Contains(respStr, "NOT_LEADER") {
		pc.consumer.handleLeaderRedirection(respStr)
	}

	if strings.Contains(respStr, "GEN_MISMATCH") || strings.Contains(respStr, "REBALANCE_REQUIRED") {
		pc.close()
		go pc.consumer.handleRebalanceSignal()
		return true
	}

	pc.closeConnection()
	return true
}

// commitOffsetWithRetry tries the commit channel first, falling back to directCommit if full.
func (pc *PartitionConsumer) commitOffsetWithRetry(offset uint64) error {
	maxRetries := pc.consumer.config.MaxCommitRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	minBO := pc.consumer.config.CommitRetryBackoff
	maxBO := pc.consumer.config.CommitRetryMaxBackoff
	bo := newBackoff(minBO, maxBO)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if pc.consumer.mainCtx.Err() != nil {
			return fmt.Errorf("commit cancelled: consumer context done")
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
				LogWarn("Partition [%d] commitCh full, falling back to directCommit", pc.partitionID)
				return pc.consumer.directCommit(pc.partitionID, offset)
			}
		}()

		if err == nil {
			return nil
		}

		lastErr = err
		LogError("Partition [%d] commit attempt %d/%d failed: %v", pc.partitionID, attempt+1, maxRetries, err)

		if !pc.waitWithBackoff(bo) {
			return fmt.Errorf("commit aborted by shutdown")
		}
	}

	return fmt.Errorf("commit failed after %d attempts: %w", maxRetries, lastErr)
}

func (pc *PartitionConsumer) getBackoff() *backoff {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.bo == nil {
		minDur := time.Duration(pc.consumer.config.ConnectRetryBackoffMS) * time.Millisecond
		if minDur < 200*time.Millisecond {
			minDur = 200 * time.Millisecond
		}
		pc.bo = newBackoff(minDur, 30*time.Second)
	}
	return pc.bo
}

func (pc *PartitionConsumer) waitWithBackoff(bo *backoff) bool {
	d := bo.duration()
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

func (pc *PartitionConsumer) PrintConsumedMessage(batch *messageBatch) {
	if len(batch.messages) == 0 {
		return
	}
	LogInfo("Partition [%d] batch received: topic='%s', count=%d", pc.partitionID, batch.topic, len(batch.messages))

	limit := 5
	if len(batch.messages) < limit {
		limit = len(batch.messages)
	}
	for i := 0; i < limit; i++ {
		msg := batch.messages[i]
		payload := msg.Payload
		if len(payload) > 50 {
			payload = payload[:50] + "..."
		}
		if msg.Key == "" {
			LogInfo("  msg[%d]: payload='%s'", i, payload)
		} else {
			LogInfo("  msg[%d]: key=%s payload='%s'", i, msg.Key, payload)
		}
	}
	if len(batch.messages) > 5 {
		LogInfo("  ... and %d more messages.", len(batch.messages)-5)
	}
}

// close marks the consumer as closed and closes the connection and data channel.
func (pc *PartitionConsumer) close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		return
	}
	pc.closed = true
	if pc.conn != nil {
		_ = pc.conn.Close()
		pc.conn = nil
	}
	pc.closeDataCh()
}

// closeConnection drops the current connection without marking the consumer closed.
func (pc *PartitionConsumer) closeConnection() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn != nil {
		_ = pc.conn.Close()
		pc.conn = nil
	}
}
