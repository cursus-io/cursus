package subscriber

import (
	"fmt"
	"strings"
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

func (pc *PartitionConsumer) handleBrokerError(data []byte) bool {
	respStr := string(data)
	if !strings.HasPrefix(respStr, "ERROR:") {
		return false
	}

	util.Warn("Partition [%d] broker error: %s", pc.partitionID, respStr)

	if strings.Contains(respStr, "NOT_LEADER") {
		pc.consumer.handleLeaderRedirection(respStr)
	}

	if strings.Contains(respStr, "GEN_MISMATCH") || strings.Contains(respStr, "REBALANCE_REQUIRED") {
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
