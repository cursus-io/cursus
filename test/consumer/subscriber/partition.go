package subscriber

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

type PartitionConsumer struct {
	partitionID int
	consumer    *Consumer

	fetchOffset  uint64
	commitOffset uint64

	conn    net.Conn
	mu      sync.Mutex
	closed  bool
	backoff *backoff

	dataCh    chan *types.Batch
	once      sync.Once
	closeOnce sync.Once
}

func (pc *PartitionConsumer) initWorker() {
	pc.once.Do(func() {
		channelSize := pc.consumer.config.WorkerChannelSize
		pc.dataCh = make(chan *types.Batch, channelSize)
		pc.consumer.wg.Add(1)
		go pc.runWorker()
	})
}

func (pc *PartitionConsumer) closeDataCh() {
	pc.closeOnce.Do(func() {
		if pc.dataCh != nil {
			close(pc.dataCh)
		}
	})
}

func (pc *PartitionConsumer) runWorker() {
	defer pc.consumer.wg.Done()

	for batch := range pc.dataCh {
		select {
		case <-pc.consumer.mainCtx.Done():
			util.Warn("Partition [%d] worker stopping: context cancelled", pc.partitionID)
			return
		default:
		}

		if len(batch.Messages) == 0 {
			continue
		}

		// message actual process
		if pc.consumer.metrics != nil {
			pc.consumer.metrics.OnFirstConsumeAfterRebalance()

			for _, msg := range batch.Messages {
				pc.consumer.metrics.RecordMessage(pc.partitionID, int64(msg.Offset), msg.ProducerID, msg.SeqNum)
			}
			pc.consumer.metrics.RecordBatch(pc.partitionID, len(batch.Messages))
		}

		if !pc.consumer.config.EnableBenchmark {
			pc.printConsumedMessage(batch)
		}

		if pc.consumer.mainCtx.Err() != nil {
			util.Warn("Partition %d: ownership lost, skipping commit for offset %d", pc.partitionID, batch.Messages[len(batch.Messages)-1].Offset)

			pc.consumer.offsetsMu.Lock()
			if committed, ok := pc.consumer.offsets[pc.partitionID]; ok {
				atomic.StoreUint64(&pc.fetchOffset, committed)
			}
			pc.consumer.offsetsMu.Unlock()
			continue
		}

		lastOffset := batch.Messages[len(batch.Messages)-1].Offset
		commitOffset := lastOffset + 1

		if err := pc.commitOffsetWithRetry(commitOffset); err != nil {
			util.Error("Partition [%d] failed to commit offset: %v", pc.partitionID, err)
		} else {
			atomic.StoreUint64(&pc.commitOffset, commitOffset)

			pc.consumer.offsetsMu.Lock()
			pc.consumer.currentOffsets[pc.partitionID] = commitOffset
			pc.consumer.offsets[pc.partitionID] = commitOffset
			pc.consumer.offsetsMu.Unlock()
		}
	}
}

func (pc *PartitionConsumer) pollAndProcess() {
	select {
	case <-pc.consumer.mainCtx.Done():
		return
	default:
	}

	pc.initWorker()

	if err := pc.ensureConnection(); err != nil {
		util.Warn("Partition [%d] cannot poll: %v", pc.partitionID, err)
		return
	}

	pc.mu.Lock()
	conn := pc.conn
	currentOffset := atomic.LoadUint64(&pc.fetchOffset)
	pc.mu.Unlock()

	c := pc.consumer
	c.mu.RLock()
	memberID := c.memberID
	generation := c.generation
	c.mu.RUnlock()

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s generation=%d member=%s",
		pc.consumer.config.Topic, pc.partitionID, currentOffset, pc.consumer.config.GroupID, generation, memberID)
	if err := util.WriteWithLength(conn, util.EncodeMessage(pc.consumer.config.Topic, consumeCmd)); err != nil {
		util.Error("Partition [%d] send command failed: %v", pc.partitionID, err)
		pc.closeConnection()
		return
	}

	idleTimeout := time.Duration(pc.consumer.config.StreamingReadDeadlineMS) * time.Millisecond
	if idleTimeout == 0 {
		idleTimeout = 5 * time.Second
	}

	if err := conn.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
		util.Error("Partition [%d] failed to set read deadline: %v", pc.partitionID, err)
		pc.closeConnection()
		return
	}

	bo := pc.getBackoff()
	batchData, err := util.ReadWithLength(conn)
	if err != nil {
		util.Error("Partition [%d] read batch error: %v", pc.partitionID, err)
		pc.closeConnection()
		pc.waitWithBackoff(bo)
		return
	}

	if pc.handleBrokerError(batchData) || len(batchData) == 0 {
		if !pc.waitWithBackoff(bo) {
			return
		}
		return
	}

	batch, err := util.DecodeBatchMessages(batchData)
	if err != nil {
		util.Error("Partition [%d] decode error: %v", pc.partitionID, err)
		return
	}

	select {
	case pc.dataCh <- batch:
		if len(batch.Messages) > 0 {
			bo.reset()

			firstMsg := batch.Messages[0]
			lastMsg := batch.Messages[len(batch.Messages)-1]

			expectedOffset := atomic.LoadUint64(&pc.fetchOffset)
			if expectedOffset > 0 && firstMsg.Offset > expectedOffset {
				util.Error("🚨 Partition [%d] offset gap detected. expected: %d, received: %d (missing: %d messages)", pc.partitionID, expectedOffset, firstMsg.Offset, firstMsg.Offset-expectedOffset)
			}

			newOffset := lastMsg.Offset + 1
			atomic.StoreUint64(&pc.fetchOffset, newOffset)
			util.Info("Partition [%d] batch: range [%d - %d], count=%d, nextFetch=%d", pc.partitionID, firstMsg.Offset, lastMsg.Offset, len(batch.Messages), lastMsg.Offset+1)
		}
	case <-c.doneCh:
		pc.closeDataCh()
	}
}

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
			util.Warn("Partition [%d] streaming connection failed, retrying: %v", pid, err)
			if !pc.waitWithBackoff(bo) {
				return
			}
			continue
		}

		pc.consumer.offsetsMu.Lock()
		if committed, ok := pc.consumer.offsets[pid]; ok {
			atomic.StoreUint64(&pc.fetchOffset, committed)
			util.Info("Partition [%d] reconnected. Rolling back offset to committed: %d", pid, committed)
		}
		pc.consumer.offsetsMu.Unlock()

		pc.mu.Lock()
		conn := pc.conn
		currentOffset := atomic.LoadUint64(&pc.fetchOffset)
		pc.mu.Unlock()

		c.mu.RLock()
		memberID, generation := c.memberID, c.generation
		c.mu.RUnlock()

		streamCmd := fmt.Sprintf("STREAM topic=%s partition=%d group=%s offset=%d generation=%d member=%s", c.config.Topic, pid, c.config.GroupID, currentOffset, generation, memberID)
		util.Debug("Partition [%d] sending STREAM command with offset %d", pid, currentOffset)

		if err := util.WriteWithLength(conn, util.EncodeMessage("", streamCmd)); err != nil {
			util.Error("Partition [%d] STREAM command send failed: %v", pid, err)
			pc.closeConnection()
			if !pc.waitWithBackoff(bo) {
				return
			}
			continue
		}

		idleTimeout := time.Duration(c.config.StreamingReadDeadlineMS) * time.Millisecond
		for atomic.LoadInt32(&c.rebalancing) != 1 {
			if err := conn.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
				util.Error("Partition [%d] failed to set read deadline: %v", pid, err)
				pc.closeConnection()
				break
			}

			batchData, err := util.ReadWithLength(conn)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}

				util.Error("Partition [%d] Stream read fatal error: %v", pid, err)
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

			batch, err := util.DecodeBatchMessages(batchData)
			if err != nil {
				util.Error("Partition [%d] stream decode error: %v", pid, err)
				if !pc.waitWithBackoff(bo) {
					return
				}
				continue
			}

			select {
			case pc.dataCh <- batch:
				if len(batch.Messages) > 0 {
					bo.reset()
					lastOffset := batch.Messages[len(batch.Messages)-1].Offset
					atomic.StoreUint64(&pc.fetchOffset, lastOffset+1)
				} else {
					bo.reset()

					select {
					case <-time.After(100 * time.Millisecond):
					case <-c.doneCh:
						return
					case <-c.mainCtx.Done():
						return
					}
				}
			case <-c.doneCh:
				return
			}
		}
	}
}
