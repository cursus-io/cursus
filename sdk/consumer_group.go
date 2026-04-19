package sdk

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

// ─── Heartbeat ────────────────────────────────────────────────────────────────

func (c *Consumer) heartbeatLoop() {
	ticker := time.NewTicker(time.Duration(c.config.HeartbeatIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.doneCh:
			return
		case <-ticker.C:
			c.hbMu.Lock()
			conn := c.hbConn
			c.hbMu.Unlock()

			if conn == nil {
				newConn, err := c.getLeaderConn()
				if err != nil {
					LogError("Heartbeat: failed to connect: %v", err)
					continue
				}
				c.hbMu.Lock()
				c.hbConn = newConn
				conn = newConn
				c.hbMu.Unlock()
			}

			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			hb := fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d",
				c.config.Topic, c.config.GroupID, c.memberID, c.generation)
			if err := WriteWithLength(conn, EncodeMessage("", hb)); err != nil {
				LogError("Heartbeat send failed: %v", err)
				c.cleanupHbConn(conn)
				continue
			}

			resp, err := ReadWithLength(conn)
			_ = conn.SetDeadline(time.Time{})
			if err != nil {
				LogError("Heartbeat response failed: %v", err)
				c.cleanupHbConn(conn)
				continue
			}

			respStr := string(resp)
			if strings.Contains(respStr, "REBALANCE_REQUIRED") || strings.Contains(respStr, "GEN_MISMATCH") {
				LogWarn("Heartbeat: rebalance triggered: %s", respStr)
				select {
				case c.rebalanceSig <- struct{}{}:
				default:
				}
				return
			}
		}
	}
}

func (c *Consumer) cleanupHbConn(bad net.Conn) {
	_ = bad.Close()
	c.hbMu.Lock()
	if c.hbConn == bad {
		c.hbConn = nil
	}
	c.hbMu.Unlock()
}

func (c *Consumer) resetHeartbeatConn() {
	c.hbMu.Lock()
	if c.hbConn != nil {
		_ = c.hbConn.Close()
		c.hbConn = nil
	}
	c.hbMu.Unlock()
}

// ─── Consume / Stream ─────────────────────────────────────────────────────────

func (c *Consumer) startConsuming() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop()
	}()

	for pid, pc := range c.partitionConsumers {
		c.wg.Add(1)
		go func(pid int, pc *PartitionConsumer) {
			defer c.wg.Done()
			for {
				select {
				case <-c.doneCh:
					return
				case <-c.mainCtx.Done():
					LogInfo("Partition [%d] polling worker stopping", pid)
					return
				default:
					if !c.ownsPartition(pid) {
						LogWarn("Partition [%d] no longer owned, stopping poller", pid)
						return
					}
					pc.pollAndProcess()
					select {
					case <-time.After(c.config.PollInterval):
					case <-c.mainCtx.Done():
						return
					case <-c.doneCh:
						return
					}
				}
			}
		}(pid, pc)
	}
}

func (c *Consumer) startStreaming() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop()
	}()

	c.mu.RLock()
	pcs := make([]*PartitionConsumer, 0, len(c.partitionConsumers))
	for _, pc := range c.partitionConsumers {
		pcs = append(pcs, pc)
	}
	c.mu.RUnlock()

	for _, pc := range pcs {
		c.wg.Add(1)
		go func(pc *PartitionConsumer) {
			defer c.wg.Done()
			pc.startStreamLoop()
		}(pc)
	}
}

func (c *Consumer) ownsPartition(pid int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pc, ok := c.partitionConsumers[pid]
	return ok && !pc.closed
}

// ─── Rebalance ────────────────────────────────────────────────────────────────

func (c *Consumer) rebalanceMonitorLoop() {
	for {
		select {
		case <-c.doneCh:
			return
		case <-c.rebalanceSig:
			c.handleRebalanceSignal()
		}
	}
}

func (c *Consumer) handleRebalanceSignal() {
	if !atomic.CompareAndSwapInt32(&c.rebalancing, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&c.rebalancing, 0)

	if c.config.EnableMetrics {
		consumerRebalanceTotal.WithLabelValues(c.config.Topic, c.config.GroupID).Inc()
	}

	LogInfo("Rebalance started — stopping existing workers")

	c.mainCancel()

	drainDone := make(chan struct{})
	go func() {
		deadline := time.After(3 * time.Second)
		for {
			select {
			case <-c.commitCh:
			case <-time.After(100 * time.Millisecond):
				close(drainDone)
				return
			case <-deadline:
				LogWarn("Rebalance drain timeout, forcing continuation")
				close(drainDone)
				return
			}
		}
	}()

	c.wg.Wait()
	<-drainDone

	c.resetHeartbeatConn()
	c.commitMu.Lock()
	if c.commitConn != nil {
		_ = c.commitConn.Close()
		c.commitConn = nil
	}
	c.commitMu.Unlock()

	c.mu.Lock()
	for _, pc := range c.partitionConsumers {
		pc.close()
	}
	c.partitionConsumers = make(map[int]*PartitionConsumer)
	c.offsets = make(map[int]uint64)
	c.mu.Unlock()

	c.mainCtx, c.mainCancel = context.WithCancel(context.Background())

	gen, mid, assignments, err := c.joinGroupWithRetry()
	if err != nil {
		LogError("Rebalance join failed: %v", err)
		return
	}
	if len(assignments) == 0 {
		assignments, err = c.syncGroup(gen, mid)
		if err != nil {
			LogError("Rebalance sync failed: %v", err)
			return
		}
	}

	c.mu.Lock()
	c.generation = gen
	c.memberID = mid
	for _, pid := range assignments {
		offset, err := c.fetchOffset(pid)
		if err != nil {
			LogWarn("Rebalance: offset fetch failed for P%d: %v, starting from 0", pid, err)
			offset = 0
		}
		c.partitionConsumers[pid] = &PartitionConsumer{
			partitionID:  pid,
			consumer:     c,
			fetchOffset:  offset,
			commitOffset: offset,
		}
		c.offsets[pid] = offset
		LogInfo("Rebalance: P%d assigned at offset %d (gen=%d)", pid, offset, gen)
	}
	c.mu.Unlock()

	if c.config.Mode == ModeStreaming {
		go c.startStreaming()
	} else {
		go c.startConsuming()
	}

	LogInfo("Rebalance completed — consuming %d partitions", len(assignments))
}
