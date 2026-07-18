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

	stopConnCloser := make(chan struct{})
	go func() {
		select {
		case <-c.doneCh:
			c.resetHeartbeatConn()
		case <-stopConnCloser:
		}
	}()
	defer close(stopConnCloser)

	for {
		select {
		case <-c.doneCh:
			return
		case <-ticker.C:
			conn := c.getOrDialHeartbeatConn()
			if conn == nil {
				continue
			}

			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			c.mu.RLock()
			memberID := c.memberID
			generation := c.generation
			c.mu.RUnlock()
			hb := fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d",
				c.config.Topic, c.config.GroupID, memberID, generation)
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
			if strings.Contains(respStr, "REBALANCE_REQUIRED") || strings.Contains(respStr, "GEN_MISMATCH") || strings.Contains(respStr, "NOT_OWNER") || strings.Contains(respStr, "member_not_found") {
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

func (c *Consumer) getOrDialHeartbeatConn() net.Conn {
	c.hbMu.Lock()
	select {
	case <-c.doneCh:
		if c.hbConn != nil {
			_ = c.hbConn.Close()
			c.hbConn = nil
		}
		c.hbMu.Unlock()
		return nil
	default:
	}
	conn := c.hbConn
	if conn != nil {
		c.hbMu.Unlock()
		return conn
	}
	c.hbMu.Unlock()

	newConn, err := c.getCoordinatorConn()
	if err != nil {
		LogError("Heartbeat: failed to connect: %v", err)
		return nil
	}

	c.hbMu.Lock()
	select {
	case <-c.doneCh:
		c.hbMu.Unlock()
		_ = newConn.Close()
		return nil
	default:
	}
	if c.hbConn != nil {
		_ = newConn.Close()
		conn = c.hbConn
	} else {
		c.hbConn = newConn
		conn = newConn
	}
	c.hbMu.Unlock()
	return conn
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
		c.metadataRefreshLoop()
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop()
	}()

	for pid, pc := range c.partitionConsumers {
		c.wg.Add(1)
		go func(pid int, pc *PartitionConsumer) {
			defer c.wg.Done()
			defer pc.closeDataCh()
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
		c.metadataRefreshLoop()
	}()

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

func (c *Consumer) scheduleRebalanceRetry() {
	delay := time.Duration(c.config.ConnectRetryBackoffMS) * time.Millisecond
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-c.doneCh:
			return
		case <-timer.C:
		}
		select {
		case <-c.doneCh:
		case c.rebalanceSig <- struct{}{}:
		default:
		}
	}()
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

	c.wg.Wait()

	drainDeadline := time.After(3 * time.Second)
	for {
		select {
		case <-c.commitCh:
		case <-time.After(100 * time.Millisecond):
			goto drainDone
		case <-drainDeadline:
			LogWarn("Rebalance drain timeout, forcing continuation")
			goto drainDone
		}
	}
drainDone:

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

	if coordAddr, err := c.findCoordinator(); err == nil {
		c.mu.Lock()
		c.coordinatorAddr = coordAddr
		c.mu.Unlock()
	}

	gen, mid, assignments, err := c.joinGroupWithRetry()
	if err != nil {
		LogError("Rebalance join failed: %v", err)
		c.scheduleRebalanceRetry()
		return
	}
	if len(assignments) == 0 {
		assignments, err = c.syncGroup(gen, mid)
		if err != nil {
			LogError("Rebalance sync failed: %v", err)
			c.scheduleRebalanceRetry()
			return
		}
	}

	offsetMap := make(map[int]uint64, len(assignments))
	for _, pid := range assignments {
		offset, err := c.fetchOffsetWithRetry(pid)
		if err != nil {
			LogError("Rebalance: offset fetch failed for P%d: %v", pid, err)
			c.scheduleRebalanceRetry()
			return
		}
		offsetMap[pid] = offset
	}

	c.mu.Lock()
	c.generation = gen
	c.memberID = mid
	for _, pid := range assignments {
		offset := offsetMap[pid]
		c.partitionConsumers[pid] = &PartitionConsumer{
			partitionID:  pid,
			consumer:     c,
			fetchOffset:  offset,
			commitOffset: offset,
		}
		c.offsets[pid] = offset
	}
	c.mu.Unlock()
	for _, pid := range assignments {
		LogInfo("Rebalance: P%d assigned at offset %d (gen=%d)", pid, offsetMap[pid], gen)
	}

	if err := c.fetchMetadata(); err != nil {
		LogWarn("Rebalance: failed to fetch metadata: %v", err)
	}

	if c.config.Mode == ModeStreaming {
		go c.startStreaming()
	} else {
		go c.startConsuming()
	}

	LogInfo("Rebalance completed — consuming %d partitions", len(assignments))
}
