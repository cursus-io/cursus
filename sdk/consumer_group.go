package sdk

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

// ─── Heartbeat ────────────────────────────────────────────────────────────────

func (c *Consumer) heartbeatLoop(ctx context.Context, assignmentGeneration uint64) {
	interval := time.Duration(c.config.HeartbeatIntervalMS) * time.Millisecond
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer c.resetHeartbeatConn()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.assignmentActive(assignmentGeneration) {
				return
			}
			conn := c.getOrDialHeartbeatConn(ctx)
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

func (c *Consumer) getOrDialHeartbeatConn(ctx context.Context) net.Conn {
	c.hbMu.Lock()
	select {
	case <-ctx.Done():
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
	case <-ctx.Done():
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

func (c *Consumer) startAssignmentWorkers(ctx context.Context, assignmentGeneration uint64) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.State() != ConsumerStateRunning || c.assignmentGeneration.Load() != assignmentGeneration {
		return
	}
	c.mu.RLock()
	pcs := make([]*PartitionConsumer, 0, len(c.partitionConsumers))
	for _, pc := range c.partitionConsumers {
		pcs = append(pcs, pc)
	}
	c.mu.RUnlock()
	for _, pc := range pcs {
		pc.initWorker()
	}
	if c.config.Mode == ModeStreaming {
		c.startStreaming(ctx, assignmentGeneration, pcs)
		return
	}
	c.startConsuming(ctx, assignmentGeneration, pcs)
}

func (c *Consumer) startConsuming(ctx context.Context, assignmentGeneration uint64, pcs []*PartitionConsumer) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.metadataRefreshLoop(ctx)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop(ctx, assignmentGeneration)
	}()

	for _, pc := range pcs {
		pid := pc.partitionID
		c.wg.Add(1)
		go func(pid int, pc *PartitionConsumer) {
			defer c.wg.Done()
			defer pc.closeDataCh()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if !c.ownsPartition(pid, assignmentGeneration) {
						LogWarn("Partition [%d] no longer owned, stopping poller", pid)
						return
					}
					pc.pollAndProcess()
					select {
					case <-time.After(c.config.PollInterval):
					case <-ctx.Done():
						return
					}
				}
			}
		}(pid, pc)
	}
}

func (c *Consumer) startStreaming(ctx context.Context, assignmentGeneration uint64, pcs []*PartitionConsumer) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.metadataRefreshLoop(ctx)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop(ctx, assignmentGeneration)
	}()

	for _, pc := range pcs {
		c.wg.Add(1)
		go func(pc *PartitionConsumer) {
			defer c.wg.Done()
			pc.startStreamLoop()
		}(pc)
	}
}

func (c *Consumer) ownsPartition(pid int, assignmentGeneration uint64) bool {
	if !c.assignmentActive(assignmentGeneration) {
		return false
	}
	c.mu.RLock()
	pc, ok := c.partitionConsumers[pid]
	c.mu.RUnlock()
	if !ok || pc.assignmentGeneration != assignmentGeneration {
		return false
	}
	pc.mu.Lock()
	closed := pc.closed
	pc.mu.Unlock()
	return !closed
}

// ─── Rebalance ────────────────────────────────────────────────────────────────

func (c *Consumer) rebalanceMonitorLoop() {
	for {
		select {
		case <-c.rootCtx.Done():
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
	c.startLifecycleWorker(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-c.rootCtx.Done():
			return
		case <-timer.C:
		}
		select {
		case <-c.rootCtx.Done():
		case c.rebalanceSig <- struct{}{}:
		default:
		}
	})
}

func (c *Consumer) handleRebalanceSignal() {
	assignmentGeneration, ok := c.beginRebalance()
	if !ok {
		return
	}
	defer c.finishRebalance()

	if c.config.EnableMetrics {
		consumerRebalanceTotal.WithLabelValues(c.config.Topic, c.config.GroupID).Inc()
	}

	LogInfo("Rebalance started — stopping existing workers")

	c.cancelAssignment()
	c.closeActiveConnections()
	c.wg.Wait()
	if c.rootCtx.Err() != nil {
		return
	}

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

	assignmentCtx := c.replaceAssignmentContext()

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
			partitionID:          pid,
			consumer:             c,
			fetchOffset:          offset,
			commitOffset:         offset,
			assignmentGeneration: assignmentGeneration,
			ctx:                  assignmentCtx,
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

	c.finishRebalance()
	c.startAssignmentWorkers(assignmentCtx, assignmentGeneration)

	LogInfo("Rebalance completed — consuming %d partitions", len(assignments))
}
