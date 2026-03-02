package subscriber

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/test/consumer/bench"
	"github.com/cursus-io/cursus/util"

	"github.com/cursus-io/cursus/test/consumer/client"
	"github.com/cursus-io/cursus/test/consumer/config"
)

type Consumer struct {
	config             *config.ConsumerConfig
	client             *client.ConsumerClient
	partitionConsumers map[int]*PartitionConsumer

	generation int64
	memberID   string

	commitConn     net.Conn
	commitCh       chan commitEntry
	commitMu       sync.Mutex
	commitRetryMap map[int]uint64

	currentOffsets map[int]uint64
	offsetsMu      sync.Mutex

	wg         sync.WaitGroup
	mainCtx    context.Context
	mainCancel context.CancelFunc

	rebalancing  int32
	rebalanceSig chan struct{}

	offsets map[int]uint64
	doneCh  chan struct{}
	mu      sync.RWMutex

	hbConn net.Conn
	hbMu   sync.Mutex

	closed     int32
	bmFinished int32 // 0: running, 1: finished

	bmStartTime time.Time
	metrics     *bench.ConsumerMetrics
}

type commitEntry struct {
	partition int
	offset    uint64
	respCh    chan error
}

func NewConsumer(cfg *config.ConsumerConfig) (*Consumer, error) {
	client := client.NewConsumerClient(cfg)
	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		config:             cfg,
		client:             client,
		partitionConsumers: make(map[int]*PartitionConsumer),
		offsets:            make(map[int]uint64),
		currentOffsets:     make(map[int]uint64),
		rebalanceSig:       make(chan struct{}, 1),
		doneCh:             make(chan struct{}),
		mainCtx:            ctx,
		mainCancel:         cancel,
	}

	if cfg.EnableBenchmark {
		c.metrics = bench.NewConsumerMetrics(int64(cfg.NumMessages), cfg.EnableCorrectness)
	}

	c.commitCh = make(chan commitEntry, 1024)
	return c, nil
}

func (c *Consumer) Done() <-chan struct{} {
	return c.doneCh
}

func (c *Consumer) GetMetrics() *bench.ConsumerMetrics {
	return c.metrics
}

func (c *Consumer) getLeaderConn() (net.Conn, error) {
	conn, _, err := c.client.ConnectWithFailover()
	if err != nil {
		return nil, err
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(true); err != nil {
			util.Error("failed to set keep alive: %v", err)
		}
		if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
			util.Error("failed to set keep alive period: %v", err)
		}
	}

	return conn, nil
}

func (c *Consumer) validateCommitConn() bool {
	if c.commitConn == nil {
		return false
	}

	if err := c.commitConn.SetReadDeadline(time.Now().Add(1 * time.Millisecond)); err != nil {
		util.Debug("failed to set read deadline: %v", err)
		if err := c.commitConn.Close(); err != nil {
			util.Debug("failed to close invalid commit connection: %v", err)
		}
		c.commitConn = nil
		return false
	}

	_, err := c.commitConn.Read(make([]byte, 0))
	if err != nil && !os.IsTimeout(err) {
		if err := c.commitConn.Close(); err != nil {
			util.Debug("failed to close broken commit connection: %v", err)
		}
		c.commitConn = nil
		return false
	}

	_ = c.commitConn.SetReadDeadline(time.Time{})
	return true
}

func (c *Consumer) Start() error {
	gen, mid, assignments, err := c.joinGroup()
	if err != nil {
		return fmt.Errorf("join group failed: %w", err)
	}
	c.generation = gen
	c.memberID = mid

	if len(assignments) == 0 {
		assignments, err = c.syncGroup(gen, mid)
		if err != nil {
			return fmt.Errorf("sync group failed: %w", err)
		}
	}

	util.Info("✅ Successfully joined topic '%s' for group '%s' with %d partitions: %v (generation=%d, member=%s)",
		c.config.Topic, c.config.GroupID, len(assignments), assignments, gen, mid)

	c.partitionConsumers = make(map[int]*PartitionConsumer)
	for _, pid := range assignments {
		offset, err := c.fetchOffset(pid)
		if err != nil {
			util.Warn("Failed to fetch offset for P%d: %v", pid, err)
			offset = 0
		}

		c.mu.Lock()
		c.offsets[pid] = offset
		c.mu.Unlock()

		pc := &PartitionConsumer{
			partitionID:  pid,
			consumer:     c,
			fetchOffset:  offset,
			commitOffset: offset,
		}
		c.partitionConsumers[pid] = pc
	}

	c.startCommitWorker()
	go c.rebalanceMonitorLoop()

	if c.config.Mode != config.ModePolling {
		go c.startStreaming()
	} else {
		go c.startConsuming()
	}

	if c.config.EnableBenchmark {
		go func() {
			c.waitForBenchmarkCompletion()
			if err := c.Close(); err != nil {
				util.Debug("failed to close consumer: %v", err)
			}
		}()
	}
	<-c.mainCtx.Done()
	return nil
}

func (c *Consumer) startCommitWorker() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.config.AutoCommitInterval)
		defer ticker.Stop()

		pendingOffsets := make(map[int]uint64)
		respChannels := make(map[int][]chan error)

		flush := func() {
			if len(pendingOffsets) > 0 {
				c.commitBatch(pendingOffsets, respChannels)
				pendingOffsets = make(map[int]uint64)
				respChannels = make(map[int][]chan error)
			}
		}

		for {
			select {
			case entry, ok := <-c.commitCh:
				if !ok {
					flush()
					return
				}

				if existing, exists := pendingOffsets[entry.partition]; !exists || entry.offset > existing {
					pendingOffsets[entry.partition] = entry.offset
				}

				if entry.respCh != nil {
					respChannels[entry.partition] = append(respChannels[entry.partition], entry.respCh)
					flush()
				}

			case <-ticker.C:
				c.flushOffsets()
				flush()
				c.processRetryQueue()

			case <-c.mainCtx.Done():
				flush()
				return
			}
		}
	}()
}

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

func (c *Consumer) commitBatch(offsets map[int]uint64, respChannels map[int][]chan error) {
	success := c.sendBatchCommit(offsets)

	for pid, channels := range respChannels {
		var err error
		if !success {
			err = fmt.Errorf("batch commit failed for partition %d", pid)
		}
		for _, ch := range channels {
			if ch != nil {
				ch <- err
			}
		}
	}
}

func (c *Consumer) waitForBenchmarkCompletion() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ok, reason := c.metrics.IsFullyConsumed(int64(c.config.NumMessages))
			if ok {
				util.Info("✅ All messages consumed.")
				return
			}
			if reason != "" {
				util.Warn("⏳ waiting: %s", reason)
			}

		case <-c.mainCtx.Done():
			return
		case <-c.doneCh:
			return
		}
	}
}

func (c *Consumer) flushOffsets() {
	if atomic.LoadInt32(&c.rebalancing) == 1 {
		return
	}

	c.offsetsMu.Lock()
	defer c.offsetsMu.Unlock()

	if len(c.currentOffsets) == 0 {
		return
	}

	for pid, offset := range c.currentOffsets {
		c.mu.RLock()
		lastCommitted := c.offsets[pid]
		c.mu.RUnlock()

		if offset > lastCommitted {
			select {
			case c.commitCh <- commitEntry{
				partition: pid,
				offset:    offset,
				respCh:    nil,
			}:
			default:
				util.Warn("⚠️ Commit channel full (1024), dropping commit for P%d offset %d", pid, offset)
			}
		}
	}
	c.currentOffsets = make(map[int]uint64)
}

func (c *Consumer) processRetryQueue() {
	if atomic.LoadInt32(&c.rebalancing) == 1 {
		return
	}

	c.commitMu.Lock()
	if len(c.commitRetryMap) == 0 {
		c.commitMu.Unlock()
		return
	}

	toRetry := make(map[int]uint64)
	for p, o := range c.commitRetryMap {
		toRetry[p] = o
	}

	c.commitRetryMap = make(map[int]uint64)
	c.commitMu.Unlock()

	util.Debug("Retrying failed commits for %d partitions...", len(toRetry))

	if !c.sendBatchCommit(toRetry) {
		util.Error("❌ Retry batch commit failed. Re-queueing...")

		c.commitMu.Lock()
		for p, o := range toRetry {
			if current, ok := c.commitRetryMap[p]; !ok || o > current {
				c.commitRetryMap[p] = o
			}
		}
		c.commitMu.Unlock()
	}
}

func (c *Consumer) handleLeaderRedirection(resp string) {
	if strings.Contains(resp, "LEADER_IS") {
		// "ERROR NOT_LEADER LEADER_IS 192.168.0.10:9000"
		parts := strings.Fields(resp)
		for i, part := range parts {
			if part == "LEADER_IS" && i+1 < len(parts) {
				newLeader := parts[i+1]
				c.client.UpdateLeader(newLeader)
				util.Debug("update leader: %s", newLeader)
				break
			}
		}
	}
}

// heartbeatLoop runs in background; if coordinator indicates generation mismatch or error => trigger rejoin
func (c *Consumer) heartbeatLoop() {
	interval := time.Duration(c.config.HeartbeatIntervalMS) * time.Millisecond
	ticker := time.NewTicker(interval)
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
					util.Error("heartbeat could not get connection: %v", err)
					continue
				}

				c.hbMu.Lock()
				c.hbConn = newConn
				conn = c.hbConn
				c.hbMu.Unlock()
			}

			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			hb := fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d", c.config.Topic, c.config.GroupID, c.memberID, c.generation)
			if err := util.WriteWithLength(conn, util.EncodeMessage("", hb)); err != nil {
				util.Error("heartbeat send failed: %v", err)
				c.cleanupHbConn(conn)
				continue
			}

			resp, err := util.ReadWithLength(conn)
			_ = conn.SetDeadline(time.Time{})

			if err != nil {
				util.Error("heartbeat response failed: %v", err)
				c.cleanupHbConn(conn)
				continue
			}

			respStr := string(resp)
			if strings.Contains(respStr, "REBALANCE_REQUIRED") || strings.Contains(respStr, "GEN_MISMATCH") {
				util.Warn("heartbeat indicated rebalance/mismatch: %s", respStr)
				select {
				case c.rebalanceSig <- struct{}{}:
				default:
				}
				return
			}
		}
	}
}

func (c *Consumer) resetHeartbeatConn() {
	c.hbMu.Lock()
	if c.hbConn != nil {
		_ = c.hbConn.Close()
		c.hbConn = nil
	}
	c.hbMu.Unlock()
}

func (c *Consumer) handleRebalanceSignal() {
	if !atomic.CompareAndSwapInt32(&c.rebalancing, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&c.rebalancing, 0)

	util.Info("🔄 Rebalance started - Stop existing workers")

	if c.metrics != nil {
		c.metrics.RebalanceStart()
	}

	c.mainCancel()

	drainDone := make(chan struct{})
	go func() {
		maxTimeout := time.After(3 * time.Second)
		for {
			select {
			case <-c.commitCh:
			case <-time.After(100 * time.Millisecond):
				close(drainDone)
				return
			case <-maxTimeout:
				util.Warn("⚠️ Rebalance drain timeout exceeded, forcing continuation")
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
		pc.closeConnection()
	}
	c.partitionConsumers = make(map[int]*PartitionConsumer)
	c.offsets = make(map[int]uint64)
	c.mu.Unlock()

	c.mainCtx, c.mainCancel = context.WithCancel(context.Background())
	gen, mid, assignments, err := c.joinGroup()
	if err != nil {
		util.Error("Rebalance join failed: %v", err)
		return
	}

	if len(assignments) == 0 {
		assignments, err = c.syncGroup(gen, mid)
		if err != nil {
			util.Error("❌ Rebalance sync failed: %v", err)
			return
		}
	}

	c.mu.Lock()
	c.generation = gen
	c.memberID = mid

	for _, pid := range assignments {
		offset, err := c.fetchOffset(pid)
		if err != nil {
			util.Warn("⚠️ Partition %d offset fetch failed, starting from 0: %v", pid, err)
			offset = 0
		}

		pc := &PartitionConsumer{
			partitionID:  pid,
			consumer:     c,
			fetchOffset:  offset,
			commitOffset: offset,
		}
		c.partitionConsumers[pid] = pc
		util.Info("✅ Partition %d assigned at offset %d (Generation: %d)", pid, offset, gen)
	}
	c.mu.Unlock()

	if c.config.Mode != config.ModePolling {
		go c.startStreaming()
	} else {
		go c.startConsuming()
	}

	util.Info("🚀 Rebalance completed - All %d partitions are being consumed", len(assignments))
}

func (c *Consumer) startConsuming() {
	if atomic.LoadInt32(&c.rebalancing) == 1 {
		return
	}

	atomic.StoreInt32(&c.rebalancing, 1)
	defer atomic.StoreInt32(&c.rebalancing, 0)

	if c.config.EnableBenchmark {
		c.bmStartTime = time.Now()
	}

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
					util.Info("Partition %d's worker stopping due to rebalance", pid)
					return
				default:
					if !c.ownsPartition(pid) {
						util.Warn("Partition %d is no longer owned, stopping worker", pid)
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

func (c *Consumer) ownsPartition(pid int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pc, ok := c.partitionConsumers[pid]
	return ok && !pc.closed
}

func (c *Consumer) TriggerBenchmarkStop() {
	if !atomic.CompareAndSwapInt32(&c.bmFinished, 0, 1) {
		return
	}

	if c.metrics != nil {
		c.metrics.PrintSummary()
	}

	go func() {
		if err := c.Close(); err != nil {
			util.Debug("failed to close consumer: %v", err)
		}
	}()
}

func (c *Consumer) joinGroup() (generation int64, memberID string, assignments []int, err error) {
	conn, err := c.getLeaderConn()
	if err != nil {
		return 0, "", nil, err
	}
	defer func() { _ = conn.Close() }()

	mID := c.memberID
	if mID == "" {
		mID = c.config.ConsumerID
	}

	joinCmd := fmt.Sprintf("JOIN_GROUP topic=%s group=%s member=%s", c.config.Topic, c.config.GroupID, mID)
	if err := util.WriteWithLength(conn, util.EncodeMessage("", joinCmd)); err != nil {
		return 0, "", nil, fmt.Errorf("send join command: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return 0, "", nil, fmt.Errorf("read response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return 0, "", nil, fmt.Errorf("broker error: %s", respStr)
	}

	util.Info("join-group: %s\n", respStr)
	// "OK generation=123 member=consumer-abc-1242 waiting"
	var gen int64 = 0
	var mid string
	var assigned []int

	if strings.Contains(respStr, "generation=") && strings.Contains(respStr, "member=") {
		parts := strings.Fields(respStr)
		for _, part := range parts {
			if strings.HasPrefix(part, "generation=") {
				if n, err := fmt.Sscanf(part, "generation=%d", &gen); err != nil || n != 1 {
					util.Debug("failed to parse generation from %s: %v", part, err)
				}
			} else if strings.HasPrefix(part, "member=") {
				mid = strings.TrimPrefix(part, "member=")
			}
		}
	}

	if strings.Contains(respStr, "assignments=") {
		start := strings.Index(respStr, "[")
		end := strings.Index(respStr, "]")
		if start != -1 && end != -1 {
			partStr := respStr[start+1 : end]
			partStr = strings.ReplaceAll(partStr, ",", " ")
			parts := strings.Fields(partStr)

			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}

				pid, err := strconv.Atoi(p)
				if err == nil {
					assigned = append(assigned, pid)
				} else {
					util.Error("⚠️ Error parsing partition ID '%s': %v", p, err)
				}
			}
		}
	}

	return gen, mid, assigned, nil
}

func (c *Consumer) syncGroup(generation int64, memberID string) ([]int, error) {
	conn, err := c.getLeaderConn()
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	syncCmd := fmt.Sprintf("SYNC_GROUP topic=%s group=%s member=%s generation=%d", c.config.Topic, c.config.GroupID, memberID, generation)
	if err := util.WriteWithLength(conn, util.EncodeMessage("", syncCmd)); err != nil {
		return nil, fmt.Errorf("send sync command: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return nil, fmt.Errorf("read sync response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return nil, fmt.Errorf("broker error: %s", respStr)
	}

	util.Info("sync-group: %s\n", respStr)

	var assigned []int
	if strings.Contains(respStr, "[") && strings.Contains(respStr, "]") {
		start := strings.Index(respStr, "[")
		end := strings.Index(respStr, "]")
		partitionStr := respStr[start+1 : end]
		for _, p := range strings.Fields(partitionStr) {
			var pid int
			if _, err := fmt.Sscanf(p, "%d", &pid); err == nil {
				assigned = append(assigned, pid)
			}
		}
	}

	c.mu.Lock()
	c.generation = generation
	c.memberID = memberID
	c.mu.Unlock()

	return assigned, nil
}

func (c *Consumer) fetchOffset(partition int) (uint64, error) {
	if err := c.mainCtx.Err(); err != nil {
		return 0, err
	}

	conn, err := c.getLeaderConn()
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	fetchCmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s", c.config.Topic, partition, c.config.GroupID)
	if err := util.WriteWithLength(conn, util.EncodeMessage("", fetchCmd)); err != nil {
		return 0, fmt.Errorf("fetch offset send failed: %v", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return 0, fmt.Errorf("fetch offset response failed: %v", err)
	}

	respStr := string(resp)
	if strings.HasPrefix(respStr, "ERROR") {
		return 0, fmt.Errorf("fetch offset error: %s", respStr)
	}

	offset, err := strconv.ParseUint(strings.TrimSpace(respStr), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid offset response: %s", respStr)
	}

	return offset, nil
}

func (c *Consumer) startStreaming() {
	if c.config.EnableBenchmark {
		c.bmStartTime = time.Now()
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop()
	}()

	for _, pc := range c.partitionConsumers {
		c.wg.Add(1)
		go func(pc *PartitionConsumer) {
			defer c.wg.Done()
			pc.startStreamLoop()
		}(pc)
	}

	<-c.doneCh
}

func (c *Consumer) sendBatchCommit(offsets map[int]uint64) bool {
	c.commitMu.Lock()
	conn := c.commitConn
	needsNewConn := conn == nil || !c.validateCommitConn()
	c.commitMu.Unlock()

	if needsNewConn {
		newConn, err := c.getLeaderConn()
		if err != nil {
			util.Error("Batch commit failed to get connection: %v", err)
			return false
		}

		c.commitMu.Lock()
		c.commitConn = newConn
		conn = newConn
		c.commitMu.Unlock()
	}

	c.mu.RLock()
	generation := c.generation
	memberID := c.memberID
	c.mu.RUnlock()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("BATCH_COMMIT topic=%s group=%s generation=%d member=%s ", c.config.Topic, c.config.GroupID, generation, memberID))
	parts := []string{}
	for pid, off := range offsets {
		parts = append(parts, fmt.Sprintf("%d:%d", pid, off))
	}
	sb.WriteString(strings.Join(parts, ","))

	if err := util.WriteWithLength(conn, util.EncodeMessage("", sb.String())); err != nil {
		util.Error("Batch commit send failed: %v", err)
		c.commitMu.Lock()
		if c.commitConn == conn {
			c.commitConn = nil
			_ = conn.Close()
		}
		c.commitMu.Unlock()
		return false
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		util.Error("Batch commit response read failed: %v", err)
		c.commitMu.Lock()
		_ = conn.Close()
		c.commitConn = nil
		c.commitMu.Unlock()
		return false
	}

	respStr := string(resp)
	if strings.HasPrefix(respStr, "OK") {
		return true
	}

	if strings.Contains(respStr, "ERROR") || strings.Contains(respStr, "STALE_METADATA") {
		util.Error("Batch commit rejected: %s", respStr)

		if strings.Contains(respStr, "NOT_OWNER") || strings.Contains(respStr, "GEN_MISMATCH") || strings.Contains(respStr, "AUTHORIZED") {
			select {
			case c.rebalanceSig <- struct{}{}:
			default:
			}
		}
		return false
	}
	return false
}

func (c *Consumer) directCommit(partition int, offset uint64) error {
	c.mu.RLock()
	generation := c.generation
	memberID := c.memberID
	c.mu.RUnlock()

	conn, err := c.getLeaderConn()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	commitCmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d generation=%d member=%s",
		c.config.Topic, partition, c.config.GroupID, offset, generation, memberID)

	if err := util.WriteWithLength(conn, util.EncodeMessage("", commitCmd)); err != nil {
		return fmt.Errorf("direct commit send failed: %v", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("direct commit response failed: %v", err)
	}

	respStr := string(resp)
	if strings.Contains(respStr, "ERROR") {
		if strings.Contains(respStr, "GEN_MISMATCH") {
			go c.handleRebalanceSignal()
		}
		return fmt.Errorf("direct commit error: %s", respStr)
	}

	return nil
}

func (c *Consumer) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}

	close(c.doneCh)
	c.mainCancel()

	c.wg.Wait()
	c.flushOffsets()

	close(c.commitCh)
	c.resetHeartbeatConn()

	if c.memberID != "" {
		if conn, err := c.getLeaderConn(); err == nil {
			leaveCmd := fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s", c.config.Topic, c.config.GroupID, c.memberID)
			_ = util.WriteWithLength(conn, util.EncodeMessage("", leaveCmd))
			_ = conn.Close()
		}
	}

	c.commitMu.Lock()
	if c.commitConn != nil {
		if err := c.commitConn.Close(); err != nil {
			util.Debug("failed to close commit connection during shutdown: %v", err)
		}
		c.commitConn = nil
	}
	c.commitMu.Unlock()

	c.mu.Lock()
	for _, pc := range c.partitionConsumers {
		pc.mu.Lock()
		pc.closed = true
		if pc.conn != nil {
			if err := pc.conn.Close(); err != nil {
				util.Debug("failed to close connection: %v", err)
			}
			pc.conn = nil
		}
		pc.mu.Unlock()
	}
	c.partitionConsumers = make(map[int]*PartitionConsumer)
	c.mu.Unlock()

	return nil
}

func (c *Consumer) cleanupHbConn(badConn net.Conn) {
	_ = badConn.Close()
	c.hbMu.Lock()
	if c.hbConn == badConn {
		c.hbConn = nil
	}
	c.hbMu.Unlock()
}
