package sdk

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

	"github.com/google/uuid"
)

// ─── ConsumerClient ──────────────────────────────────────────────────────────

type consumerLeaderInfo struct {
	addr    string
	updated time.Time
}

// ConsumerClient manages broker connections with leader-aware failover.
type ConsumerClient struct {
	ID     string
	config *ConsumerConfig
	leader atomic.Pointer[consumerLeaderInfo]
}

func NewConsumerClient(cfg *ConsumerConfig) *ConsumerClient {
	c := &ConsumerClient{
		ID:     uuid.New().String(),
		config: cfg,
	}
	c.leader.Store(&consumerLeaderInfo{addr: "", updated: time.Time{}})
	return c
}

func (c *ConsumerClient) UpdateLeader(addr string) {
	oldInfo := c.leader.Load()
	if oldInfo.addr != addr {
		c.leader.Store(&consumerLeaderInfo{
			addr:    addr,
			updated: time.Now(),
		})
		LogDebug("Updated leader: %s", addr)
	}
}

// Connect opens a TCP connection to addr with socket tuning applied.
func (c *ConsumerClient) Connect(addr string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial failed to %s: %w", addr, err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		_ = tcpConn.SetReadBuffer(2 * 1024 * 1024)
		_ = tcpConn.SetWriteBuffer(2 * 1024 * 1024)
	}

	return conn, nil
}

// ConnectWithFailover tries the cached leader first, then each broker in order.
func (c *ConsumerClient) ConnectWithFailover() (net.Conn, string, error) {
	addrs := c.config.BrokerAddrs
	if len(addrs) == 0 {
		return nil, "", fmt.Errorf("no broker addresses configured")
	}

	leaderAddr := ""
	if info := c.leader.Load(); info != nil && info.addr != "" && time.Since(info.updated) < c.config.LeaderStaleness {
		leaderAddr = info.addr
	}

	if leaderAddr != "" {
		if conn, err := c.Connect(leaderAddr); err == nil {
			return conn, leaderAddr, nil
		}
	}

	var lastErr error
	for _, addr := range addrs {
		if addr == leaderAddr {
			continue
		}
		conn, err := c.Connect(addr)
		if err == nil {
			c.UpdateLeader(addr)
			return conn, addr, nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return nil, "", fmt.Errorf("all brokers unreachable: %w", lastErr)
	}
	return nil, "", fmt.Errorf("all brokers unreachable")
}

type commitEntry struct {
	partition int
	offset    uint64
	respCh    chan error
}

// Consumer manages group membership, partition assignment, and message delivery.
type Consumer struct {
	config             *ConsumerConfig
	client             *ConsumerClient
	partitionConsumers map[int]*PartitionConsumer

	generation int64
	memberID   string

	commitConn     net.Conn
	commitCh       chan commitEntry
	commitMu       sync.Mutex
	commitRetryMap map[int]uint64

	currentOffsets map[int]uint64
	offsetsMu      sync.Mutex

	// wg tracks per-rebalance goroutines (heartbeat, partition workers, runWorkers).
	wg sync.WaitGroup
	// commitWg tracks the commit worker (long-lived, survives rebalances).
	commitWg sync.WaitGroup

	mainCtx    context.Context
	mainCancel context.CancelFunc

	rebalancing  int32
	rebalanceSig chan struct{}

	offsets map[int]uint64
	doneCh  chan struct{}
	mu      sync.RWMutex

	hbConn net.Conn
	hbMu   sync.Mutex

	closed int32

	MessageHandler func(Message) error
}

func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	client := NewConsumerClient(cfg)
	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		config:             cfg,
		client:             client,
		partitionConsumers: make(map[int]*PartitionConsumer),
		offsets:            make(map[int]uint64),
		currentOffsets:     make(map[int]uint64),
		commitRetryMap:     make(map[int]uint64),
		rebalanceSig:       make(chan struct{}, 1),
		doneCh:             make(chan struct{}),
		mainCtx:            ctx,
		mainCancel:         cancel,
	}

	c.commitCh = make(chan commitEntry, 1024)
	return c, nil
}

// Done returns a channel that is closed when the consumer is closed.
func (c *Consumer) Done() <-chan struct{} {
	return c.doneCh
}

// Start joins the consumer group, begins consuming, and blocks until Close is called.
func (c *Consumer) Start(handler func(Message) error) error {
	c.MessageHandler = handler

	gen, mid, assignments, err := c.joinGroupWithRetry()
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

	LogInfo("Joined group '%s' on topic '%s': %d partitions %v (gen=%d member=%s)",
		c.config.GroupID, c.config.Topic, len(assignments), assignments, gen, mid)

	c.mu.Lock()
	c.partitionConsumers = make(map[int]*PartitionConsumer)
	for _, pid := range assignments {
		offset, err := c.fetchOffset(pid)
		if err != nil {
			LogWarn("Failed to fetch offset for P%d: %v, starting from 0", pid, err)
			offset = 0
		}
		c.offsets[pid] = offset
		c.partitionConsumers[pid] = &PartitionConsumer{
			partitionID:  pid,
			consumer:     c,
			fetchOffset:  offset,
			commitOffset: offset,
		}
	}
	c.mu.Unlock()

	c.startCommitWorker()
	go c.rebalanceMonitorLoop()

	if c.config.Mode == ModeStreaming {
		go c.startStreaming()
	} else {
		go c.startConsuming()
	}

	<-c.mainCtx.Done()
	return nil
}

func (c *Consumer) startCommitWorker() {
	c.commitWg.Add(1)
	go func() {
		defer c.commitWg.Done()
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

			case <-c.doneCh:
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
						}
					default:
						flush()
						return
					}
				}
			}
		}
	}()
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
			case c.commitCh <- commitEntry{partition: pid, offset: offset}:
			default:
				LogWarn("commitCh full, dropping auto-commit for P%d offset %d", pid, offset)
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
	toRetry := make(map[int]uint64, len(c.commitRetryMap))
	for p, o := range c.commitRetryMap {
		toRetry[p] = o
	}
	c.commitRetryMap = make(map[int]uint64)
	c.commitMu.Unlock()

	LogDebug("Retrying failed commits for %d partitions", len(toRetry))
	if !c.sendBatchCommit(toRetry) {
		LogError("Retry batch commit failed, re-queuing")
		c.commitMu.Lock()
		for p, o := range toRetry {
			if current, ok := c.commitRetryMap[p]; !ok || o > current {
				c.commitRetryMap[p] = o
			}
		}
		c.commitMu.Unlock()
	}
}

func (c *Consumer) commitBatch(offsets map[int]uint64, respChannels map[int][]chan error) {
	success := c.sendBatchCommit(offsets)

	for pid, channels := range respChannels {
		var err error
		if !success {
			c.commitMu.Lock()
			if current, ok := c.commitRetryMap[pid]; !ok || offsets[pid] > current {
				c.commitRetryMap[pid] = offsets[pid]
			}
			c.commitMu.Unlock()
			err = fmt.Errorf("batch commit failed for partition %d", pid)
		}
		for _, ch := range channels {
			if ch != nil {
				ch <- err
			}
		}
	}
}

// validateCommitConn probes the commit connection with a 1ms deadline to detect stale connections.
func (c *Consumer) validateCommitConn() bool {
	if c.commitConn == nil {
		return false
	}
	if err := c.commitConn.SetReadDeadline(time.Now().Add(1 * time.Millisecond)); err != nil {
		_ = c.commitConn.Close()
		c.commitConn = nil
		return false
	}
	_, err := c.commitConn.Read(make([]byte, 0))
	if err != nil && !os.IsTimeout(err) {
		_ = c.commitConn.Close()
		c.commitConn = nil
		return false
	}
	_ = c.commitConn.SetReadDeadline(time.Time{})
	return true
}

func (c *Consumer) sendBatchCommit(offsets map[int]uint64) bool {
	c.commitMu.Lock()
	needsNewConn := c.commitConn == nil || !c.validateCommitConn()
	c.commitMu.Unlock()

	if needsNewConn {
		newConn, err := c.getLeaderConn()
		if err != nil {
			LogError("Batch commit: failed to get connection: %v", err)
			return false
		}
		c.commitMu.Lock()
		c.commitConn = newConn
		c.commitMu.Unlock()
	}

	c.commitMu.Lock()
	conn := c.commitConn
	c.commitMu.Unlock()

	c.mu.RLock()
	generation := c.generation
	memberID := c.memberID
	c.mu.RUnlock()

	var sb strings.Builder
	fmt.Fprintf(&sb, "BATCH_COMMIT topic=%s group=%s generation=%d member=%s ",
		c.config.Topic, c.config.GroupID, generation, memberID)
	parts := make([]string, 0, len(offsets))
	for pid, off := range offsets {
		parts = append(parts, fmt.Sprintf("%d:%d", pid, off))
	}
	sb.WriteString(strings.Join(parts, ","))

	if err := WriteWithLength(conn, EncodeMessage("", sb.String())); err != nil {
		LogError("Batch commit send failed: %v", err)
		c.commitMu.Lock()
		if c.commitConn == conn {
			_ = conn.Close()
			c.commitConn = nil
		}
		c.commitMu.Unlock()
		return false
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		LogError("Batch commit response failed: %v", err)
		c.commitMu.Lock()
		if c.commitConn == conn {
			_ = conn.Close()
			c.commitConn = nil
		}
		c.commitMu.Unlock()
		return false
	}

	respStr := string(resp)
	if strings.HasPrefix(respStr, "OK") {
		return true
	}

	c.handleLeaderRedirection(respStr)

	if strings.Contains(respStr, "NOT_OWNER") ||
		strings.Contains(respStr, "GEN_MISMATCH") ||
		strings.Contains(respStr, "REBALANCE_REQUIRED") {
		select {
		case c.rebalanceSig <- struct{}{}:
		default:
		}
	}

	LogError("Batch commit rejected: %s", respStr)
	return false
}

// directCommit sends a single COMMIT_OFFSET without going through the commit channel.
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

	if err := WriteWithLength(conn, EncodeMessage("", commitCmd)); err != nil {
		return fmt.Errorf("direct commit send: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("direct commit response: %w", err)
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

// handleLeaderRedirection parses a LEADER_IS response and updates the cached leader.
func (c *Consumer) handleLeaderRedirection(resp string) {
	if !strings.Contains(resp, "LEADER_IS") {
		return
	}
	fields := strings.Fields(resp)
	for i, f := range fields {
		if f == "LEADER_IS" && i+1 < len(fields) {
			c.client.UpdateLeader(fields[i+1])
			return
		}
	}
}

// joinGroupWithRetry retries joining with exponential backoff up to 30 attempts.
func (c *Consumer) joinGroupWithRetry() (int64, string, []int, error) {
	const maxAttempts = 30
	bo := newBackoff(1*time.Second, 5*time.Second)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		gen, mid, assignments, err := c.joinGroup()
		if err == nil {
			return gen, mid, assignments, nil
		}

		LogWarn("Join group attempt %d/%d failed: %v", attempt, maxAttempts, err)
		if attempt == maxAttempts {
			break
		}

		waitDur := bo.duration()
		select {
		case <-c.mainCtx.Done():
			return 0, "", nil, fmt.Errorf("consumer shutting down during join retry")
		case <-time.After(waitDur):
		}
	}
	return 0, "", nil, fmt.Errorf("failed to join group after %d attempts", maxAttempts)
}

func (c *Consumer) joinGroup() (int64, string, []int, error) {
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
	if err := WriteWithLength(conn, EncodeMessage("", joinCmd)); err != nil {
		return 0, "", nil, fmt.Errorf("send join: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return 0, "", nil, fmt.Errorf("read join response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if !strings.HasPrefix(respStr, "OK") {
		return 0, "", nil, fmt.Errorf("join rejected: %s", respStr)
	}

	var gen int64
	var mid string
	var assigned []int

	for _, part := range strings.Fields(respStr) {
		if strings.HasPrefix(part, "generation=") {
			_, _ = fmt.Sscanf(part, "generation=%d", &gen)
		} else if strings.HasPrefix(part, "member=") {
			mid = strings.TrimPrefix(part, "member=")
		}
	}

	if strings.Contains(respStr, "assignments=") {
		start := strings.Index(respStr, "[")
		end := strings.Index(respStr, "]")
		if start != -1 && end != -1 {
			partStr := strings.ReplaceAll(respStr[start+1:end], ",", " ")
			for _, p := range strings.Fields(partStr) {
				pid, err := strconv.Atoi(strings.TrimSpace(p))
				if err == nil {
					assigned = append(assigned, pid)
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

	syncCmd := fmt.Sprintf("SYNC_GROUP topic=%s group=%s member=%s generation=%d",
		c.config.Topic, c.config.GroupID, memberID, generation)
	if err := WriteWithLength(conn, EncodeMessage("", syncCmd)); err != nil {
		return nil, fmt.Errorf("send sync: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return nil, fmt.Errorf("read sync response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if !strings.HasPrefix(respStr, "OK") {
		return nil, fmt.Errorf("sync rejected: %s", respStr)
	}

	c.mu.Lock()
	c.generation = generation
	c.memberID = memberID
	c.mu.Unlock()

	var assigned []int
	if strings.Contains(respStr, "[") && strings.Contains(respStr, "]") {
		start := strings.Index(respStr, "[")
		end := strings.Index(respStr, "]")
		for _, p := range strings.Fields(respStr[start+1 : end]) {
			var pid int
			if _, err := fmt.Sscanf(p, "%d", &pid); err == nil {
				assigned = append(assigned, pid)
			}
		}
	}
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
	fetchCmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s",
		c.config.Topic, partition, c.config.GroupID)
	if err := WriteWithLength(conn, EncodeMessage("", fetchCmd)); err != nil {
		return 0, fmt.Errorf("fetch offset send: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return 0, fmt.Errorf("fetch offset response: %w", err)
	}

	respStr := string(resp)
	if strings.HasPrefix(respStr, "ERROR") {
		return 0, fmt.Errorf("fetch offset broker error: %s", respStr)
	}

	offset, err := strconv.ParseUint(strings.TrimSpace(respStr), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid offset response: %s", respStr)
	}
	return offset, nil
}

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

	LogInfo("Rebalance started — stopping existing workers")

	c.mainCancel()

	// Drain commitCh for up to 3 seconds concurrently with wg.Wait.
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

// ─── Close ────────────────────────────────────────────────────────────────────

func (c *Consumer) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return fmt.Errorf("%w", ErrConsumerClosed)
	}

	close(c.doneCh)
	c.wg.Wait()
	c.mainCancel()

	c.flushOffsets()
	close(c.commitCh)
	c.commitWg.Wait()

	// Send LEAVE_GROUP to broker.
	if c.memberID != "" {
		if conn, err := c.getLeaderConn(); err == nil {
			leaveCmd := fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s",
				c.config.Topic, c.config.GroupID, c.memberID)
			_ = WriteWithLength(conn, EncodeMessage("", leaveCmd))
			_ = conn.Close()
		}
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
	c.mu.Unlock()

	return nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func (c *Consumer) getLeaderConn() (net.Conn, error) {
	conn, _, err := c.client.ConnectWithFailover()
	return conn, err
}
