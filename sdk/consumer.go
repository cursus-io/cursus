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
)

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

	wg       sync.WaitGroup
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
	if cfg.EnableMetrics {
		initMetrics()
	}

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

// ─── Commit Worker ────────────────────────────────────────────────────────────

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

// ─── Group Protocol ───────────────────────────────────────────────────────────

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
