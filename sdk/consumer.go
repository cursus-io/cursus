package sdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
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

	generation      int64
	memberID        string
	coordinatorAddr string

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

	partitionLeaders map[int]string
	partitionMu      sync.RWMutex

	hbConn net.Conn
	hbMu   sync.Mutex

	closed int32

	MessageHandler func(Message) error
}

func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	if cfg.EnableMetrics {
		initMetrics()
	}

	client, err := NewConsumerClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer client: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		config:             cfg,
		client:             client,
		partitionConsumers: make(map[int]*PartitionConsumer),
		offsets:            make(map[int]uint64),
		currentOffsets:     make(map[int]uint64),
		partitionLeaders:   make(map[int]string),
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
	if err := validateSDKTopicName(c.config.Topic); err != nil {
		return err
	}
	c.MessageHandler = handler

	if coordAddr, err := c.findCoordinator(); err == nil {
		c.coordinatorAddr = coordAddr
		LogInfo("Coordinator for group '%s': %s", c.config.GroupID, coordAddr)
	}

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

	// Fetch offsets BEFORE holding the lock (fetchOffset calls getCoordinatorConn which needs mu.RLock)
	offsetMap := make(map[int]uint64)
	for _, pid := range assignments {
		offset, err := c.fetchOffsetWithRetry(pid)
		if err != nil {
			return fmt.Errorf("fetch offset for P%d failed: %w", pid, err)
		}
		offsetMap[pid] = offset
	}

	c.mu.Lock()
	c.partitionConsumers = make(map[int]*PartitionConsumer)
	for _, pid := range assignments {
		offset := offsetMap[pid]
		c.offsets[pid] = offset
		c.partitionConsumers[pid] = &PartitionConsumer{
			partitionID:  pid,
			consumer:     c,
			fetchOffset:  offset,
			commitOffset: offset,
		}
	}
	c.mu.Unlock()

	LogInfo("Fetching metadata for topic '%s'...", c.config.Topic)
	if err := c.fetchMetadata(); err != nil {
		LogWarn("Failed to fetch metadata, will rely on NOT_LEADER redirects: %v", err)
	} else {
		LogInfo("Partition leaders: %v", c.partitionLeaders)
	}
	LogInfo("Starting consume/stream workers...")

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
		newConn, err := c.getCoordinatorConn()
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
	partitions := make([]int, 0, len(offsets))
	for pid := range offsets {
		partitions = append(partitions, pid)
	}
	sort.Ints(partitions)
	parts := make([]string, 0, len(partitions))
	for _, pid := range partitions {
		parts = append(parts, fmt.Sprintf("P%d:%d", pid, offsets[pid]))
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
	if hasOKStatus(respStr) {
		return true
	}

	if c.handleNotCoordinator(respStr) {
		c.closeCommitConn(conn)
		LogWarn("Batch commit coordinator moved: %s", respStr)
		return false
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

func (c *Consumer) closeCommitConn(conn net.Conn) {
	c.commitMu.Lock()
	if c.commitConn == conn {
		_ = conn.Close()
		c.commitConn = nil
	}
	c.commitMu.Unlock()
}

func (c *Consumer) directCommit(partition int, offset uint64) error {
	c.mu.RLock()
	generation := c.generation
	memberID := c.memberID
	c.mu.RUnlock()

	conn, err := c.getCoordinatorConn()
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

	respStr := strings.TrimSpace(string(resp))
	if brokerErr, ok := ParseBrokerError(respStr); ok {
		if c.handleNotCoordinator(respStr) {
			return brokerErr
		}
		switch strings.ToUpper(brokerErr.Code) {
		case "GEN_MISMATCH", "NOT_OWNER", "REBALANCE_REQUIRED", "MEMBER_NOT_FOUND":
			go c.handleRebalanceSignal()
		}
		return brokerErr
	}
	if !hasOKStatus(respStr) {
		return fmt.Errorf("unexpected direct commit response: %s", respStr)
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

// ─── Metadata ─────────────────────────────────────────────────────────────────

func (c *Consumer) fetchMetadata() error {
	conn, _, err := c.client.ConnectWithFailover()
	if err != nil {
		return fmt.Errorf("connect for metadata: %w", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	cmd := fmt.Sprintf("METADATA topic=%s", c.config.Topic)
	if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
		return fmt.Errorf("send metadata: %w", err)
	}

	resp, err := ReadWithLength(conn)
	_ = conn.SetDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if !hasOKStatus(respStr) {
		return fmt.Errorf("metadata failed: %s", respStr)
	}

	var leadersStr string
	for _, part := range strings.Fields(respStr) {
		if strings.HasPrefix(part, "leaders=") {
			leadersStr = strings.TrimPrefix(part, "leaders=")
		}
	}
	if leadersStr == "" {
		return fmt.Errorf("metadata: missing leaders in response")
	}

	addrs := strings.Split(leadersStr, ",")
	c.partitionMu.Lock()
	for i, addr := range addrs {
		c.partitionLeaders[i] = addr
	}
	c.partitionMu.Unlock()

	return nil
}

func (c *Consumer) getPartitionLeaderAddr(partitionID int) string {
	c.partitionMu.RLock()
	defer c.partitionMu.RUnlock()
	return c.partitionLeaders[partitionID]
}

func (c *Consumer) updatePartitionLeader(partitionID int, addr string) {
	c.partitionMu.Lock()
	c.partitionLeaders[partitionID] = addr
	c.partitionMu.Unlock()
}

// ─── Metadata Refresh Loop ────────────────────────────────────────────────────

func (c *Consumer) metadataRefreshLoop() {
	interval := c.config.MetadataRefreshInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.doneCh:
			return
		case <-ticker.C:
			if err := c.fetchMetadata(); err != nil {
				LogDebug("Metadata refresh failed: %v", err)
			}
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
	conn, err := c.getCoordinatorConn()
	if err != nil {
		return 0, "", nil, err
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	c.mu.RLock()
	mID := c.memberID
	generation := c.generation
	c.mu.RUnlock()
	resuming := mID != "" && generation > 0
	if mID == "" {
		mID = c.config.ConsumerID
	}

	joinCmd := fmt.Sprintf("JOIN_GROUP topic=%s group=%s member=%s", c.config.Topic, c.config.GroupID, mID)
	if resuming {
		joinCmd += fmt.Sprintf(" generation=%d", generation)
	}
	if err := WriteWithLength(conn, EncodeMessage("", joinCmd)); err != nil {
		return 0, "", nil, fmt.Errorf("send join: %w", err)
	}

	resp, err := ReadWithLength(conn)
	_ = conn.SetDeadline(time.Time{})
	if err != nil {
		return 0, "", nil, fmt.Errorf("read join response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if c.handleNotCoordinator(respStr) {
		return 0, "", nil, fmt.Errorf("coordinator moved, retry")
	}
	if !hasOKStatus(respStr) {
		brokerErr, hasBrokerErr := ParseBrokerError(respStr)
		if resuming && hasBrokerErr && strings.EqualFold(brokerErr.Code, "GEN_MISMATCH") {
			currentText := brokerErr.Fields["current"]
			current, parseErr := strconv.ParseInt(currentText, 10, 64)
			if parseErr == nil {
				assignments, syncErr := c.syncGroup(current, mID)
				if syncErr == nil {
					return current, mID, assignments, nil
				}
			}
		}
		if resuming && hasBrokerErr && strings.EqualFold(brokerErr.Code, "member_not_found") {
			c.mu.Lock()
			if c.memberID == mID {
				c.memberID = ""
				c.generation = 0
			}
			c.mu.Unlock()
			return c.joinGroup()
		}
		return 0, "", nil, fmt.Errorf("join rejected: %s", respStr)
	}

	var gen int64
	var mid string

	for _, part := range strings.Fields(respStr) {
		if strings.HasPrefix(part, "generation=") {
			_, _ = fmt.Sscanf(part, "generation=%d", &gen)
		} else if strings.HasPrefix(part, "member=") {
			mid = strings.TrimPrefix(part, "member=")
		}
	}

	return gen, mid, parseGroupAssignments(respStr), nil
}

func (c *Consumer) syncGroup(generation int64, memberID string) ([]int, error) {
	conn, err := c.getCoordinatorConn()
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
	if !hasOKStatus(respStr) {
		return nil, fmt.Errorf("sync rejected: %s", respStr)
	}

	c.mu.Lock()
	c.generation = generation
	c.memberID = memberID
	c.mu.Unlock()

	return parseGroupAssignments(respStr), nil
}

func parseGroupAssignments(resp string) []int {
	start := strings.Index(resp, "[")
	end := strings.Index(resp, "]")
	if start == -1 || end <= start {
		return nil
	}

	partitionText := strings.ReplaceAll(resp[start+1:end], ",", " ")
	assignments := make([]int, 0)
	for _, field := range strings.Fields(partitionText) {
		partition, err := strconv.Atoi(field)
		if err == nil {
			assignments = append(assignments, partition)
		}
	}
	return assignments
}

func (c *Consumer) fetchOffsetWithRetry(partition int) (uint64, error) {
	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		offset, err := c.fetchOffset(partition)
		if err == nil {
			return offset, nil
		}
		lastErr = err
		if !isRetryableFetchOffsetError(err) {
			return 0, err
		}
		if attempt == 5 {
			break
		}
		select {
		case <-c.mainCtx.Done():
			return 0, c.mainCtx.Err()
		case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
		}
	}
	return 0, lastErr
}

func (c *Consumer) fetchOffset(partition int) (uint64, error) {
	if err := c.mainCtx.Err(); err != nil {
		return 0, err
	}

	conn, err := c.getCoordinatorConn()
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

	respStr := strings.TrimSpace(string(resp))
	if brokerErr, ok := ParseBrokerError(respStr); ok {
		_ = c.handleNotCoordinator(respStr)
		return 0, brokerErr
	}
	return parseFetchOffsetResponse(respStr)
}

func isRetryableFetchOffsetError(err error) bool {
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		return false
	}
	if brokerErr.Retryable {
		return true
	}
	switch strings.ToLower(brokerErr.Code) {
	case "group_not_found", "member_not_found", "not_coordinator":
		return true
	default:
		return false
	}
}

func parseFetchOffsetResponse(respStr string) (uint64, error) {
	if brokerErr, ok := ParseBrokerError(respStr); ok {
		return 0, brokerErr
	}
	fields, err := parseOKResponse(respStr)
	if err != nil {
		return 0, fmt.Errorf("unexpected offset response: %s", respStr)
	}
	offsetValue := fields["offset"]
	if offsetValue == "" {
		return 0, fmt.Errorf("missing offset in response: %s", respStr)
	}
	offset, err := strconv.ParseUint(offsetValue, 10, 64)
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
	// Cancel the active consume context and close live sockets so blocked I/O exits promptly.
	c.mainCancel()
	c.closeActiveConnections()
	c.wg.Wait()

	c.flushOffsets()
	close(c.commitCh)
	c.commitWg.Wait()

	c.mu.RLock()
	memberID := c.memberID
	generation := c.generation
	c.mu.RUnlock()
	if memberID != "" && generation > 0 {
		if conn, err := c.getCoordinatorConn(); err == nil {
			leaveCmd := fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
				c.config.Topic, c.config.GroupID, memberID, generation)
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

func (c *Consumer) closeActiveConnections() {
	c.mu.RLock()
	pcs := make([]*PartitionConsumer, 0, len(c.partitionConsumers))
	for _, pc := range c.partitionConsumers {
		pcs = append(pcs, pc)
	}
	c.mu.RUnlock()

	for _, pc := range pcs {
		pc.closeConnection()
	}

	c.resetHeartbeatConn()

	c.commitMu.Lock()
	if c.commitConn != nil {
		_ = c.commitConn.Close()
		c.commitConn = nil
	}
	c.commitMu.Unlock()
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func (c *Consumer) getLeaderConn() (net.Conn, error) {
	conn, _, err := c.client.ConnectWithFailover()
	return conn, err
}

func (c *Consumer) findCoordinator() (string, error) {
	conn, _, err := c.client.ConnectWithFailover()
	if err != nil {
		return "", fmt.Errorf("connect for find_coordinator: %w", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	cmd := fmt.Sprintf("FIND_COORDINATOR group=%s", c.config.GroupID)
	if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
		return "", fmt.Errorf("send find_coordinator: %w", err)
	}

	resp, err := ReadWithLength(conn)
	_ = conn.SetDeadline(time.Time{})
	if err != nil {
		return "", fmt.Errorf("read find_coordinator: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if brokerErr, ok := ParseBrokerError(respStr); ok {
		return "", brokerErr
	}
	fields, err := parseOKResponse(respStr)
	if err != nil {
		return "", fmt.Errorf("find_coordinator failed: %s", respStr)
	}

	host, port := fields["host"], fields["port"]
	if host == "" || port == "" {
		return "", fmt.Errorf("find_coordinator: missing host/port in response: %s", respStr)
	}
	return c.coordinatorAddrFromHostPort(host, port), nil
}

func (c *Consumer) getCoordinatorConn() (net.Conn, error) {
	c.mu.RLock()
	addr := c.coordinatorAddr
	c.mu.RUnlock()

	if addr != "" {
		conn, err := c.client.ConnectToAddr(addr)
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			return conn, nil
		}
		LogWarn("Coordinator %s unreachable: %v, rediscovering", addr, err)
	}

	newAddr, err := c.findCoordinator()
	if err != nil {
		return c.getLeaderConn()
	}
	c.mu.Lock()
	c.coordinatorAddr = newAddr
	c.mu.Unlock()
	conn, err := c.client.ConnectToAddr(newAddr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	return conn, nil
}

func (c *Consumer) coordinatorAddrFromHostPort(host, port string) string {
	if isLoopbackCoordinatorHost(host) && len(c.config.BrokerAddrs) > 0 {
		if bootstrapHost, _, err := net.SplitHostPort(c.config.BrokerAddrs[0]); err == nil && !isLoopbackCoordinatorHost(bootstrapHost) {
			host = bootstrapHost
		}
	}
	return net.JoinHostPort(host, port)
}

func isLoopbackCoordinatorHost(host string) bool {
	switch strings.ToLower(strings.TrimSpace(host)) {
	case "localhost", "127.0.0.1", "::1", "[::1]":
		return true
	default:
		return false
	}
}

func (c *Consumer) handleNotCoordinator(respStr string) bool {
	brokerErr, ok := ParseBrokerError(strings.TrimSpace(respStr))
	if !ok || !strings.EqualFold(brokerErr.Code, "NOT_COORDINATOR") {
		return false
	}
	host, port := brokerErr.Fields["host"], brokerErr.Fields["port"]
	if host != "" && port != "" {
		newAddr := c.coordinatorAddrFromHostPort(host, port)
		c.mu.Lock()
		c.coordinatorAddr = newAddr
		c.mu.Unlock()
		LogInfo("Coordinator moved to %s", newAddr)
	}
	return true
}
