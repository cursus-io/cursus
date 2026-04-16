package sdk

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type consumerLeaderInfo struct {
	addr    string
	updated time.Time
}

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
	}
}

func (c *ConsumerClient) Connect(addr string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial failed to %s: %w", addr, err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetReadBuffer(2 * 1024 * 1024)
		tcpConn.SetWriteBuffer(2 * 1024 * 1024)
	}

	return conn, nil
}

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

	closed int32

	MessageHandler func(Message) error
}

type commitEntry struct {
	partition int
	offset    uint64
	respCh    chan error
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
		rebalanceSig:       make(chan struct{}, 1),
		doneCh:             make(chan struct{}),
		mainCtx:            ctx,
		mainCancel:         cancel,
	}

	c.commitCh = make(chan commitEntry, 1024)
	return c, nil
}

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

	LogInfo("✅ Successfully joined topic '%s' for group '%s' with %d partitions: %v (generation=%d, member=%s)",
		c.config.Topic, c.config.GroupID, len(assignments), assignments, gen, mid)

	c.mu.Lock()
	c.partitionConsumers = make(map[int]*PartitionConsumer)
	for _, pid := range assignments {
		offset, err := c.fetchOffset(pid)
		if err != nil {
			logWarn("Failed to fetch offset for P%d: %v", pid, err)
			offset = 0
		}

		c.offsets[pid] = offset

		pc := &PartitionConsumer{
			partitionID:  pid,
			consumer:     c,
			fetchOffset:  offset,
			commitOffset: offset,
		}
		c.partitionConsumers[pid] = pc
	}
	c.mu.Unlock()

	c.startCommitWorker()
	go c.rebalanceMonitorLoop()

	if c.config.Mode != ModeStreaming {
		go c.startConsuming()
	} else {
		go c.startStreaming()
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

			case <-c.doneCh:
				flush()
				return
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
			case c.commitCh <- commitEntry{
				partition: pid,
				offset:    offset,
				respCh:    nil,
			}:
			default:
			}
		}
	}
	c.currentOffsets = make(map[int]uint64)
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

func (c *Consumer) sendBatchCommit(offsets map[int]uint64) bool {
	c.commitMu.Lock()
	conn := c.commitConn
	if conn == nil {
		newConn, err := c.getLeaderConn()
		if err != nil {
			c.commitMu.Unlock()
			return false
		}
		c.commitConn = newConn
		conn = newConn
	}
	c.commitMu.Unlock()

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

	if err := WriteWithLength(conn, EncodeMessage("", sb.String())); err != nil {
		c.commitMu.Lock()
		if c.commitConn == conn {
			c.commitConn = nil
			_ = conn.Close()
		}
		c.commitMu.Unlock()
		return false
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		c.commitMu.Lock()
		if c.commitConn == conn {
			c.commitConn = nil
			_ = conn.Close()
		}
		c.commitMu.Unlock()
		return false
	}

	respStr := string(resp)
	if strings.HasPrefix(respStr, "OK") {
		return true
	}

	if strings.Contains(respStr, "REBALANCE_REQUIRED") || strings.Contains(respStr, "GEN_MISMATCH") {
		select {
		case c.rebalanceSig <- struct{}{}:
		default:
		}
	}
	return false
}

func (c *Consumer) getLeaderConn() (net.Conn, error) {
	conn, _, err := c.client.ConnectWithFailover()
	return conn, err
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

func (c *Consumer) handleRebalanceSignal() {
	if !atomic.CompareAndSwapInt32(&c.rebalancing, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&c.rebalancing, 0)

	LogInfo("🔄 Rebalance started - Stop existing workers")
	c.mainCancel()

	c.wg.Wait()

	c.hbMu.Lock()
	if c.hbConn != nil {
		_ = c.hbConn.Close()
		c.hbConn = nil
	}
	c.hbMu.Unlock()

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
	gen, mid, assignments, err := c.joinGroupWithRetry()
	if err != nil {
		logError("Rebalance join failed: %v", err)
		return
	}

	if len(assignments) == 0 {
		assignments, err = c.syncGroup(gen, mid)
		if err != nil {
			logError("❌ Rebalance sync failed: %v", err)
			return
		}
	}

	c.mu.Lock()
	c.generation = gen
	c.memberID = mid

	for _, pid := range assignments {
		offset, err := c.fetchOffset(pid)
		if err != nil {
			offset = 0
		}

		pc := &PartitionConsumer{
			partitionID:  pid,
			consumer:     c,
			fetchOffset:  offset,
			commitOffset: offset,
		}
		c.partitionConsumers[pid] = pc
	}
	c.mu.Unlock()

	if c.config.Mode != ModeStreaming {
		go c.startConsuming()
	} else {
		go c.startStreaming()
	}
}

func (c *Consumer) startConsuming() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop()
	}()

	c.mu.RLock()
	defer c.mu.RUnlock()
	for pid, pc := range c.partitionConsumers {
		c.wg.Add(1)
		go func(pid int, pc *PartitionConsumer) {
			defer c.wg.Done()

			for {
				select {
				case <-c.doneCh:
					return
				case <-c.mainCtx.Done():
					return
				default:
					pc.pollAndProcess()
					time.Sleep(c.config.PollInterval)
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
	defer c.mu.RUnlock()
	for _, pc := range c.partitionConsumers {
		c.wg.Add(1)
		go func(pc *PartitionConsumer) {
			defer c.wg.Done()
			pc.startStreamLoop()
		}(pc)
	}
}

func (c *Consumer) joinGroupWithRetry() (int64, string, []int, error) {
	for i := 0; i < 30; i++ {
		gen, mid, assignments, err := c.joinGroup()
		if err == nil {
			return gen, mid, assignments, nil
		}
		time.Sleep(1 * time.Second)
	}
	return 0, "", nil, fmt.Errorf("join group timeout")
}

func (c *Consumer) joinGroup() (int64, string, []int, error) {
	conn, err := c.getLeaderConn()
	if err != nil {
		return 0, "", nil, err
	}
	defer conn.Close()

	mID := c.memberID
	if mID == "" {
		mID = c.config.ConsumerID
	}

	joinCmd := fmt.Sprintf("JOIN_GROUP topic=%s group=%s member=%s", c.config.Topic, c.config.GroupID, mID)
	if err := WriteWithLength(conn, EncodeMessage("", joinCmd)); err != nil {
		return 0, "", nil, err
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return 0, "", nil, err
	}

	respStr := string(resp)
	if !strings.HasPrefix(respStr, "OK") {
		return 0, "", nil, fmt.Errorf("join failed: %s", respStr)
	}

	var gen int64
	var mid string
	var assigned []int

	parts := strings.Fields(respStr)
	for _, part := range parts {
		if strings.HasPrefix(part, "generation=") {
			fmt.Sscanf(part, "generation=%d", &gen)
		} else if strings.HasPrefix(part, "member=") {
			mid = strings.TrimPrefix(part, "member=")
		}
	}

	if strings.Contains(respStr, "assignments=") {
		start := strings.Index(respStr, "[")
		end := strings.Index(respStr, "]")
		if start != -1 && end != -1 {
			partStr := respStr[start+1 : end]
			for _, p := range strings.Fields(strings.ReplaceAll(partStr, ",", " ")) {
				pid, _ := strconv.Atoi(p)
				assigned = append(assigned, pid)
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
	defer conn.Close()

	syncCmd := fmt.Sprintf("SYNC_GROUP topic=%s group=%s member=%s generation=%d", c.config.Topic, c.config.GroupID, memberID, generation)
	if err := WriteWithLength(conn, EncodeMessage("", syncCmd)); err != nil {
		return nil, err
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return nil, err
	}

	respStr := string(resp)
	var assigned []int
	if strings.Contains(respStr, "[") && strings.Contains(respStr, "]") {
		start := strings.Index(respStr, "[")
		end := strings.Index(respStr, "]")
		partitionStr := respStr[start+1 : end]
		for _, p := range strings.Fields(partitionStr) {
			var pid int
			fmt.Sscanf(p, "%d", &pid)
			assigned = append(assigned, pid)
		}
	}

	return assigned, nil
}

func (c *Consumer) fetchOffset(partition int) (uint64, error) {
	conn, err := c.getLeaderConn()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	fetchCmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s", c.config.Topic, partition, c.config.GroupID)
	if err := WriteWithLength(conn, EncodeMessage("", fetchCmd)); err != nil {
		return 0, err
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return 0, err
	}

	return strconv.ParseUint(strings.TrimSpace(string(resp)), 10, 64)
}

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
			if conn == nil {
				var err error
				c.hbConn, err = c.getLeaderConn()
				conn = c.hbConn
				if err != nil {
					c.hbMu.Unlock()
					continue
				}
			}
			c.hbMu.Unlock()

			hb := fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d", c.config.Topic, c.config.GroupID, c.memberID, c.generation)
			if err := WriteWithLength(conn, EncodeMessage("", hb)); err != nil {
				c.hbMu.Lock()
				c.hbConn = nil
				conn.Close()
				c.hbMu.Unlock()
				continue
			}

			resp, err := ReadWithLength(conn)
			if err != nil {
				c.hbMu.Lock()
				c.hbConn = nil
				conn.Close()
				c.hbMu.Unlock()
				continue
			}

			respStr := string(resp)
			if strings.Contains(respStr, "REBALANCE_REQUIRED") || strings.Contains(respStr, "GEN_MISMATCH") {
				select {
				case c.rebalanceSig <- struct{}{}:
				default:
				}
				return
			}
		}
	}
}

func (c *Consumer) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}

	close(c.doneCh)
	c.mainCancel()
	c.wg.Wait()

	return nil
}

type PartitionConsumer struct {
	partitionID  int
	consumer     *Consumer
	fetchOffset  uint64
	commitOffset uint64
	conn         net.Conn
	mu           sync.Mutex
	closed       bool
}

func (pc *PartitionConsumer) ensureConnection() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		return nil
	}
	conn, err := pc.consumer.getLeaderConn()
	if err != nil {
		return err
	}
	pc.conn = conn
	return nil
}

func (pc *PartitionConsumer) closeConnection() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}

func (pc *PartitionConsumer) pollAndProcess() {
	if err := pc.ensureConnection(); err != nil {
		return
	}

	c := pc.consumer
	c.mu.RLock()
	memberID, generation := c.memberID, c.generation
	c.mu.RUnlock()

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s generation=%d member=%s",
		c.config.Topic, pc.partitionID, atomic.LoadUint64(&pc.fetchOffset), c.config.GroupID, generation, memberID)

	pc.mu.Lock()
	conn := pc.conn
	pc.mu.Unlock()

	if err := WriteWithLength(conn, EncodeMessage(c.config.Topic, consumeCmd)); err != nil {
		pc.closeConnection()
		return
	}

	data, err := ReadWithLength(conn)
	if err != nil {
		pc.closeConnection()
		return
	}

	if len(data) == 0 || strings.HasPrefix(string(data), "ERROR") {
		return
	}

	messages, _, _, err := DecodeBatchMessages(data)
	if err != nil {
		return
	}

	if len(messages) > 0 {
		for _, msg := range messages {
			if c.MessageHandler != nil {
				c.MessageHandler(msg)
			}
		}
		lastOffset := messages[len(messages)-1].Offset
		atomic.StoreUint64(&pc.fetchOffset, lastOffset+1)

		c.offsetsMu.Lock()
		c.currentOffsets[pc.partitionID] = lastOffset + 1
		c.offsetsMu.Unlock()
	}
}

func (pc *PartitionConsumer) startStreamLoop() {
	for {
		select {
		case <-pc.consumer.doneCh:
			pc.closeConnection()
			return
		default:
		}

		if atomic.LoadInt32(&pc.consumer.rebalancing) == 1 {
			pc.closeConnection()
			time.Sleep(1 * time.Second)
			continue
		}

		if err := pc.ensureConnection(); err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		c := pc.consumer
		c.mu.RLock()
		memberID, generation := c.memberID, c.generation
		c.mu.RUnlock()

		streamCmd := fmt.Sprintf("STREAM topic=%s partition=%d group=%s offset=%d generation=%d member=%s",
			c.config.Topic, pc.partitionID, c.config.GroupID, atomic.LoadUint64(&pc.fetchOffset), generation, memberID)

		pc.mu.Lock()
		conn := pc.conn
		pc.mu.Unlock()

		if err := WriteWithLength(conn, EncodeMessage("", streamCmd)); err != nil {
			logError("Failed to send STREAM command: %v", err)
			pc.closeConnection()
			time.Sleep(1 * time.Second) // 대기 추가
			continue
		}

		LogInfo("📡 Stream request sent for P%d at offset %d", pc.partitionID, atomic.LoadUint64(&pc.fetchOffset))

		for atomic.LoadInt32(&c.rebalancing) != 1 {
			data, err := ReadWithLength(conn)
			if err != nil {
				logError("Stream read error for P%d: %v", pc.partitionID, err)
				pc.closeConnection()
				time.Sleep(1 * time.Second)
				break
			}

			if len(data) == 0 {
				continue
			}

			if strings.HasPrefix(string(data), "ERROR") {
				logWarn("Stream received ERROR for P%d: %s", pc.partitionID, string(data))
				time.Sleep(2 * time.Second)
				break
			}

			messages, _, _, err := DecodeBatchMessages(data)
			if err != nil {
				logError("Failed to decode stream messages for P%d: %v", pc.partitionID, err)
				continue
			}

			if len(messages) > 0 {
				for _, msg := range messages {
					if c.MessageHandler != nil {
						c.MessageHandler(msg)
					}
				}
				lastOffset := messages[len(messages)-1].Offset
				atomic.StoreUint64(&pc.fetchOffset, lastOffset+1)

				c.offsetsMu.Lock()
				c.currentOffsets[pc.partitionID] = lastOffset + 1
				c.offsetsMu.Unlock()
			}
		}
	}
}
