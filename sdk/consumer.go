package sdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/wire"
)

type commitEntry struct {
	partition            int
	offset               uint64
	assignmentGeneration uint64
	respCh               chan error
}

type retryCommit struct {
	offset               uint64
	assignmentGeneration uint64
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
	commitRetryMap map[int]retryCommit

	currentOffsets           map[int]uint64
	currentOffsetGenerations map[int]uint64
	offsetsMu                sync.Mutex

	wg          sync.WaitGroup
	commitWg    sync.WaitGroup
	lifecycleWg sync.WaitGroup
	lifecycleMu sync.Mutex
	failureMu   sync.Mutex
	failureErr  error

	mainCtx    context.Context
	mainCancel context.CancelFunc
	rootCtx    context.Context
	rootCancel context.CancelFunc

	state                atomic.Uint32
	assignmentGeneration atomic.Uint64
	rebalanceSig         chan struct{}

	offsets   map[int]uint64
	doneCh    chan struct{}
	closeDone chan struct{}
	closeErr  error
	mu        sync.RWMutex

	partitionLeaders  map[int]string
	partitionMu       sync.RWMutex
	compactionEnabled atomic.Bool

	hbConn net.Conn
	hbMu   sync.Mutex

	MessageHandler func(Message) error
}

// TransactionalOffsetMetadata is the active group membership required by
// SEND_OFFSETS_TO_TXN. It is exposed for broker-native processors such as the
// Saga runtime; callers must obtain a fresh value for each input because a
// rebalance fences the previous member/generation pair.
type TransactionalOffsetMetadata struct {
	Topic      string
	Group      string
	Member     string
	Generation int
}

// TransactionalOffsetMetadata returns the consumer's currently assigned group
// generation. It fails before the consumer joins a group or after a rebalance
// has cleared the active membership.
func (c *Consumer) TransactionalOffsetMetadata() (TransactionalOffsetMetadata, error) {
	if c == nil || c.config == nil {
		return TransactionalOffsetMetadata{}, fmt.Errorf("consumer is not initialized")
	}
	c.mu.RLock()
	member, generation := c.memberID, c.generation
	c.mu.RUnlock()
	if member == "" || generation <= 0 {
		return TransactionalOffsetMetadata{}, fmt.Errorf("consumer has no active group assignment")
	}
	return TransactionalOffsetMetadata{Topic: c.config.Topic, Group: c.config.GroupID, Member: member, Generation: int(generation)}, nil
}

func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	return NewConsumerWithContext(context.Background(), cfg)
}

// NewConsumerWithContext creates a consumer whose worker lifecycle is bounded by ctx.
// Rebalances derive replacement workers from the same root cancellation source.
func NewConsumerWithContext(ctx context.Context, cfg *ConsumerConfig) (*Consumer, error) {
	if ctx == nil {
		return nil, fmt.Errorf("consumer context must not be nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.EnableMetrics {
		initMetrics()
	}

	client, err := NewConsumerClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer client: %w", err)
	}
	rootCtx, rootCancel := context.WithCancel(ctx)
	workerCtx, cancel := context.WithCancel(rootCtx)

	c := &Consumer{
		config:                   cfg,
		client:                   client,
		partitionConsumers:       make(map[int]*PartitionConsumer),
		offsets:                  make(map[int]uint64),
		currentOffsets:           make(map[int]uint64),
		currentOffsetGenerations: make(map[int]uint64),
		partitionLeaders:         make(map[int]string),
		commitRetryMap:           make(map[int]retryCommit),
		rebalanceSig:             make(chan struct{}, 1),
		doneCh:                   make(chan struct{}),
		closeDone:                make(chan struct{}),
		mainCtx:                  workerCtx,
		rootCtx:                  rootCtx,
		rootCancel:               rootCancel,
		mainCancel:               cancel,
	}

	c.commitCh = make(chan commitEntry, 1024)
	return c, nil
}

// Done returns a channel that is closed when the consumer is closed.
func (c *Consumer) Done() <-chan struct{} {
	return c.doneCh
}

// CommitOffset persists the next offset for an assigned partition immediately.
//
// It is intended for consumers that disable EnableAutoCommit because they need
// an external durable side effect to complete before acknowledging a message.
// Call it with message.Offset + 1 only after that side effect has succeeded.
// The method waits for the broker acknowledgement and returns an error without
// advancing the local committed offset when the commit is rejected.
func (c *Consumer) CommitOffset(partition int, offset uint64) error {
	if offset == 0 {
		return fmt.Errorf("commit offset must be greater than zero")
	}
	if err := c.mainCtx.Err(); err != nil {
		return fmt.Errorf("commit cancelled: %w", err)
	}

	c.mu.RLock()
	pc, assigned := c.partitionConsumers[partition]
	c.mu.RUnlock()
	if !assigned {
		return fmt.Errorf("partition %d is not assigned to this consumer", partition)
	}

	if err := pc.commitOffsetWithRetry(offset); err != nil {
		return err
	}

	atomic.StoreUint64(&pc.commitOffset, offset)
	c.mu.Lock()
	if current := c.offsets[partition]; offset > current {
		c.offsets[partition] = offset
	}
	c.mu.Unlock()
	return nil
}

// Start joins the consumer group, begins consuming, and blocks until Close is called.
func (c *Consumer) Start(handler func(Message) error) error {
	if err := c.beginStart(); err != nil {
		return err
	}
	started := false
	defer func() {
		if !started {
			_ = c.Close()
		}
	}()
	if handler == nil {
		return ErrConsumerHandlerRequired
	}
	if err := c.rootCtx.Err(); err != nil {
		return fmt.Errorf("consumer context is already done: %w", err)
	}
	if err := validateSDKTopicName(c.config.Topic); err != nil {
		return err
	}
	c.MessageHandler = handler

	if c.config.CoordinatorAddr != "" {
		c.mu.Lock()
		c.coordinatorAddr = c.config.CoordinatorAddr
		c.mu.Unlock()
		LogInfo("Using configured coordinator for group '%s': %s", c.config.GroupID, c.coordinatorAddr)
	} else if coordAddr, err := c.findCoordinatorContext(c.assignmentContext()); err == nil {
		c.mu.Lock()
		c.coordinatorAddr = coordAddr
		c.mu.Unlock()
		LogInfo("Coordinator for group '%s': %s", c.config.GroupID, coordAddr)
	}

	gen, mid, assignments, err := c.joinGroupWithRetry()
	if err != nil {
		return fmt.Errorf("join group failed: %w", err)
	}
	c.mu.Lock()
	c.generation = gen
	c.memberID = mid
	c.mu.Unlock()

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

	assignmentGeneration := c.assignmentGeneration.Add(1)
	c.mu.Lock()
	c.partitionConsumers = make(map[int]*PartitionConsumer)
	for _, pid := range assignments {
		offset := offsetMap[pid]
		c.offsets[pid] = offset
		c.partitionConsumers[pid] = &PartitionConsumer{
			partitionID:          pid,
			consumer:             c,
			fetchOffset:          offset,
			commitOffset:         offset,
			assignmentGeneration: assignmentGeneration,
			ctx:                  c.assignmentContext(),
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
	c.startLifecycleWorker(c.rebalanceMonitorLoop)
	c.startAssignmentWorkers(c.assignmentContext(), assignmentGeneration)
	started = true

	return c.waitForShutdown()
}

func (c *Consumer) waitForShutdown() error {
	<-c.rootCtx.Done()
	return errors.Join(c.Err(), c.Close())
}

// ─── Commit Worker ────────────────────────────────────────────────────────────

func (c *Consumer) startCommitWorker() {
	c.lifecycleMu.Lock()
	state := c.State()
	if state != ConsumerStateRunning {
		c.lifecycleMu.Unlock()
		return
	}
	c.commitWg.Add(1)
	c.lifecycleMu.Unlock()
	go func() {
		defer c.commitWg.Done()
		interval := c.config.AutoCommitInterval
		if interval <= 0 {
			interval = 5 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		pendingOffsets := make(map[int]uint64)
		respChannels := make(map[int][]chan error)
		var pendingGeneration uint64

		flush := func() {
			if len(pendingOffsets) > 0 {
				c.commitBatch(pendingOffsets, respChannels, pendingGeneration)
				pendingOffsets = make(map[int]uint64)
				respChannels = make(map[int][]chan error)
				pendingGeneration = 0
			}
		}

		for {
			select {
			case entry, ok := <-c.commitCh:
				if !ok {
					flush()
					return
				}
				if !c.assignmentActive(entry.assignmentGeneration) {
					if entry.respCh != nil {
						entry.respCh <- ErrConsumerRebalancing
					}
					continue
				}
				if pendingGeneration != 0 && pendingGeneration != entry.assignmentGeneration {
					flush()
				}
				pendingGeneration = entry.assignmentGeneration
				if existing, exists := pendingOffsets[entry.partition]; !exists || entry.offset > existing {
					pendingOffsets[entry.partition] = entry.offset
				}
				if entry.respCh != nil {
					respChannels[entry.partition] = append(respChannels[entry.partition], entry.respCh)
					flush()
				}

			case <-ticker.C:
				flush()
				if c.config.EnableAutoCommit {
					c.flushAutoCommitOffsets(false)
				}
				c.processRetryQueue()

			case <-c.doneCh:
				for {
					select {
					case entry, ok := <-c.commitCh:
						if !ok {
							flush()
							return
						}
						if !c.assignmentActive(entry.assignmentGeneration) {
							if entry.respCh != nil {
								entry.respCh <- ErrConsumerRebalancing
							}
							continue
						}
						if pendingGeneration != 0 && pendingGeneration != entry.assignmentGeneration {
							flush()
						}
						pendingGeneration = entry.assignmentGeneration
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
	assignmentGeneration := c.assignmentGeneration.Load()
	if !c.assignmentActive(assignmentGeneration) {
		return
	}

	c.offsetsMu.Lock()
	defer c.offsetsMu.Unlock()

	if len(c.currentOffsets) == 0 {
		return
	}

	for pid, offset := range c.currentOffsets {
		generation := c.currentOffsetGenerations[pid]
		if generation == 0 {
			generation = assignmentGeneration
		}
		if generation != assignmentGeneration {
			delete(c.currentOffsets, pid)
			delete(c.currentOffsetGenerations, pid)
			continue
		}
		c.mu.RLock()
		lastCommitted := c.offsets[pid]
		c.mu.RUnlock()

		if offset > lastCommitted {
			select {
			case c.commitCh <- commitEntry{partition: pid, offset: offset, assignmentGeneration: assignmentGeneration}:
			default:
				LogWarn("commitCh full, dropping auto-commit for P%d offset %d", pid, offset)
			}
		}
	}
	c.currentOffsets = make(map[int]uint64)
	c.currentOffsetGenerations = make(map[int]uint64)
}

func (c *Consumer) recordAutoCommitOffset(partition int, offset, assignmentGeneration uint64) {
	c.offsetsMu.Lock()
	defer c.offsetsMu.Unlock()
	if c.currentOffsets == nil {
		c.currentOffsets = make(map[int]uint64)
	}
	if c.currentOffsetGenerations == nil {
		c.currentOffsetGenerations = make(map[int]uint64)
	}
	if currentGeneration := c.currentOffsetGenerations[partition]; currentGeneration != 0 && currentGeneration != assignmentGeneration {
		delete(c.currentOffsets, partition)
	}
	if current, ok := c.currentOffsets[partition]; !ok || offset > current {
		c.currentOffsets[partition] = offset
		c.currentOffsetGenerations[partition] = assignmentGeneration
	}
}

func (c *Consumer) flushAutoCommitOffsets(allowClosing bool) bool {
	assignmentGeneration := c.assignmentGeneration.Load()
	if !c.assignmentCanCommit(assignmentGeneration, allowClosing) {
		return false
	}

	c.offsetsMu.Lock()
	offsets := make(map[int]uint64)
	for partition, offset := range c.currentOffsets {
		generation := c.currentOffsetGenerations[partition]
		if generation == 0 {
			generation = assignmentGeneration
		}
		if generation == assignmentGeneration {
			offsets[partition] = offset
		}
	}
	c.offsetsMu.Unlock()
	if len(offsets) == 0 {
		return true
	}

	if !c.sendBatchCommitWithState(offsets, assignmentGeneration, allowClosing) {
		return false
	}
	c.offsetsMu.Lock()
	for partition, committed := range offsets {
		generation := c.currentOffsetGenerations[partition]
		if generation == assignmentGeneration && c.currentOffsets[partition] <= committed {
			delete(c.currentOffsets, partition)
			delete(c.currentOffsetGenerations, partition)
		}
	}
	c.offsetsMu.Unlock()
	c.recordCommittedOffsets(offsets, assignmentGeneration)
	return true
}

func (c *Consumer) processRetryQueue() {
	if c.State() != ConsumerStateRunning {
		return
	}

	c.commitMu.Lock()
	if len(c.commitRetryMap) == 0 {
		c.commitMu.Unlock()
		return
	}
	assignmentGeneration := c.assignmentGeneration.Load()
	toRetry := make(map[int]uint64, len(c.commitRetryMap))
	for partition, entry := range c.commitRetryMap {
		if entry.assignmentGeneration == assignmentGeneration {
			toRetry[partition] = entry.offset
		}
	}
	c.commitRetryMap = make(map[int]retryCommit)
	c.commitMu.Unlock()

	// A later commit can succeed while an older failed commit remains queued.
	// Replaying that older offset would be rejected as a regression forever.
	c.mu.RLock()
	for partition, offset := range toRetry {
		if c.offsets[partition] >= offset {
			delete(toRetry, partition)
		}
	}
	c.mu.RUnlock()

	LogDebug("Retrying failed commits for %d partitions", len(toRetry))
	if len(toRetry) > 0 {
		if c.sendBatchCommit(toRetry, assignmentGeneration) {
			c.recordCommittedOffsets(toRetry, assignmentGeneration)
		} else {
			LogError("Retry batch commit failed, re-queuing")
			c.commitMu.Lock()
			for partition, offset := range toRetry {
				if current, ok := c.commitRetryMap[partition]; !ok || offset > current.offset {
					c.commitRetryMap[partition] = retryCommit{offset: offset, assignmentGeneration: assignmentGeneration}
				}
			}
			c.commitMu.Unlock()
		}
	}
}

func (c *Consumer) commitBatch(offsets map[int]uint64, respChannels map[int][]chan error, assignmentGeneration uint64) {
	success := c.sendBatchCommit(offsets, assignmentGeneration)
	if success {
		c.recordCommittedOffsets(offsets, assignmentGeneration)
	} else {
		c.commitMu.Lock()
		if c.assignmentActive(assignmentGeneration) {
			for partition, offset := range offsets {
				if current, ok := c.commitRetryMap[partition]; !ok || offset > current.offset {
					c.commitRetryMap[partition] = retryCommit{offset: offset, assignmentGeneration: assignmentGeneration}
				}
			}
		}
		c.commitMu.Unlock()
	}

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

func (c *Consumer) sendBatchCommit(offsets map[int]uint64, assignmentGeneration uint64) bool {
	return c.sendBatchCommitWithState(offsets, assignmentGeneration, false)
}

func (c *Consumer) sendBatchCommitWithState(offsets map[int]uint64, assignmentGeneration uint64, allowClosing bool) bool {
	if !c.assignmentCanCommit(assignmentGeneration, allowClosing) {
		return false
	}
	requestParent := c.assignmentContext()
	if allowClosing {
		requestParent = context.Background()
	}
	requestCtx, cancelRequest := boundedRequestContext(requestParent, c.requestTimeout())
	defer cancelRequest()
	c.commitMu.Lock()
	needsNewConn := c.commitConn == nil || !c.validateCommitConn()
	c.commitMu.Unlock()

	if needsNewConn {
		newConn, err := c.getCoordinatorConnContext(requestCtx)
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
	cleanup, err := bindConnectionToContext(requestCtx, conn)
	if err != nil {
		c.closeCommitConn(conn)
		return false
	}
	defer cleanup()

	c.mu.RLock()
	generation := c.generation
	memberID := c.memberID
	c.mu.RUnlock()

	pairs := make([]wire.OffsetPair, 0, len(offsets))
	for partition, offset := range offsets {
		pairs = append(pairs, wire.OffsetPair{Partition: partition, Offset: offset})
	}
	encodedOffsets, err := wire.EncodeOffsetPairs(pairs)
	if err != nil {
		LogError("Batch commit: invalid offsets: %v", err)
		return false
	}
	command := fmt.Sprintf("BATCH_COMMIT topic=%s group=%s generation=%d member=%s offsets=%s",
		c.config.Topic, c.config.GroupID, generation, memberID, encodedOffsets)

	c.lifecycleMu.Lock()
	if !c.assignmentCanCommit(assignmentGeneration, allowClosing) {
		c.lifecycleMu.Unlock()
		return false
	}
	err = WriteWithLength(conn, []byte(command))
	c.lifecycleMu.Unlock()
	if err != nil {
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
		var brokerErr *BrokerError
		if errors.As(err, &brokerErr) {
			if c.handleNotCoordinatorError(brokerErr) {
				c.closeCommitConn(conn)
				LogWarn("Batch commit coordinator moved: %v", brokerErr)
				return false
			}
			switch strings.ToUpper(brokerErr.Code) {
			case "NOT_OWNER", "GEN_MISMATCH", "REBALANCE_REQUIRED", "MEMBER_NOT_FOUND":
				select {
				case c.rebalanceSig <- struct{}{}:
				default:
				}
			}
			LogError("Batch commit rejected: %v", brokerErr)
			return false
		}
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

	LogError("Batch commit rejected: %s", respStr)
	return false
}

func (c *Consumer) assignmentCanCommit(generation uint64, allowClosing bool) bool {
	if generation == 0 || c.assignmentGeneration.Load() != generation {
		return false
	}
	state := c.State()
	return state == ConsumerStateRunning || (allowClosing && state == ConsumerStateClosing)
}

func (c *Consumer) recordCommittedOffsets(offsets map[int]uint64, assignmentGeneration uint64) {
	if c.assignmentGeneration.Load() != assignmentGeneration {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for partition, offset := range offsets {
		if offset > c.offsets[partition] {
			c.offsets[partition] = offset
		}
		if pc := c.partitionConsumers[partition]; pc != nil && pc.assignmentGeneration == assignmentGeneration {
			atomic.StoreUint64(&pc.commitOffset, offset)
		}
	}
}

func (c *Consumer) closeCommitConn(conn net.Conn) {
	c.commitMu.Lock()
	if c.commitConn == conn {
		_ = conn.Close()
		c.commitConn = nil
	}
	c.commitMu.Unlock()
}

func (c *Consumer) directCommit(partition int, offset uint64, assignmentGeneration uint64) error {
	if !c.assignmentActive(assignmentGeneration) {
		return ErrConsumerRebalancing
	}
	c.mu.RLock()
	generation := c.generation
	memberID := c.memberID
	c.mu.RUnlock()

	requestCtx, cancelRequest := boundedRequestContext(c.assignmentContext(), c.requestTimeout())
	defer cancelRequest()
	conn, err := c.getCoordinatorConnContext(requestCtx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	cleanup, err := bindConnectionToContext(requestCtx, conn)
	if err != nil {
		return err
	}
	defer cleanup()

	commitCmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d generation=%d member=%s",
		c.config.Topic, partition, c.config.GroupID, offset, generation, memberID)

	c.lifecycleMu.Lock()
	if !c.assignmentActive(assignmentGeneration) {
		c.lifecycleMu.Unlock()
		return ErrConsumerRebalancing
	}
	err = WriteWithLength(conn, []byte(commitCmd))
	c.lifecycleMu.Unlock()
	if err != nil {
		return fmt.Errorf("direct commit send: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		var brokerErr *BrokerError
		if errors.As(err, &brokerErr) {
			_ = c.handleNotCoordinatorError(brokerErr)
			switch strings.ToUpper(brokerErr.Code) {
			case "GEN_MISMATCH", "NOT_OWNER", "REBALANCE_REQUIRED", "MEMBER_NOT_FOUND":
				select {
				case c.rebalanceSig <- struct{}{}:
				default:
				}
			}
			return brokerErr
		}
		return fmt.Errorf("direct commit response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if !hasOKStatus(respStr) {
		return fmt.Errorf("unexpected direct commit response: %s", respStr)
	}
	return nil
}

// ─── Metadata ─────────────────────────────────────────────────────────────────

func (c *Consumer) fetchMetadata() error {
	requestCtx, cancelRequest := boundedRequestContext(c.assignmentContext(), c.requestTimeout())
	defer cancelRequest()
	conn, _, err := c.client.ConnectWithFailoverContext(requestCtx)
	if err != nil {
		return fmt.Errorf("connect for metadata: %w", err)
	}
	defer func() { _ = conn.Close() }()
	cleanup, err := bindConnectionToContext(requestCtx, conn)
	if err != nil {
		return err
	}
	defer cleanup()
	cmd := fmt.Sprintf("METADATA topic=%s", c.config.Topic)
	if err := WriteWithLength(conn, []byte(cmd)); err != nil {
		return fmt.Errorf("send metadata: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if !hasOKStatus(respStr) {
		return fmt.Errorf("metadata failed: %s", respStr)
	}

	var leadersStr string
	var cleanupPolicy string
	for _, part := range strings.Fields(respStr) {
		if strings.HasPrefix(part, "leaders=") {
			leadersStr = strings.TrimPrefix(part, "leaders=")
		} else if strings.HasPrefix(part, "cleanup_policy=") {
			cleanupPolicy = strings.TrimPrefix(part, "cleanup_policy=")
		}
	}
	if leadersStr == "" {
		return fmt.Errorf("metadata: missing leaders in response")
	}

	addrs := strings.Split(leadersStr, ",")
	c.partitionMu.Lock()
	for i, addr := range addrs {
		c.partitionLeaders[i] = bootstrapAddressForAdvertisedLoopback(addr, c.config.BrokerAddrs, c.config.UseBootstrapAddressForAdvertisedLoopback)
	}
	c.partitionMu.Unlock()
	c.compactionEnabled.Store(cleanupPolicyIncludesCompaction(cleanupPolicy))

	return nil
}

func cleanupPolicyIncludesCompaction(policy string) bool {
	normalized, err := normalizeSDKCleanupPolicy(TopicCleanupPolicy(policy))
	return err == nil && (normalized == string(TopicCleanupCompact) || normalized == string(TopicCleanupDeleteCompact))
}

func (c *Consumer) getPartitionLeaderAddr(partitionID int) string {
	c.partitionMu.RLock()
	defer c.partitionMu.RUnlock()
	return c.partitionLeaders[partitionID]
}

func (c *Consumer) updatePartitionLeader(partitionID int, addr string) {
	c.partitionMu.Lock()
	c.partitionLeaders[partitionID] = bootstrapAddressForAdvertisedLoopback(addr, c.config.BrokerAddrs, c.config.UseBootstrapAddressForAdvertisedLoopback)
	c.partitionMu.Unlock()
}

// ─── Metadata Refresh Loop ────────────────────────────────────────────────────

func (c *Consumer) metadataRefreshLoop(ctx context.Context) {
	interval := c.config.MetadataRefreshInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
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
		case <-c.assignmentContext().Done():
			return 0, "", nil, fmt.Errorf("consumer shutting down during join retry")
		case <-time.After(waitDur):
		}
	}
	return 0, "", nil, fmt.Errorf("failed to join group after %d attempts", maxAttempts)
}

func (c *Consumer) joinGroup() (int64, string, []int, error) {
	requestCtx, cancelRequest := boundedRequestContext(c.assignmentContext(), c.requestTimeout())
	defer cancelRequest()
	conn, err := c.getCoordinatorConnContext(requestCtx)
	if err != nil {
		return 0, "", nil, err
	}
	defer func() { _ = conn.Close() }()
	cleanup, err := bindConnectionToContext(requestCtx, conn)
	if err != nil {
		return 0, "", nil, err
	}
	defer cleanup()

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
	if err := WriteWithLength(conn, []byte(joinCmd)); err != nil {
		return 0, "", nil, fmt.Errorf("send join: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		var brokerErr *BrokerError
		if !errors.As(err, &brokerErr) {
			return 0, "", nil, fmt.Errorf("read join response: %w", err)
		}
		if c.handleNotCoordinatorError(brokerErr) {
			return 0, "", nil, fmt.Errorf("coordinator moved, retry: %w", brokerErr)
		}
		if resuming && strings.EqualFold(brokerErr.Code, "GEN_MISMATCH") {
			currentText := brokerErr.Fields["current"]
			current, parseErr := strconv.ParseInt(currentText, 10, 64)
			if parseErr == nil {
				assignments, syncErr := c.syncGroup(current, mID)
				if syncErr == nil {
					return current, mID, assignments, nil
				}
			}
		}
		if resuming && strings.EqualFold(brokerErr.Code, "member_not_found") {
			c.mu.Lock()
			if c.memberID == mID {
				c.memberID = ""
				c.generation = 0
			}
			c.mu.Unlock()
			return c.joinGroup()
		}
		return 0, "", nil, brokerErr
	}

	respStr := strings.TrimSpace(string(resp))
	if !hasOKStatus(respStr) {
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
	requestCtx, cancelRequest := boundedRequestContext(c.assignmentContext(), c.requestTimeout())
	defer cancelRequest()
	conn, err := c.getCoordinatorConnContext(requestCtx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	cleanup, err := bindConnectionToContext(requestCtx, conn)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	syncCmd := fmt.Sprintf("SYNC_GROUP topic=%s group=%s member=%s generation=%d",
		c.config.Topic, c.config.GroupID, memberID, generation)
	if err := WriteWithLength(conn, []byte(syncCmd)); err != nil {
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
	ctx := c.assignmentContext()
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
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
		}
	}
	return 0, lastErr
}

func (c *Consumer) fetchOffset(partition int) (uint64, error) {
	if err := c.assignmentContext().Err(); err != nil {
		return 0, err
	}

	requestCtx, cancelRequest := boundedRequestContext(c.assignmentContext(), c.requestTimeout())
	defer cancelRequest()
	conn, err := c.getCoordinatorConnContext(requestCtx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()
	cleanup, err := bindConnectionToContext(requestCtx, conn)
	if err != nil {
		return 0, err
	}
	defer cleanup()
	fetchCmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s",
		c.config.Topic, partition, c.config.GroupID)
	if err := WriteWithLength(conn, []byte(fetchCmd)); err != nil {
		return 0, fmt.Errorf("fetch offset send: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		var brokerErr *BrokerError
		if errors.As(err, &brokerErr) {
			_ = c.handleNotCoordinatorError(brokerErr)
			return 0, brokerErr
		}
		return 0, fmt.Errorf("fetch offset response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
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
	c.lifecycleMu.Lock()
	state := c.State()
	if state == ConsumerStateClosing || state == ConsumerStateClosed {
		c.lifecycleMu.Unlock()
		<-c.closeDone
		c.lifecycleMu.Lock()
		err := c.closeErr
		c.lifecycleMu.Unlock()
		return err
	}
	c.state.Store(uint32(ConsumerStateClosing))
	c.lifecycleMu.Unlock()

	c.rootCancel()
	c.cancelAssignment()
	c.closeActiveConnections()
	c.wg.Wait()
	var closeErr error
	if c.config.EnableAutoCommit && !c.flushAutoCommitOffsets(true) {
		c.offsetsMu.Lock()
		pending := len(c.currentOffsets)
		c.offsetsMu.Unlock()
		if pending > 0 {
			closeErr = fmt.Errorf("flush pending auto-commit offsets during close")
		}
	}

	close(c.commitCh)
	c.commitWg.Wait()
	c.lifecycleWg.Wait()

	c.mu.RLock()
	memberID := c.memberID
	generation := c.generation
	c.mu.RUnlock()
	if memberID != "" && generation > 0 {
		requestCtx, cancelRequest := boundedRequestContext(context.Background(), c.requestTimeout())
		if conn, err := c.getCoordinatorConnContext(requestCtx); err == nil {
			cleanup, bindErr := bindConnectionToContext(requestCtx, conn)
			if bindErr != nil {
				closeErr = errors.Join(closeErr, bindErr)
				_ = conn.Close()
				cancelRequest()
			} else {
				leaveCmd := fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s generation=%d",
					c.config.Topic, c.config.GroupID, memberID, generation)
				if err := WriteWithLength(conn, []byte(leaveCmd)); err != nil {
					closeErr = errors.Join(closeErr, fmt.Errorf("leave consumer group: %w", err))
				} else if response, err := ReadWithLength(conn); err != nil {
					closeErr = errors.Join(closeErr, fmt.Errorf("leave consumer group: %w", err))
				} else if !hasOKStatus(strings.TrimSpace(string(response))) {
					closeErr = errors.Join(closeErr, fmt.Errorf("leave consumer group: unexpected response %q", strings.TrimSpace(string(response))))
				}
				cleanup()
				_ = conn.Close()
				cancelRequest()
			}
		} else {
			cancelRequest()
			closeErr = errors.Join(closeErr, fmt.Errorf("connect to leave consumer group: %w", err))
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

	c.lifecycleMu.Lock()
	c.state.Store(uint32(ConsumerStateClosed))
	c.closeErr = closeErr
	close(c.doneCh)
	close(c.closeDone)
	c.lifecycleMu.Unlock()
	return closeErr
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

func (c *Consumer) requestTimeout() time.Duration {
	if c.config != nil && c.config.RequestTimeoutMS > 0 {
		return time.Duration(c.config.RequestTimeoutMS) * time.Millisecond
	}
	return defaultSDKRequestTimeout
}

func (c *Consumer) getLeaderConnContext(ctx context.Context) (net.Conn, error) {
	conn, _, err := c.client.ConnectWithFailoverContext(ctx)
	return conn, err
}

func (c *Consumer) findCoordinatorContext(ctx context.Context) (string, error) {
	requestCtx, cancelRequest := boundedRequestContext(ctx, c.requestTimeout())
	defer cancelRequest()
	conn, _, err := c.client.ConnectWithFailoverContext(requestCtx)
	if err != nil {
		return "", fmt.Errorf("connect for find_coordinator: %w", err)
	}
	defer func() { _ = conn.Close() }()
	cleanup, err := bindConnectionToContext(requestCtx, conn)
	if err != nil {
		return "", err
	}
	defer cleanup()
	cmd := fmt.Sprintf("FIND_COORDINATOR group=%s", c.config.GroupID)
	if err := WriteWithLength(conn, []byte(cmd)); err != nil {
		return "", fmt.Errorf("send find_coordinator: %w", err)
	}

	resp, err := ReadWithLength(conn)
	_ = conn.SetDeadline(time.Time{})
	if err != nil {
		return "", fmt.Errorf("read find_coordinator: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
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

func (c *Consumer) getCoordinatorConnContext(ctx context.Context) (net.Conn, error) {
	c.mu.RLock()
	addr := c.coordinatorAddr
	c.mu.RUnlock()

	if addr != "" {
		conn, err := c.client.ConnectContext(ctx, addr)
		if err == nil {
			return conn, nil
		}
		LogWarn("Coordinator %s unreachable: %v, rediscovering", addr, err)
	}

	newAddr, err := c.findCoordinatorContext(ctx)
	if err != nil {
		return c.getLeaderConnContext(ctx)
	}
	c.mu.Lock()
	c.coordinatorAddr = newAddr
	c.mu.Unlock()
	conn, err := c.client.ConnectContext(ctx, newAddr)
	if err != nil {
		return nil, err
	}
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

func bootstrapAddressForAdvertisedLoopback(advertised string, bootstrap []string, enabled bool) string {
	if !enabled || len(bootstrap) != 1 {
		return advertised
	}
	host, _, err := net.SplitHostPort(advertised)
	if err != nil || !isLoopbackCoordinatorHost(host) {
		return advertised
	}
	return bootstrap[0]
}

func (c *Consumer) handleNotCoordinatorError(brokerErr *BrokerError) bool {
	if brokerErr == nil || !strings.EqualFold(brokerErr.Code, "NOT_COORDINATOR") {
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
