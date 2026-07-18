package sdk

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BatchState struct {
	BatchID     string
	StartSeqNum uint64
	EndSeqNum   uint64
	Partition   int
	SentTime    time.Time
	Acked       bool
}

type partitionBuffer struct {
	mu           sync.Mutex
	msgs         []Message
	cond         *sync.Cond
	drainWake    chan struct{}
	drainWaiters []chan struct{}
	closed       bool
}

func newPartitionBuffer() *partitionBuffer {
	p := &partitionBuffer{
		msgs:      make([]Message, 0),
		drainWake: make(chan struct{}, 1),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

type Producer struct {
	config *PublisherConfig
	client *ProducerClient

	partitions int
	buffers    []*partitionBuffer

	sendersWG sync.WaitGroup

	rr       uint32
	inFlight []int32

	partitionSentMus  []sync.Mutex
	partitionSentSeqs []map[uint64]struct{}

	ackedCount    atomic.Uint64
	uniqueCount   atomic.Uint64
	attemptsCount atomic.Uint64

	partitionBatchStates []map[string]*BatchState
	partitionBatchMus    []sync.Mutex
	gcTicker             *time.Ticker

	partitionLeaders map[int]string
	partitionMu      sync.RWMutex

	done    chan struct{}
	closed  int32
	closeMu sync.Mutex

	bmMu         sync.Mutex
	bmTotalTime  map[int]time.Duration
	bmTotalCount map[int]int
	bmLatencies  []time.Duration
}

func NewProducer(cfg *PublisherConfig) (*Producer, error) {
	if cfg.EnableMetrics {
		initMetrics()
	}

	client, err := NewProducerClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create producer client: %w", err)
	}

	p := &Producer{
		config:           cfg,
		client:           client,
		partitions:       cfg.Partitions,
		buffers:          make([]*partitionBuffer, cfg.Partitions),
		done:             make(chan struct{}),
		bmTotalTime:      make(map[int]time.Duration),
		bmTotalCount:     make(map[int]int),
		bmLatencies:      make([]time.Duration, 0),
		inFlight:         make([]int32, cfg.Partitions),
		gcTicker:         time.NewTicker(1 * time.Minute),
		partitionLeaders: make(map[int]string),
	}

	p.partitionSentSeqs = make([]map[uint64]struct{}, cfg.Partitions)
	p.partitionSentMus = make([]sync.Mutex, cfg.Partitions)
	for i := 0; i < cfg.Partitions; i++ {
		p.partitionSentSeqs[i] = make(map[uint64]struct{})
	}

	p.partitionBatchStates = make([]map[string]*BatchState, cfg.Partitions)
	p.partitionBatchMus = make([]sync.Mutex, cfg.Partitions)
	for i := 0; i < cfg.Partitions; i++ {
		p.partitionBatchStates[i] = make(map[string]*BatchState)
	}

	if err := p.CreateTopic(cfg.Topic, cfg.Partitions); err != nil {
		LogError("failed to create topic '%s': %v", cfg.Topic, err)
	}

	p.fetchMetadata()

	connectedCount := 0
	for i := 0; i < cfg.Partitions; i++ {
		p.buffers[i] = newPartitionBuffer()
		brokerAddr := p.getPartitionLeaderAddr(i)
		if err := p.client.ConnectPartition(i, brokerAddr); err != nil {
			LogError("Failed to connect partition %d: %v", i, err)
		} else {
			connectedCount++
		}
		p.sendersWG.Add(1)
		go p.partitionSender(i)
	}
	if connectedCount == 0 {
		return nil, fmt.Errorf("failed to connect to any partition")
	}

	go p.batchStateGC()
	return p, nil
}

func (p *Producer) fetchMetadata() {
	addrs := p.config.BrokerAddrs
	if len(addrs) == 0 {
		return
	}
	for _, addr := range addrs {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			continue
		}
		cmd := fmt.Sprintf("METADATA topic=%s", p.config.Topic)
		if err := WriteWithLength(conn, EncodeMessage("", cmd)); err != nil {
			_ = conn.Close()
			continue
		}
		resp, err := ReadWithLength(conn)
		_ = conn.Close()
		if err != nil {
			continue
		}
		respStr := strings.TrimSpace(string(resp))
		if !strings.HasPrefix(respStr, "OK") {
			continue
		}
		for _, part := range strings.Fields(respStr) {
			if strings.HasPrefix(part, "leaders=") {
				addrs := strings.Split(strings.TrimPrefix(part, "leaders="), ",")
				p.partitionMu.Lock()
				for i, a := range addrs {
					if a = strings.TrimSpace(a); a != "" {
						p.partitionLeaders[i] = a
					}
				}
				p.partitionMu.Unlock()
				return
			}
		}
		return
	}
}

func (p *Producer) getPartitionLeaderAddr(partition int) string {
	p.partitionMu.RLock()
	defer p.partitionMu.RUnlock()
	return p.partitionLeaders[partition]
}

func (p *Producer) nextPartition() int {
	idx := int((atomic.AddUint32(&p.rr, 1) - 1) % uint32(p.partitions))
	return idx
}

func (p *Producer) CreateTopic(topic string, partitions int) error {
	if len(p.config.BrokerAddrs) == 0 {
		return fmt.Errorf("no broker addresses available")
	}
	brokerAddr := p.config.BrokerAddrs[0]
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close() }()

	createCmd := fmt.Sprintf("CREATE topic=%s partitions=%d", topic, partitions)
	cmdBytes := EncodeMessage("admin", createCmd)

	if err := WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	resp, err := ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if strings.Contains(string(resp), "ERROR:") {
		return fmt.Errorf("broker error: %s", string(resp))
	}

	LogInfo("create topic %s partition %d", topic, partitions)
	return nil
}

// Send enqueues payload for delivery and returns the assigned sequence number.
func (p *Producer) Send(payload string) (uint64, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return 0, fmt.Errorf("send: %w", ErrProducerClosed)
	}

	p.closeMu.Lock()
	part := p.nextPartition()
	buf := p.buffers[part]
	p.closeMu.Unlock()

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if buf.closed {
		return 0, fmt.Errorf("partition %d buffer closed: %w", part, ErrProducerClosed)
	}

	if len(buf.msgs) >= p.config.BufferSize {
		return 0, fmt.Errorf("partition %d buffer full", part)
	}

	seqNum := p.client.NextSeqNum(part)
	bm := Message{
		SeqNum:     seqNum,
		Payload:    payload,
		ProducerID: p.client.ID,
		Epoch:      p.client.Epoch,
	}

	buf.msgs = append(buf.msgs, bm)
	buf.cond.Signal()

	return seqNum, nil
}

// PublishMessage is an alias for Send, for compatibility with test/publisher.
func (p *Producer) PublishMessage(payload string) (uint64, error) {
	return p.Send(payload)
}

func (p *Producer) partitionSender(part int) {
	defer p.sendersWG.Done()

	buf := p.buffers[part]
	linger := time.Duration(p.config.LingerMS) * time.Millisecond

	timer := time.NewTimer(linger)
	defer timer.Stop()

	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	for {
		select {
		case <-p.done:
			return
		default:
		}

		buf.mu.Lock()
		for len(buf.msgs) == 0 && len(buf.drainWaiters) == 0 && !buf.closed {
			buf.cond.Wait()
		}

		if len(buf.msgs) == 0 {
			waiters := buf.drainWaiters
			buf.drainWaiters = nil
			select {
			case <-buf.drainWake:
			default:
			}
			closed := buf.closed
			buf.mu.Unlock()

			for _, waiter := range waiters {
				close(waiter)
			}
			if closed {
				return
			}
			continue
		}

		var batch []Message
		if len(buf.msgs) >= p.config.BatchSize || len(buf.drainWaiters) > 0 || buf.closed {
			batch = p.extract(buf)
			buf.mu.Unlock()
		} else {
			buf.mu.Unlock()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(linger)

			select {
			case <-timer.C:
				buf.mu.Lock()
				if len(buf.msgs) > 0 {
					batch = p.extractAny(buf)
				}
				buf.mu.Unlock()
			case <-buf.drainWake:
				buf.mu.Lock()
				if len(buf.msgs) > 0 {
					batch = p.extract(buf)
				}
				buf.mu.Unlock()
			case <-p.done:
				return
			}
		}

		if len(batch) > 0 {
			p.sendBatch(part, batch)
		}
	}
}

func (p *Producer) extract(buf *partitionBuffer) []Message {
	if len(buf.msgs) == 0 {
		return nil
	}

	n := p.config.BatchSize
	if len(buf.msgs) < n {
		n = len(buf.msgs)
	}

	batch := make([]Message, n)
	copy(batch, buf.msgs[:n])
	buf.msgs = buf.msgs[n:]

	return batch
}

func (p *Producer) extractAny(buf *partitionBuffer) []Message {
	if len(buf.msgs) == 0 {
		return nil
	}

	n := len(buf.msgs)
	batch := make([]Message, n)
	copy(batch, buf.msgs[:n])
	buf.msgs = buf.msgs[:0]

	return batch
}

// ─── Flush / Stats ────────────────────────────────────────────────────────────

func (p *Producer) Flush() {
	timeout := p.flushTimeout()

	p.closeMu.Lock()
	if atomic.LoadInt32(&p.closed) == 1 {
		p.closeMu.Unlock()
		return
	}
	waiters := p.requestDrain(false)
	p.closeMu.Unlock()

	if !waitForDrain(waiters, timeout) {
		LogWarn("Flush timeout after %v", timeout)
	}
}

func (p *Producer) flushTimeout() time.Duration {
	timeout := time.Duration(p.config.FlushTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		return 30 * time.Second
	}
	return timeout
}

// requestDrain must be called while closeMu is held so a concurrent Close cannot
// let a sender exit before its drain waiter is registered.
func (p *Producer) requestDrain(markClosed bool) []chan struct{} {
	waiters := make([]chan struct{}, 0, len(p.buffers))
	for _, buf := range p.buffers {
		waiter := make(chan struct{})
		buf.mu.Lock()
		if markClosed {
			buf.closed = true
		}
		buf.drainWaiters = append(buf.drainWaiters, waiter)
		buf.cond.Broadcast()
		select {
		case buf.drainWake <- struct{}{}:
		default:
		}
		buf.mu.Unlock()
		waiters = append(waiters, waiter)
	}
	return waiters
}

func waitForDrain(waiters []chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for _, waiter := range waiters {
		select {
		case <-waiter:
		case <-timer.C:
			return false
		}
	}
	return true
}

// FlushBenchmark waits until all expectedTotal messages are acknowledged or timeout expires.
func (p *Producer) FlushBenchmark(expectedTotal int) {
	for _, buf := range p.buffers {
		buf.mu.Lock()
		buf.cond.Broadcast()
		buf.mu.Unlock()
	}

	timeout := time.Duration(p.config.FlushTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	start := time.Now()

	for time.Now().Before(deadline) {
		allInFlightClear := true
		for part := 0; part < p.partitions; part++ {
			if atomic.LoadInt32(&p.inFlight[part]) > 0 {
				LogDebug("Partition %d still has in-flight messages", part)
				allInFlightClear = false
				break
			}
		}

		if allInFlightClear {
			ackedSoFar := p.GetUniqueAckCount()
			totalPending := 0
			for part := 0; part < p.partitions; part++ {
				p.partitionBatchMus[part].Lock()
				totalPending += len(p.partitionBatchStates[part])
				p.partitionBatchMus[part].Unlock()
			}

			if ackedSoFar >= expectedTotal && totalPending == 0 {
				LogInfo("FlushBenchmark completed — all %d messages acknowledged (%.3fs)", expectedTotal, time.Since(start).Seconds())
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	LogWarn("FlushBenchmark timeout after %v. Only %d/%d messages acknowledged.", timeout, p.GetUniqueAckCount(), expectedTotal)
}

func (p *Producer) VerifySentSequences(expectedCount int) error {
	totalSent := 0
	for part := 0; part < p.partitions; part++ {
		p.partitionSentMus[part].Lock()
		totalSent += len(p.partitionSentSeqs[part])
		p.partitionSentMus[part].Unlock()
	}

	if totalSent != expectedCount {
		return fmt.Errorf("expected %d messages sent, got %d", expectedCount, totalSent)
	}

	LogInfo("All %d sequences sent successfully across all partitions", expectedCount)
	return nil
}

func (p *Producer) GetPartitionStats() []PartitionStat {
	p.bmMu.Lock()
	defer p.bmMu.Unlock()

	stats := make([]PartitionStat, 0, p.partitions)
	for part := 0; part < p.partitions; part++ {
		count := p.bmTotalCount[part]
		totalTime := p.bmTotalTime[part]
		var avg time.Duration
		if count > 0 {
			avg = totalTime / time.Duration(count)
		}
		stats = append(stats, PartitionStat{
			PartitionID: part,
			BatchCount:  count,
			AvgDuration: avg,
		})
	}
	return stats
}

func (p *Producer) GetLatencies() []time.Duration {
	p.bmMu.Lock()
	defer p.bmMu.Unlock()

	res := make([]time.Duration, len(p.bmLatencies))
	copy(res, p.bmLatencies)
	return res
}

func (p *Producer) GetUniqueAckCount() int {
	return int(p.uniqueCount.Load())
}

func (p *Producer) GetAttemptsCount() int {
	return int(p.attemptsCount.Load())
}

func (p *Producer) GetPartitionCount() int {
	return p.partitions
}

func (p *Producer) batchStateGC() {
	for {
		select {
		case <-p.gcTicker.C:
			now := time.Now()
			ackedCutoff := now.Add(-1 * time.Minute)
			staleCutoff := now.Add(-5 * time.Minute)

			for part := 0; part < p.partitions; part++ {
				p.partitionBatchMus[part].Lock()

				for id, st := range p.partitionBatchStates[part] {
					if st.Acked && st.SentTime.Before(ackedCutoff) {
						delete(p.partitionBatchStates[part], id)
						continue
					}

					if !st.Acked && st.SentTime.Before(staleCutoff) {
						LogWarn("GC: Dropping unacked stale batch: %s", id)
						delete(p.partitionBatchStates[part], id)
					}
				}
				p.partitionBatchMus[part].Unlock()
			}
		case <-p.done:
			return
		}
	}
}

func (p *Producer) Close() error {
	p.closeMu.Lock()
	if atomic.LoadInt32(&p.closed) == 1 {
		p.closeMu.Unlock()
		return nil
	}
	atomic.StoreInt32(&p.closed, 1)
	waiters := p.requestDrain(true)
	p.closeMu.Unlock()

	timeout := p.flushTimeout()
	drained := waitForDrain(waiters, timeout)
	if !drained {
		LogWarn("Close drain timeout after %v", timeout)
	}

	p.gcTicker.Stop()
	close(p.done)
	clientErr := p.client.Close()
	p.sendersWG.Wait()

	if !drained {
		if clientErr != nil {
			return fmt.Errorf("producer close: drain timeout after %v; close client: %w", timeout, clientErr)
		}
		return fmt.Errorf("producer close: drain timeout after %v", timeout)
	}
	return clientErr
}
