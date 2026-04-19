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
	mu     sync.Mutex
	msgs   []Message
	cond   *sync.Cond
	closed bool
}

func newPartitionBuffer() *partitionBuffer {
	p := &partitionBuffer{
		msgs: make([]Message, 0),
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

	p := &Producer{
		config:       cfg,
		client:       NewProducerClient(cfg),
		partitions:   cfg.Partitions,
		buffers:      make([]*partitionBuffer, cfg.Partitions),
		done:         make(chan struct{}),
		bmTotalTime:  make(map[int]time.Duration),
		bmTotalCount: make(map[int]int),
		bmLatencies:  make([]time.Duration, 0),
		inFlight:     make([]int32, cfg.Partitions),
		gcTicker:     time.NewTicker(1 * time.Minute),
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

	connectedCount := 0
	for i := 0; i < cfg.Partitions; i++ {
		p.buffers[i] = newPartitionBuffer()
		brokerAddr := p.client.selectBroker()
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
		buf.mu.Lock()

		for len(buf.msgs) == 0 && !buf.closed && atomic.LoadInt32(&p.closed) == 0 {
			buf.cond.Wait()
		}

		if (buf.closed || atomic.LoadInt32(&p.closed) == 1) && len(buf.msgs) == 0 {
			buf.mu.Unlock()
			return
		}

		var batch []Message
		if len(buf.msgs) >= p.config.BatchSize {
			batch = p.extract(buf)
			buf.mu.Unlock()
		} else {
			buf.mu.Unlock()
			timer.Reset(linger)

			select {
			case <-timer.C:
				buf.mu.Lock()
				if len(buf.msgs) > 0 {
					batch = p.extractAny(buf)
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
	for time.Now().Before(deadline) {
		allClear := true
		for part := 0; part < p.partitions; part++ {
			if atomic.LoadInt32(&p.inFlight[part]) > 0 {
				allClear = false
				break
			}
		}

		if allClear {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	LogWarn("Flush timeout after %v", timeout)
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
	close(p.done)

	for _, buf := range p.buffers {
		buf.mu.Lock()
		buf.closed = true
		buf.cond.Broadcast()
		buf.mu.Unlock()
	}
	p.closeMu.Unlock()

	p.gcTicker.Stop()
	p.sendersWG.Wait()

	return p.client.Close()
}
