package sdk

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const defaultLeaderStalenessThreshold = 30 * time.Second

type leaderInfo struct {
	addr    string
	updated time.Time
}

type ProducerClient struct {
	ID               string
	globalSeqNum     atomic.Uint64
	partitionSeqNums sync.Map // int -> *atomic.Uint64

	Epoch  int64
	mu     sync.RWMutex
	conns  atomic.Pointer[[]net.Conn]
	config *PublisherConfig

	leader atomic.Pointer[leaderInfo]
}

func NewProducerClient(config *PublisherConfig) *ProducerClient {
	pc := &ProducerClient{
		ID:     uuid.New().String(),
		Epoch:  time.Now().UnixNano(),
		config: config,
	}

	pc.leader.Store(&leaderInfo{
		addr:    "",
		updated: time.Time{},
	})

	return pc
}

func (pc *ProducerClient) NextSeqNum(partition int) uint64 {
	if !pc.config.EnableIdempotence {
		return pc.globalSeqNum.Add(1)
	}

	val, _ := pc.partitionSeqNums.LoadOrStore(partition, &atomic.Uint64{})
	return val.(*atomic.Uint64).Add(1)
}

func (pc *ProducerClient) connectPartitionLocked(idx int, addr string) error {
	if idx < 0 {
		return fmt.Errorf("invalid partition index: %d", idx)
	}

	var conn net.Conn
	var err error

	if pc.config.UseTLS {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(pc.config.TLSCertPath, pc.config.TLSKeyPath)
		if err != nil {
			return fmt.Errorf("load TLS cert: %w", err)
		}
		conn, err = tls.Dial("tcp", addr, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err != nil {
			return fmt.Errorf("TLS dial to %s failed: %w", addr, err)
		}
	} else {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("TCP dial to %s failed: %w", addr, err)
		}
	}

	var currentConns []net.Conn
	if ptr := pc.conns.Load(); ptr != nil {
		currentConns = *ptr
	}

	newSize := idx + 1
	if len(currentConns) > newSize {
		newSize = len(currentConns)
	}

	tmp := make([]net.Conn, newSize)
	copy(tmp, currentConns)
	tmp[idx] = conn

	pc.conns.Store(&tmp)
	return nil
}

func (pc *ProducerClient) GetConn(part int) net.Conn {
	ptr := pc.conns.Load()
	if ptr == nil {
		return nil
	}
	conns := *ptr
	if part >= 0 && part < len(conns) {
		return conns[part]
	}
	return nil
}

func (pc *ProducerClient) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	ptr := pc.conns.Swap(nil)
	if ptr == nil {
		return nil
	}

	conns := *ptr
	for i, c := range conns {
		if c != nil {
			_ = c.Close()
			conns[i] = nil
		}
	}

	return nil
}

func (pc *ProducerClient) GetLeaderAddr() string {
	info := pc.leader.Load()
	if info == nil || info.addr == "" {
		return ""
	}
	return info.addr
}

func (pc *ProducerClient) UpdateLeader(leaderAddr string) {
	old := pc.leader.Load()
	if old != nil && old.addr == leaderAddr {
		return
	}

	pc.leader.Store(&leaderInfo{
		addr:    leaderAddr,
		updated: time.Now(),
	})
}

func (pc *ProducerClient) selectBroker() string {
	if pc.config == nil || len(pc.config.BrokerAddrs) == 0 {
		return ""
	}

	info := pc.leader.Load()
	if info != nil && info.addr != "" && time.Since(info.updated) < defaultLeaderStalenessThreshold {
		return info.addr
	}

	return pc.config.BrokerAddrs[0]
}

func (pc *ProducerClient) ConnectPartition(idx int, addr string) error {
	if addr == "" {
		addr = pc.selectBroker()
	}
	if addr == "" {
		return fmt.Errorf("no broker address available for partition %d", idx)
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.connectPartitionLocked(idx, addr)
}

func (pc *ProducerClient) ReconnectPartition(idx int, addr string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	oldPtr := pc.conns.Load()
	if oldPtr != nil {
		conns := *oldPtr
		if idx < len(conns) && conns[idx] != nil {
			_ = conns[idx].Close()
		}
	}

	return pc.connectPartitionLocked(idx, addr)
}

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
		logError("failed to create topic '%s': %v", cfg.Topic, err)
		// We might still continue if AutoCreateTopics is true on broker, but better to fail early if explicit create fails
	}

	connectedCount := 0
	for i := 0; i < cfg.Partitions; i++ {
		p.buffers[i] = newPartitionBuffer()
		brokerAddr := p.client.selectBroker()
		if err := p.client.ConnectPartition(i, brokerAddr); err != nil {
			logError("Failed to connect partition %d: %v", i, err)
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
	brokerAddr := p.config.BrokerAddrs[0]
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

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

func (p *Producer) Send(payload string) (uint64, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return 0, fmt.Errorf("producer closed")
	}

	p.closeMu.Lock()
	part := p.nextPartition()
	buf := p.buffers[part]
	p.closeMu.Unlock()

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if buf.closed {
		return 0, fmt.Errorf("partition %d buffer is closed", part)
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

func (p *Producer) sendBatch(part int, batch []Message) {
	if len(batch) == 0 {
		return
	}

	atomic.AddInt32(&p.inFlight[part], 1)
	defer atomic.AddInt32(&p.inFlight[part], -1)

	var batchStart, batchEnd uint64
	if len(batch) > 0 {
		batchStart = batch[0].SeqNum
		batchEnd = batch[len(batch)-1].SeqNum
	}

	shortID := p.client.ID[:8]
	batchID := fmt.Sprintf("%s-%d-p%d-%d-%d", shortID, p.client.Epoch, part, batchStart, batchEnd)

	p.partitionBatchMus[part].Lock()
	p.partitionBatchStates[part][batchID] = &BatchState{
		BatchID:     batchID,
		StartSeqNum: batchStart,
		EndSeqNum:   batchEnd,
		Partition:   part,
		SentTime:    time.Now(),
		Acked:       false,
	}
	p.partitionBatchMus[part].Unlock()

	data, err := EncodeBatchMessages(p.config.Topic, part, p.config.Acks, p.config.EnableIdempotence, batch)
	if err != nil {
		logError("encode batch failed: %v", err)
		p.cleanupBatchState(part, batchID)
		p.handleSendFailure(part, batch)
		return
	}

	payload, err := CompressMessage(data, p.config.CompressionType)
	if err != nil {
		logError("compress batch failed: %v", err)
		p.cleanupBatchState(part, batchID)
		p.handleSendFailure(part, batch)
		return
	}

	ackResp, err := p.sendWithRetry(payload, part)
	if err != nil {
		logError("send failed: %v", err)
		p.cleanupBatchState(part, batchID)
		p.handleSendFailure(part, batch)
		return
	}

	p.attemptsCount.Add(uint64(len(batch)))

	switch ackResp.Status {
	case "OK":
		p.partitionSentMus[part].Lock()
		for _, m := range batch {
			p.partitionSentSeqs[part][m.SeqNum] = struct{}{}
		}
		p.partitionSentMus[part].Unlock()
		p.markBatchAckedByID(part, batchID, len(batch))
	case "PARTIAL":
		logWarn("Partial success for batch %s", batchID)
		p.cleanupBatchState(part, batchID)
		p.handlePartialFailure(part, batch, ackResp)
	default:
		p.cleanupBatchState(part, batchID)
	}
}

func (p *Producer) cleanupBatchState(part int, batchID string) {
	p.partitionBatchMus[part].Lock()
	delete(p.partitionBatchStates[part], batchID)
	p.partitionBatchMus[part].Unlock()
}

func (p *Producer) handleSendFailure(part int, batch []Message) {
	if len(batch) == 0 {
		return
	}

	buf := p.buffers[part]
	buf.mu.Lock()
	defer buf.mu.Unlock()

	p.partitionSentMus[part].Lock()
	var retryBatch []Message
	for _, msg := range batch {
		if _, exists := p.partitionSentSeqs[part][msg.SeqNum]; !exists {
			msg.Retry = true
			retryBatch = append(retryBatch, msg)
		}
	}
	p.partitionSentMus[part].Unlock()

	allMsgs := append(buf.msgs, retryBatch...)
	sort.Slice(allMsgs, func(i, j int) bool {
		if allMsgs[i].Retry && !allMsgs[j].Retry {
			return true
		}
		if !allMsgs[i].Retry && allMsgs[j].Retry {
			return false
		}
		return allMsgs[i].SeqNum < allMsgs[j].SeqNum
	})

	buf.msgs = allMsgs
	buf.cond.Signal()
}

func (p *Producer) handlePartialFailure(part int, batch []Message, ackResp *AckResponse) {
	lastSuccessSeq := ackResp.SeqEnd

	buf := p.buffers[part]
	buf.mu.Lock()
	defer buf.mu.Unlock()

	var retryBatch []Message
	for _, msg := range batch {
		if msg.SeqNum > lastSuccessSeq {
			retryBatch = append(retryBatch, msg)
		}
	}

	if len(retryBatch) > 0 {
		allMsgs := append(buf.msgs, retryBatch...)
		sort.Slice(allMsgs, func(i, j int) bool {
			return allMsgs[i].SeqNum < allMsgs[j].SeqNum
		})
		buf.msgs = allMsgs
		buf.cond.Signal()
	}
}

func (p *Producer) sendWithRetry(payload []byte, part int) (*AckResponse, error) {
	maxAttempts := p.config.MaxRetries + 1
	backoff := p.config.RetryBackoffMS

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn := p.client.GetConn(part)
		if conn == nil {
			brokerAddr := p.client.selectBroker()
			if err := p.client.ReconnectPartition(part, brokerAddr); err != nil {
				lastErr = fmt.Errorf("reconnect failed: %w", err)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff = min(backoff*2, p.config.MaxBackoffMS)
				continue
			}
			conn = p.client.GetConn(part)
			if conn == nil {
				lastErr = fmt.Errorf("no connection after reconnect")
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff = min(backoff*2, p.config.MaxBackoffMS)
				continue
			}
		}

		if err := conn.SetWriteDeadline(time.Now().Add(time.Duration(p.config.WriteTimeoutMS) * time.Millisecond)); err != nil {
			lastErr = fmt.Errorf("set write deadline failed: %w", err)
			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		if err := WriteWithLength(conn, payload); err != nil {
			lastErr = fmt.Errorf("write failed: %w", err)
			brokerAddr := p.client.selectBroker()
			_ = p.client.ReconnectPartition(part, brokerAddr)
			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		if p.config.Acks == "0" {
			return &AckResponse{Status: "OK"}, nil
		}

		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(p.config.AckTimeoutMS) * time.Millisecond))
		resp, err := ReadWithLength(conn)
		_ = conn.SetReadDeadline(time.Time{})

		if err != nil {
			lastErr = fmt.Errorf("read ack failed: %w", err)
			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		ackResp, err := p.parseAckResponse(resp)
		if err != nil {
			lastErr = err
			continue
		}

		return ackResp, nil
	}
	return nil, lastErr
}

func (p *Producer) markBatchAckedByID(part int, batchID string, batchLen int) {
	p.partitionBatchMus[part].Lock()
	state, ok := p.partitionBatchStates[part][batchID]
	if !ok || state.Acked {
		p.partitionBatchMus[part].Unlock()
		return
	}

	state.Acked = true
	p.uniqueCount.Add(uint64(batchLen))

	delete(p.partitionBatchStates[part], batchID)
	p.partitionBatchMus[part].Unlock()

	p.ackedCount.Store(state.EndSeqNum)

	elapsed := time.Since(state.SentTime)
	p.bmMu.Lock()
	p.bmTotalCount[part] += 1
	p.bmTotalTime[part] += elapsed
	p.bmLatencies = append(p.bmLatencies, elapsed)
	p.bmMu.Unlock()
}

func (p *Producer) parseAckResponse(resp []byte) (*AckResponse, error) {
	var ackResp AckResponse
	if err := json.Unmarshal(resp, &ackResp); err != nil {
		return nil, fmt.Errorf("invalid ack format: %w", err)
	}

	if ackResp.Leader != "" {
		if ackResp.Leader != p.client.GetLeaderAddr() {
			p.client.UpdateLeader(ackResp.Leader)
		}
	}

	if ackResp.Status == "ERROR" {
		return &ackResp, fmt.Errorf("broker error: %s", ackResp.ErrorMsg)
	}

	return &ackResp, nil
}

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
			inFlight := atomic.LoadInt32(&p.inFlight[part])
			if inFlight > 0 {
				allClear = false
				break
			}
		}

		if allClear {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
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

func (p *Producer) Close() {
	p.closeMu.Lock()
	if atomic.LoadInt32(&p.closed) == 1 {
		p.closeMu.Unlock()
		return
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

	_ = p.client.Close()
}
