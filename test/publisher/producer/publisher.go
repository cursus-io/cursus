package producer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/test/publisher/bench"
	"github.com/cursus-io/cursus/test/publisher/config"
	"github.com/cursus-io/cursus/util"
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
	msgs   []types.Message
	cond   *sync.Cond
	closed bool
}

func newPartitionBuffer() *partitionBuffer {
	p := &partitionBuffer{
		msgs: make([]types.Message, 0),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

type Publisher struct {
	config   *config.PublisherConfig
	producer *ProducerClient

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

func NewPublisher(cfg *config.PublisherConfig) (*Publisher, error) {
	p := &Publisher{
		config:       cfg,
		producer:     NewProducerClient(cfg.Partitions, cfg),
		partitions:   cfg.Partitions,
		buffers:      make([]*partitionBuffer, cfg.Partitions),
		done:         make(chan struct{}),
		bmTotalTime:  make(map[int]time.Duration),
		bmTotalCount: make(map[int]int),
		bmLatencies:  make([]time.Duration, 0, cfg.NumMessages/cfg.BatchSize),
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
		return nil, fmt.Errorf("failed to create topic '%s': %w", cfg.Topic, err)
	}

	connectedCount := 0
	for i := 0; i < cfg.Partitions; i++ {
		p.buffers[i] = newPartitionBuffer()
		brokerAddr := p.producer.selectBroker()
		if err := p.producer.ConnectPartition(i, brokerAddr, cfg.UseTLS, cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
			util.Error("Failed to connect partition %d: %v", i, err)
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

func (p *Publisher) nextPartition() int {
	idx := int((atomic.AddUint32(&p.rr, 1) - 1) % uint32(p.partitions))
	return idx
}

func (p *Publisher) CreateTopic(topic string, partitions int) error {
	brokerAddr := p.config.BrokerAddrs[0]
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close() }()

	createCmd := fmt.Sprintf("CREATE topic=%s partitions=%d", topic, partitions)
	cmdBytes := util.EncodeMessage("admin", createCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if strings.Contains(string(resp), "ERROR:") {
		return fmt.Errorf("broker error: %s", string(resp))
	}

	util.Info("create topic %s partition %d", topic, partitions)
	return nil
}

func (p *Publisher) PublishMessage(message string) (uint64, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return 0, fmt.Errorf("publisher closed")
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

	seqNum := p.producer.NextSeqNum(part)
	bm := types.Message{
		SeqNum:     seqNum,
		Payload:    message,
		ProducerID: p.producer.ID,
		Epoch:      p.producer.Epoch,
	}

	buf.msgs = append(buf.msgs, bm)
	buf.cond.Signal()

	return seqNum, nil
}

func (p *Publisher) partitionSender(part int) {
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

		var batch []types.Message
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

func (p *Publisher) extract(buf *partitionBuffer) []types.Message {
	if len(buf.msgs) == 0 {
		return nil
	}

	n := p.config.BatchSize
	if len(buf.msgs) < n {
		n = len(buf.msgs)
	}

	batch := make([]types.Message, n)
	copy(batch, buf.msgs[:n])
	buf.msgs = buf.msgs[n:]

	return batch
}

func (p *Publisher) extractAny(buf *partitionBuffer) []types.Message {
	if len(buf.msgs) == 0 {
		util.Warn("extractAny: buffer empty")
		return nil
	}

	n := len(buf.msgs)
	batch := make([]types.Message, n)
	copy(batch, buf.msgs[:n])
	buf.msgs = buf.msgs[:0]

	return batch
}

func (p *Publisher) sendBatch(part int, batch []types.Message) {
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

	shortID := p.producer.ID[:8]
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}

	shortEpoch := p.producer.Epoch % 1000
	batchID := fmt.Sprintf("%s-%03d-p%d-%d-%d", shortID, shortEpoch, part, batchStart, batchEnd)
	util.Info("Sending batch %s: partition=%d, messages=%d, epoch=%d, seqRange=%d-%d", batchID, part, len(batch), p.producer.Epoch, batchStart, batchEnd)

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

	data, err := util.EncodeBatchMessages(p.config.Topic, part, p.config.Acks, batch)
	if err != nil {
		util.Error("encode batch failed: %v", err)
		p.cleanupBatchState(part, batchID)
		p.handleSendFailure(part, batch)
		return
	}

	payload, err := util.CompressMessage(data, p.config.CompressionType)
	if err != nil {
		util.Error("compress batch failed: %v", err)
		p.cleanupBatchState(part, batchID)
		p.handleSendFailure(part, batch)
		return
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	payload = append(lenBuf, payload...)

	ackResp, err := p.sendWithRetry(payload, part)
	if err != nil {
		util.Error("send failed: %v", err)
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
		util.Warn("Partial success for batch %s", batchID)
		p.cleanupBatchState(part, batchID)
		p.handlePartialFailure(part, batch, ackResp)
	default:
		p.cleanupBatchState(part, batchID)
	}
}

func (p *Publisher) cleanupBatchState(part int, batchID string) {
	p.partitionBatchMus[part].Lock()
	delete(p.partitionBatchStates[part], batchID)
	p.partitionBatchMus[part].Unlock()
}

func (p *Publisher) handleSendFailure(part int, batch []types.Message) {
	if len(batch) == 0 {
		return
	}

	buf := p.buffers[part]
	buf.mu.Lock()
	defer buf.mu.Unlock()

	p.partitionSentMus[part].Lock()
	var retryBatch []types.Message
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

func (p *Publisher) handlePartialFailure(part int, batch []types.Message, ackResp *types.AckResponse) {
	lastSuccessSeq := ackResp.SeqEnd

	buf := p.buffers[part]
	buf.mu.Lock()
	defer buf.mu.Unlock()

	var retryBatch []types.Message
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

func (p *Publisher) sendWithRetry(payload []byte, part int) (*types.AckResponse, error) {
	maxAttempts := p.config.MaxRetries + 1
	backoff := p.config.RetryBackoffMS

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn := p.producer.GetConn(part)
		if conn == nil {
			brokerAddr := p.producer.selectBroker()
			if err := p.producer.ReconnectPartition(part, brokerAddr, p.config.UseTLS, p.config.TLSCertPath, p.config.TLSKeyPath); err != nil {
				lastErr = fmt.Errorf("reconnect failed: %w", err)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff = min(backoff*2, p.config.MaxBackoffMS)
				continue
			}
			conn = p.producer.GetConn(part)
			if conn == nil {
				lastErr = fmt.Errorf("no connection after reconnect")
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff = min(backoff*2, p.config.MaxBackoffMS)
				continue
			}
		}

		if err := conn.SetWriteDeadline(time.Now().Add(time.Duration(p.config.WriteTimeoutMS) * time.Millisecond)); err != nil {
			lastErr = fmt.Errorf("set write deadline failed: %w", err)
			util.Error("⚠️ SetWriteDeadline error: %v", err)

			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		if _, err := conn.Write(payload); err != nil {
			lastErr = fmt.Errorf("write failed: %w", err)
			brokerAddr := p.producer.selectBroker()
			_ = p.producer.ReconnectPartition(part, brokerAddr, p.config.UseTLS, p.config.TLSCertPath, p.config.TLSKeyPath)
			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		if p.config.Acks == "0" {
			return &types.AckResponse{Status: "OK"}, nil
		}

		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(p.config.AckTimeoutMS) * time.Millisecond))
		resp, err := util.ReadWithLength(conn)
		_ = conn.SetReadDeadline(time.Time{})

		if err != nil {
			lastErr = fmt.Errorf("read ack failed: %w", err)
			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		util.Debug("Partition %d: Received Ack raw data: %s", part, resp)

		ackResp, err := p.parseAckResponse(resp)
		if err != nil {
			lastErr = err
			continue
		}

		return ackResp, nil
	}
	return nil, lastErr
}

func (p *Publisher) markBatchAckedByID(part int, batchID string, batchLen int) {
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

func (p *Publisher) GetLatencies() []time.Duration {
	p.bmMu.Lock()
	defer p.bmMu.Unlock()

	res := make([]time.Duration, len(p.bmLatencies))
	copy(res, p.bmLatencies)
	return res
}

func (p *Publisher) parseAckResponse(resp []byte) (*types.AckResponse, error) {
	respStr := string(resp)
	if strings.HasPrefix(respStr, "ERROR:") {
		ackResp := types.AckResponse{
			Status:   "ERROR",
			ErrorMsg: strings.TrimSpace(respStr),
		}
		util.Error("broker responded with error: %s", respStr)
		return &ackResp, fmt.Errorf("broker responded with error: %s", respStr)
	}

	var ackResp types.AckResponse
	if err := json.Unmarshal(resp, &ackResp); err != nil {
		util.Error("invalid ack format: %v, %w", string(resp), err)
		return nil, fmt.Errorf("invalid ack format: %v, %w", string(resp), err)
	}

	if ackResp.Leader != "" {
		if ackResp.Leader != p.producer.GetLeaderAddr() {
			p.producer.UpdateLeader(ackResp.Leader)
		}
	}

	if ackResp.ProducerID == "" {
		ackStr, err := json.Marshal(ackResp)
		if err != nil {
			util.Error("Failed to marshal response: %v", err)
			return nil, err
		}

		util.Info("missing producerID: expected %d, got %d, ack=%s", p.producer.Epoch, ackResp.ProducerEpoch, ackStr)
		return nil, fmt.Errorf("incomplete ack response: missing required fields")
	}

	if ackResp.ProducerEpoch != p.producer.Epoch {
		ackStr, err := json.Marshal(ackResp)
		if err != nil {
			util.Error("Failed to marshal response: %v", err)
			return nil, err
		}

		util.Info("epoch mismatch: expected %d, got %d, ack=%s", p.producer.Epoch, ackResp.ProducerEpoch, ackStr)
		return nil, fmt.Errorf("epoch mismatch: expected %d, got %d", p.producer.Epoch, ackResp.ProducerEpoch)
	}

	return &ackResp, nil
}

func (p *Publisher) VerifySentSequences(expectedCount int) error {
	totalSent := 0
	for part := 0; part < p.partitions; part++ {
		p.partitionSentMus[part].Lock()
		totalSent += len(p.partitionSentSeqs[part])
		p.partitionSentMus[part].Unlock()
	}

	if totalSent != expectedCount {
		return fmt.Errorf("expected %d messages sent, got %d", expectedCount, totalSent)
	}

	util.Info("All %d sequences sent successfully across all partitions", expectedCount)
	return nil
}

func (p *Publisher) Flush() {
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
			util.Info("Flush completed - all pending batches sent")
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	util.Warn("Flush timeout after %v", timeout)
}

func (p *Publisher) FlushBenchmark(expectedTotal int) {
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
		allInFlightClear := true
		for part := 0; part < p.partitions; part++ {
			inf := atomic.LoadInt32(&p.inFlight[part])
			if inf > 0 {
				util.Debug("Partition %d still has %d messages in-flight", part, inf) // todo
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
				util.Info("Flush completed - all expected messages (%d) acknowledged. (Elapsed: %.3fs)", expectedTotal, time.Since(deadline.Add(-timeout)).Seconds())
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	util.Warn("Flush timeout after %v. Only %d/%d messages acknowledged.", timeout, p.GetSentMessageCount(), expectedTotal)
}

func (p *Publisher) batchStateGC() {
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
						util.Warn("GC: Dropping unacked stale batch state: %s", id)
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

func (p *Publisher) Close() {
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

	if err := p.producer.Close(); err != nil {
		util.Debug("error closing producer: %v", err)
	}
}

// GetPartitionStats returns benchmark statistics for all partitions
func (p *Publisher) GetPartitionStats() []bench.PartitionStat {
	p.bmMu.Lock()
	defer p.bmMu.Unlock()

	stats := make([]bench.PartitionStat, 0, p.partitions)
	for part := 0; part < p.partitions; part++ {
		count := p.bmTotalCount[part]
		totalTime := p.bmTotalTime[part]
		var avg time.Duration
		if count > 0 {
			avg = totalTime / time.Duration(count)
		}
		stats = append(stats, bench.PartitionStat{
			PartitionID: part,
			BatchCount:  count,
			AvgDuration: avg,
		})
	}
	return stats
}

// GetSentMessageCount returns the number of successfully sent messages
func (p *Publisher) GetSentMessageCount() int {
	return int(p.ackedCount.Load())
}

// GetUniqueAckCount returns the number of unique messages
func (p *Publisher) GetUniqueAckCount() int {
	return int(p.uniqueCount.Load())
}

// GetattemptsCount returns the number of published messages
func (p *Publisher) GetAttemptsCount() int {
	return int(p.attemptsCount.Load())
}

// GetPartitionCount returns the number of partitions
func (p *Publisher) GetPartitionCount() int {
	return p.partitions
}
