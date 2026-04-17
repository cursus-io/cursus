package topic

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// Partition handles messages for one shard of a topic.
type Partition struct {
	id            int
	topic         string
	newMessageCh  chan struct{}
	LEO           atomic.Uint64
	HWM           uint64
	mu            sync.RWMutex
	dh            types.StorageHandler
	closed        bool
	streamManager StreamManager
	producerState sync.Map // map[string]uint64 (ProducerID -> LastSeqNum)
	isIdempotent  bool
}

// NewPartition creates a partition instance.
func NewPartition(id int, topic string, dh types.StorageHandler, sm StreamManager, cfg *config.Config) *Partition {
	latest := dh.GetLatestOffset()
	initialOffset := latest + 1

	p := &Partition{
		id:            id,
		topic:         topic,
		dh:            dh,
		streamManager: sm,
		newMessageCh:  make(chan struct{}, 1),
	}

	p.LEO.Store(initialOffset)
	p.HWM = initialOffset

	if handler, ok := dh.(*disk.DiskHandler); ok {
		p.HWM = handler.GetAbsoluteOffset()
		handler.OnSync = func(flushedOffset uint64) {
			p.mu.Lock()
			p.HWM = flushedOffset
			p.mu.Unlock()
			p.NotifyNewMessage()
		}
	}

	return p
}

func (p *Partition) isDuplicate(msg *types.Message) bool {
	if !p.isIdempotent {
		return false
	}
	// SeqNum == 0 means the producer did not explicitly set a sequence number;
	// skip dedup to avoid incorrectly rejecting every message after the first.
	if msg.ProducerID == "" || msg.SeqNum == 0 {
		return false
	}

	lastSeq, ok := p.producerState.Load(msg.ProducerID)
	if ok {
		if msg.SeqNum <= lastSeq.(uint64) {
			return true
		}
	}
	return false
}

func (p *Partition) updateProducerState(msg *types.Message) {
	if msg.ProducerID != "" {
		p.producerState.Store(msg.ProducerID, msg.SeqNum)
	}
}

// Enqueue pushes a message into the partition queue.
func (p *Partition) Enqueue(msg types.Message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		util.Warn("⚠️ Partition closed, dropping message [partition-%d]", p.id)
		return
	}

	if p.isDuplicate(&msg) {
		util.Debug("Partition %d: skipping duplicate message from producer %s (seq %d)", p.id, msg.ProducerID, msg.SeqNum)
		return
	}

	offset, err := p.dh.AppendMessage(p.topic, p.id, &msg)
	if err != nil {
		util.Error("❌ Failed to enqueue message to disk [partition-%d]: %v", p.id, err)
		return
	}

	p.updateProducerState(&msg)
	msg.Offset = offset
	p.LEO.Store(offset + 1)
	if p.HWM < offset+1 {
		p.HWM = offset + 1
	}

	p.NotifyNewMessage()
}

func (p *Partition) EnqueueSync(msg types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	if p.isDuplicate(&msg) {
		util.Debug("Partition %d: skipping duplicate message from producer %s (seq %d)", p.id, msg.ProducerID, msg.SeqNum)
		return nil
	}

	offset, err := p.dh.AppendMessageSync(p.topic, p.id, &msg)
	if err != nil {
		return fmt.Errorf("disk write failed: %w", err)
	}

	p.updateProducerState(&msg)
	msg.Offset = offset
	p.LEO.Store(offset + 1)
	if p.HWM < offset+1 {
		p.HWM = offset + 1
	}

	p.NotifyNewMessage()
	return nil
}

// EnqueueBatchSync pushes multiple messages into the partition queue synchronously.
func (p *Partition) EnqueueBatchSync(msgs []types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	for i := range msgs {
		if p.isDuplicate(&msgs[i]) {
			util.Debug("Partition %d: skipping duplicate message from producer %s (seq %d) in batch sync", p.id, msgs[i].ProducerID, msgs[i].SeqNum)
			continue
		}

		offset, err := p.dh.AppendMessageSync(p.topic, p.id, &msgs[i])
		if err != nil {
			p.NotifyNewMessage()
			return fmt.Errorf("disk write failed for partition %d: %w", p.id, err)
		}

		p.updateProducerState(&msgs[i])
		msgs[i].Offset = offset
		p.LEO.Store(offset + 1)
		if p.HWM < offset+1 {
			p.HWM = offset + 1
		}
	}
	p.NotifyNewMessage()
	return nil
}

// EnqueueBatch pushes multiple messages into the partition queue asynchronously.
func (p *Partition) EnqueueBatch(msgs []types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	for i := range msgs {
		if p.isDuplicate(&msgs[i]) {
			util.Debug("Partition %d: skipping duplicate message from producer %s (seq %d) in batch", p.id, msgs[i].ProducerID, msgs[i].SeqNum)
			continue
		}

		offset, err := p.dh.AppendMessage(p.topic, p.id, &msgs[i])
		if err != nil {
			p.NotifyNewMessage()
			return fmt.Errorf("batch enqueue failed at index %d: %w", i, err)
		}

		p.updateProducerState(&msgs[i])
		msgs[i].Offset = offset
		p.LEO.Store(offset + 1)
		if p.HWM < offset+1 {
			p.HWM = offset + 1
		}
	}
	p.NotifyNewMessage()
	return nil
}

func (p *Partition) NotifyNewMessage() {
	select {
	case p.newMessageCh <- struct{}{}:
	default:
	}
}

func (p *Partition) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	return p.dh.ReadMessages(offset, max)
}

func (p *Partition) ReadCommitted(offset uint64, max int) ([]types.Message, error) {
	p.mu.RLock()
	hwm := p.HWM
	p.mu.RUnlock()

	if offset >= hwm {
		return nil, nil
	}

	canRead := int(hwm - offset)
	if max > canRead {
		max = canRead
	}

	return p.ReadMessages(offset, max)
}

func (p *Partition) GetLatestOffset() uint64 {
	if p.dh == nil {
		return 0
	}
	return p.dh.GetLatestOffset()
}

func (p *Partition) ID() int {
	return p.id
}

// NextOffset returns the next available offset in the partition (Log End Offset).
func (p *Partition) NextOffset() uint64 {
	return p.LEO.Load()
}

func (p *Partition) ReserveOffsets(count int) uint64 {
	return p.LEO.Add(uint64(count)) - uint64(count)
}

func (p *Partition) SetHWM(hwm uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if hwm > p.HWM {
		p.HWM = hwm
		p.NotifyNewMessage()
	}
}

func (p *Partition) UpdateLEO(leo uint64) {
	p.LEO.Store(leo)
}

// Close shuts down the partition.
func (p *Partition) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
}
