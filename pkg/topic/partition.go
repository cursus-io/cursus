package topic

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
	broadcastCh   chan types.Message
	LEO           atomic.Uint64
	HWM           uint64
	mu            sync.RWMutex
	dh            types.StorageHandler
	closed        bool
	streamManager StreamManager
}

// NewPartition creates a partition instance.
func NewPartition(id int, topic string, dh types.StorageHandler, sm StreamManager, cfg *config.Config) *Partition {
	bufSize := DefaultBufSize
	if cfg != nil && cfg.BroadcastChannelBufferSize > 0 {
		bufSize = cfg.BroadcastChannelBufferSize
	}

	latest := dh.GetLatestOffset()
	initialOffset := latest + 1

	p := &Partition{
		id:            id,
		topic:         topic,
		dh:            dh,
		streamManager: sm,
		newMessageCh:  make(chan struct{}, 1),
		broadcastCh:   make(chan types.Message, bufSize),
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

	go p.runBroadcaster()
	return p
}

func (p *Partition) runBroadcaster() {
	for msg := range p.broadcastCh {
		p.broadcastToStreams(msg)
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

	offset, err := p.dh.AppendMessage(p.topic, p.id, &msg)
	if err != nil {
		util.Error("❌ Failed to enqueue message to disk [partition-%d]: %v", p.id, err)
		return
	}

	msg.Offset = offset
	p.LEO.Store(offset + 1)

	p.NotifyNewMessage()
	p.enqueueToBroadcast(msg)
}

func (p *Partition) EnqueueSync(msg types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	offset, err := p.dh.AppendMessageSync(p.topic, p.id, &msg)
	if err != nil {
		return fmt.Errorf("disk write failed: %w", err)
	}
	msg.Offset = offset
	p.LEO.Store(offset + 1)

	p.NotifyNewMessage()
	p.enqueueToBroadcast(msg)
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
		offset, err := p.dh.AppendMessageSync(p.topic, p.id, &msgs[i])
		if err != nil {
			p.NotifyNewMessage()
			return fmt.Errorf("disk write failed for partition %d: %w", p.id, err)
		}
		msgs[i].Offset = offset
		p.LEO.Store(offset + 1)
		p.enqueueToBroadcast(msgs[i])
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
		offset, err := p.dh.AppendMessage(p.topic, p.id, &msgs[i])
		if err != nil {
			p.NotifyNewMessage()
			return fmt.Errorf("batch enqueue failed at index %d: %w", i, err)
		}

		msgs[i].Offset = offset
		p.LEO.Store(offset + 1)
		p.enqueueToBroadcast(msgs[i])
	}
	p.NotifyNewMessage()
	return nil
}

func (p *Partition) enqueueToBroadcast(msg types.Message) {
	select {
	case p.broadcastCh <- msg:
	default:
		util.Warn("⚠️ partition %d: Broadcast channel full, dropping real-time delivery", p.id)
	}
}

func (p *Partition) broadcastToStreams(msg types.Message) {
	if p.streamManager == nil {
		return
	}

	streams := p.streamManager.GetStreamsForPartition(p.topic, p.id)
	for _, stream := range streams {
		conn := stream.Conn()
		if conn == nil {
			continue
		}

		if err := conn.SetWriteDeadline(time.Now().Add(1 * time.Second)); err != nil {
			util.Error("⚠️ SetWriteDeadline error: %v", err)
			continue
		}

		if err := util.WriteWithLength(conn, []byte(msg.Payload)); err != nil {
			util.Warn("Failed to broadcast to stream for topic '%s' partition %d: %v", p.topic, p.id, err)
			continue
		}

		stream.IncrementOffset()
		stream.SetLastActive(time.Now())
	}
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

// Close shuts down the partition.
func (p *Partition) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.broadcastCh)
}
