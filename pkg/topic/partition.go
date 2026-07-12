package topic

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// producerEntry tracks the last producer epoch, sequence number, and activity time for a producer.
type producerEntry struct {
	lastEpoch int64
	lastSeq   uint64
	lastSeen  time.Time
}

type stagedProducerEntry struct {
	lastEpoch int64
	lastSeq   uint64
}

// PartitionOffsetRange describes the retained and committed offsets for a partition.
// Latest is the next readable committed offset, capped by the flushed disk tail.
type PartitionOffsetRange struct {
	Earliest uint64
	Latest   uint64
	LEO      uint64
	HWM      uint64
}

// Partition handles messages for one shard of a topic.
type Partition struct {
	id                int
	topic             string
	newMessageCh      chan struct{}
	LEO               atomic.Uint64
	HWM               uint64
	mu                sync.RWMutex
	dh                types.StorageHandler
	closed            bool
	streamManager     StreamManager
	hwmCheckpointPath string
	hwmCheckpointCh   chan struct{}
	hwmCheckpointMu   sync.Mutex
	hwmCheckpointWG   sync.WaitGroup
	producerStatePath string
	producerStateCh   chan struct{}
	producerStateMu   sync.Mutex
	producerStateWG   sync.WaitGroup
	producerState     sync.Map // map[string]*producerEntry
	isIdempotent      bool
	closeCh           chan struct{}
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
		closeCh:       make(chan struct{}),
	}

	p.LEO.Store(initialOffset)
	p.HWM = initialOffset

	if handler, ok := dh.(*disk.DiskHandler); ok {
		p.hwmCheckpointPath = hwmCheckpointPath(handler, id)
		p.hwmCheckpointCh = make(chan struct{}, 1)
		p.producerStatePath = producerStateCheckpointPath(handler, id)
		p.producerStateCh = make(chan struct{}, 1)
		durableTail := handler.GetAbsoluteOffset()
		if persistedHWM, ok := loadHWMCheckpoint(p.hwmCheckpointPath); ok {
			if persistedHWM > durableTail {
				util.Warn("clamping HWM checkpoint %s from %d to durable tail %d", p.hwmCheckpointPath, persistedHWM, durableTail)
				p.HWM = durableTail
			} else {
				p.HWM = persistedHWM
			}
		} else {
			p.HWM = durableTail
		}
		notifyCh := p.newMessageCh
		handler.SetOnSync(func(uint64) {
			select {
			case notifyCh <- struct{}{}:
			default:
			}
		})
	}

	if p.hwmCheckpointCh != nil {
		p.hwmCheckpointWG.Add(1)
		go p.runHWMCheckpointLoop()
	}
	if p.producerStateCh != nil {
		p.loadProducerStateCheckpoint()
	}

	return p
}

func (p *Partition) validateProducerMessage(msg *types.Message) (bool, error) {
	return p.validateProducerMessageWithStage(msg, nil, false)
}

func (p *Partition) validateProducerMessageWithStage(msg *types.Message, staged map[string]stagedProducerEntry, force bool) (bool, error) {
	if !p.isIdempotent && !force {
		return false, nil
	}
	if msg.ProducerID == "" {
		return false, nil
	}
	if msg.SeqNum == 0 {
		if force {
			return false, fmt.Errorf("idempotency error: producer %s must set seqNum > 0", msg.ProducerID)
		}
		// SeqNum == 0 means a non-transactional producer did not explicitly set a sequence number;
		// skip dedup to avoid incorrectly rejecting every message after the first.
		return false, nil
	}

	if staged != nil {
		if entry, ok := staged[msg.ProducerID]; ok {
			return p.validateAgainstProducerState(msg, entry.lastEpoch, entry.lastSeq, true)
		}
	}

	if val, ok := p.producerState.Load(msg.ProducerID); ok {
		entry := val.(*producerEntry)
		return p.validateAgainstProducerState(msg, entry.lastEpoch, entry.lastSeq, true)
	}

	if msg.SeqNum != 1 {
		return false, fmt.Errorf("idempotency error: first message for producer %s must have seqNum 1, got %d", msg.ProducerID, msg.SeqNum)
	}

	return false, nil
}
func (p *Partition) validateAgainstProducerState(msg *types.Message, lastEpoch int64, lastSeq uint64, allowDuplicate bool) (bool, error) {
	if msg.Epoch < lastEpoch {
		return false, fmt.Errorf("stale_producer_epoch producer=%s current=%d got=%d", msg.ProducerID, lastEpoch, msg.Epoch)
	}
	if msg.Epoch == lastEpoch {
		if allowDuplicate && msg.SeqNum <= lastSeq {
			metrics.SeqNumDuplicateTotal.WithLabelValues(p.topic, fmt.Sprintf("%d", p.id)).Inc()
			return true, nil
		}
		if msg.SeqNum != lastSeq+1 {
			return false, fmt.Errorf("idempotency gap for producer %s: expected %d, got %d", msg.ProducerID, lastSeq+1, msg.SeqNum)
		}
		return false, nil
	}
	if msg.SeqNum != 1 {
		return false, fmt.Errorf("idempotency error: first message in new producer epoch for producer %s must have seqNum 1, got %d", msg.ProducerID, msg.SeqNum)
	}
	return false, nil
}

func (p *Partition) updateProducerState(msg *types.Message) {
	p.updateProducerStateWithMode(msg, false)
}

func (p *Partition) updateProducerStateWithMode(msg *types.Message, force bool) {
	if (!p.isIdempotent && !force) || msg.ProducerID == "" {
		return
	}
	if msg.SeqNum > 0 {
		if val, ok := p.producerState.Load(msg.ProducerID); ok {
			entry := val.(*producerEntry)
			if msg.Epoch == entry.lastEpoch && msg.SeqNum > entry.lastSeq+1 {
				gap := msg.SeqNum - entry.lastSeq - 1
				metrics.SeqNumGapTotal.WithLabelValues(p.topic, fmt.Sprintf("%d", p.id), msg.ProducerID).Add(float64(gap))
				util.Warn("Partition %d: seqNum gap detected for producer %s: expected %d, got %d (gap=%d)",
					p.id, msg.ProducerID, entry.lastSeq+1, msg.SeqNum, gap)
			}
		}
	}
	p.producerState.Store(msg.ProducerID, &producerEntry{
		lastEpoch: msg.Epoch,
		lastSeq:   msg.SeqNum,
		lastSeen:  time.Now(),
	})
	p.signalProducerStateCheckpoint()
}

// Enqueue pushes a message into the partition queue.
func (p *Partition) Enqueue(msg types.Message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		util.Warn("⚠️ Partition closed, dropping message [partition-%d]", p.id)
		return
	}

	duplicate, err := p.validateProducerMessage(&msg)
	if err != nil {
		util.Warn("Partition %d: rejecting message from producer %s: %v", p.id, msg.ProducerID, err)
		return
	}
	if duplicate {
		util.Debug("Partition %d: skipping duplicate message from producer %s (epoch %d seq %d)", p.id, msg.ProducerID, msg.Epoch, msg.SeqNum)
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
	p.setHWMLocked(offset + 1)

	p.NotifyNewMessage()
}

func (p *Partition) EnqueueSync(msg types.Message) error {
	return p.enqueueSync(msg, false)
}

func (p *Partition) EnqueueSyncIdempotent(msg types.Message) error {
	return p.enqueueSync(msg, true)
}

func (p *Partition) enqueueSync(msg types.Message, forceIdempotent bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	duplicate, err := p.validateProducerMessageWithStage(&msg, nil, forceIdempotent)
	if err != nil {
		return err
	}
	if duplicate {
		util.Debug("Partition %d: skipping duplicate message from producer %s (epoch %d seq %d)", p.id, msg.ProducerID, msg.Epoch, msg.SeqNum)
		return nil
	}

	offset, err := p.dh.AppendMessageSync(p.topic, p.id, &msg)
	if err != nil {
		return fmt.Errorf("disk write failed: %w", err)
	}

	p.updateProducerStateWithMode(&msg, forceIdempotent)
	msg.Offset = offset
	p.LEO.Store(offset + 1)
	p.setHWMLocked(offset + 1)

	p.NotifyNewMessage()
	return nil
}
func batchHasTransactionalMessages(msgs []types.Message) bool {
	for _, msg := range msgs {
		if msg.TransactionalID != "" {
			return true
		}
	}
	return false
}

// EnqueueBatchSync pushes multiple messages into the partition queue synchronously.
func (p *Partition) EnqueueBatchSync(msgs []types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	for i := range msgs {
		duplicate, err := p.validateProducerMessage(&msgs[i])
		if err != nil {
			return err
		}
		if duplicate {
			util.Debug("Partition %d: skipping duplicate message from producer %s (epoch %d seq %d) in batch sync", p.id, msgs[i].ProducerID, msgs[i].Epoch, msgs[i].SeqNum)
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
		p.setHWMLocked(offset + 1)
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
		duplicate, err := p.validateProducerMessage(&msgs[i])
		if err != nil {
			return err
		}
		if duplicate {
			util.Debug("Partition %d: skipping duplicate message from producer %s (epoch %d seq %d) in batch", p.id, msgs[i].ProducerID, msgs[i].Epoch, msgs[i].SeqNum)
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
		p.setHWMLocked(offset + 1)
	}
	p.NotifyNewMessage()
	return nil
}

// EnqueueBatchLeader appends messages and updates LEO, but does NOT update HWM.
// Used by the partition leader in cluster mode. HWM is updated separately after
// successful replication, ensuring consumers never read unreplicated messages.
func (p *Partition) EnqueueBatchLeader(msgs []types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}
	if p.id > math.MaxInt32 {
		return fmt.Errorf("partition ID %d exceeds int32 range", p.id)
	}
	partitionID := int32(p.id) // #nosec G115 -- p.id is validated against math.MaxInt32 before narrowing.

	type pendingLeaderMessage struct {
		index int
	}

	nextOffset := p.LEO.Load()
	pending := make([]pendingLeaderMessage, 0, len(msgs))
	diskBatch := make([]types.DiskMessage, 0, len(msgs))
	staged := make(map[string]stagedProducerEntry)
	forceIdempotent := batchHasTransactionalMessages(msgs)

	for i := range msgs {
		duplicate, err := p.validateProducerMessageWithStage(&msgs[i], staged, forceIdempotent)
		if err != nil {
			return err
		}
		if duplicate {
			continue
		}
		if (p.isIdempotent || forceIdempotent) && msgs[i].ProducerID != "" && msgs[i].SeqNum > 0 {
			staged[msgs[i].ProducerID] = stagedProducerEntry{
				lastEpoch: msgs[i].Epoch,
				lastSeq:   msgs[i].SeqNum,
			}
		}

		offset := nextOffset
		nextOffset++
		msgs[i].Offset = offset
		pending = append(pending, pendingLeaderMessage{
			index: i,
		})
		diskBatch = append(diskBatch, types.DiskMessage{
			Topic:             p.topic,
			Partition:         partitionID,
			Offset:            offset,
			ProducerID:        msgs[i].ProducerID,
			SeqNum:            msgs[i].SeqNum,
			Epoch:             msgs[i].Epoch,
			Payload:           msgs[i].Payload,
			Key:               msgs[i].Key,
			EventType:         msgs[i].EventType,
			SchemaVersion:     msgs[i].SchemaVersion,
			AggregateVersion:  msgs[i].AggregateVersion,
			Metadata:          msgs[i].Metadata,
			TransactionalID:   msgs[i].TransactionalID,
			TransactionState:  msgs[i].TransactionState,
			TransactionMarker: msgs[i].TransactionMarker,
		})
	}

	if len(diskBatch) == 0 {
		p.NotifyNewMessage()
		return nil
	}

	if err := p.dh.WriteBatch(diskBatch); err != nil {
		for _, msg := range pending {
			msgs[msg.index].Offset = 0
		}
		p.NotifyNewMessage()
		return fmt.Errorf("leader batch write failed: %w", err)
	}

	for _, msg := range pending {
		p.updateProducerStateWithMode(&msgs[msg.index], forceIdempotent)
	}
	p.LEO.Store(nextOffset)
	p.NotifyNewMessage()
	return nil
}

// AdvanceHWM sets HWM to the current LEO. Called after successful replication.
func (p *Partition) AdvanceHWM() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setHWMLocked(p.LEO.Load())
}

// ReplicaAppend writes messages with pre-assigned offsets from the leader (follower replication).
// It preserves the leader's offset assignments and updates LEO/HWM accordingly.
func (p *Partition) ReplicaAppend(msgs []types.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	for i := range msgs {
		if err := p.dh.AppendMessageWithOffset(p.topic, p.id, &msgs[i]); err != nil {
			return fmt.Errorf("replica append failed at index %d: %w", i, err)
		}

		newLEO := msgs[i].Offset + 1
		if currentLEO := p.LEO.Load(); newLEO > currentLEO {
			p.LEO.Store(newLEO)
		}
		p.setHWMLocked(newLEO)
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

	// Cap at flushed offset to avoid reading data not yet on disk.
	flushed := p.dh.GetFlushedOffset()
	if flushed < hwm {
		hwm = flushed
	}

	earliest := p.dh.GetFirstOffset()
	if offset < earliest {
		return nil, &types.OffsetOutOfRangeError{
			Requested: offset,
			Earliest:  earliest,
			Latest:    hwm,
		}
	}

	if offset >= hwm {
		return nil, nil
	}

	canRead := hwm - offset
	if canRead <= math.MaxInt && max > int(canRead) { // #nosec G115 -- canRead is bounded by math.MaxInt before narrowing.
		max = int(canRead) // #nosec G115 -- canRead is bounded by math.MaxInt before narrowing.
	}

	return p.readVisibleCommitted(offset, max, hwm)
}

func (p *Partition) readVisibleCommitted(offset uint64, max int, hwm uint64) ([]types.Message, error) {
	if max <= 0 {
		return nil, nil
	}

	messages, err := p.readCommittedScanRange(offset, hwm, max)
	if err != nil {
		return nil, err
	}

	markers := make(map[transactionMarkerKey]transactionMarkerInfo)
	for _, msg := range messages {
		if msg.Offset >= hwm {
			break
		}
		if msg.TransactionMarker != types.TransactionMarkerNone && msg.TransactionalID != "" {
			markers[messageTransactionMarkerKey(msg)] = transactionMarkerInfo{marker: msg.TransactionMarker, offset: msg.Offset}
		}
	}

	firstUnresolved := hwm
	for _, msg := range messages {
		if msg.Offset >= hwm {
			break
		}
		if msg.TransactionalID == "" || msg.TransactionState != types.TransactionStateOpen {
			continue
		}
		if !hasResolvingMarkerAfter(msg, markers) && msg.Offset < firstUnresolved {
			firstUnresolved = msg.Offset
		}
	}

	visible := make([]types.Message, 0, max)
	for _, msg := range messages {
		if msg.Offset >= hwm || msg.Offset >= firstUnresolved {
			break
		}
		if isReadCommittedVisible(msg, markers) {
			visible = append(visible, msg)
			if len(visible) == max {
				break
			}
		}
	}
	return visible, nil
}

func (p *Partition) readCommittedScanRange(offset uint64, hwm uint64, maxVisible int) ([]types.Message, error) {
	if offset >= hwm {
		return nil, nil
	}

	messages := make([]types.Message, 0)
	current := offset
	const scanBatchSize = 1024
	for current < hwm {
		remaining := hwm - current
		readMax := scanBatchSize
		if remaining <= math.MaxInt && readMax > int(remaining) { // #nosec G115 -- remaining is bounded by math.MaxInt before narrowing.
			readMax = int(remaining) // #nosec G115 -- remaining is bounded by math.MaxInt before narrowing.
		}
		if readMax <= 0 {
			break
		}

		batch, err := p.ReadMessages(current, readMax)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		for _, msg := range batch {
			if msg.Offset >= hwm {
				break
			}
			messages = append(messages, msg)
			next := msg.Offset + 1
			if next <= current {
				next = current + 1
			}
			current = next
		}
		if len(batch) < readMax || readCommittedScanHasEnoughVisible(messages, hwm, maxVisible) {
			break
		}
	}
	return messages, nil
}

func readCommittedScanHasEnoughVisible(messages []types.Message, hwm uint64, maxVisible int) bool {
	if maxVisible <= 0 {
		return true
	}
	markers := make(map[transactionMarkerKey]transactionMarkerInfo)
	for _, msg := range messages {
		if msg.Offset >= hwm {
			break
		}
		if msg.TransactionMarker != types.TransactionMarkerNone && msg.TransactionalID != "" {
			markers[messageTransactionMarkerKey(msg)] = transactionMarkerInfo{marker: msg.TransactionMarker, offset: msg.Offset}
		}
	}

	firstUnresolved := hwm
	for _, msg := range messages {
		if msg.Offset >= hwm {
			break
		}
		if msg.TransactionalID == "" || msg.TransactionState != types.TransactionStateOpen {
			continue
		}
		if !hasResolvingMarkerAfter(msg, markers) && msg.Offset < firstUnresolved {
			firstUnresolved = msg.Offset
		}
	}
	if firstUnresolved != hwm {
		return false
	}

	visible := 0
	for _, msg := range messages {
		if msg.Offset >= hwm {
			break
		}
		if isReadCommittedVisible(msg, markers) {
			visible++
			if visible >= maxVisible {
				return true
			}
		}
	}
	return false
}

type transactionMarkerKey struct {
	transactionalID string
	epoch           int64
}

type transactionMarkerInfo struct {
	marker string
	offset uint64
}

func messageTransactionMarkerKey(msg types.Message) transactionMarkerKey {
	return transactionMarkerKey{transactionalID: msg.TransactionalID, epoch: msg.Epoch}
}

func hasResolvingMarkerAfter(msg types.Message, markers map[transactionMarkerKey]transactionMarkerInfo) bool {
	marker, ok := markers[messageTransactionMarkerKey(msg)]
	return ok && marker.offset > msg.Offset
}

func isReadCommittedVisible(msg types.Message, markers map[transactionMarkerKey]transactionMarkerInfo) bool {
	if msg.TransactionMarker != types.TransactionMarkerNone {
		return false
	}
	if msg.TransactionalID == "" {
		return true
	}
	if msg.TransactionState == types.TransactionStateAborted {
		return false
	}
	marker, ok := markers[messageTransactionMarkerKey(msg)]
	return ok && marker.offset > msg.Offset && marker.marker == types.TransactionMarkerCommit
}

func (p *Partition) RecoverProducerStateFromLog() {
	if p.dh == nil || p.producerStateCh == nil {
		return
	}

	first := p.dh.GetFirstOffset()
	durableTail := p.dh.GetAbsoluteOffset()
	if durableTail <= first {
		return
	}
	offset := first
	const batchSize = 1024
	now := time.Now()

	for {
		msgs, err := p.dh.ReadMessages(offset, batchSize)
		if err != nil || len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			if msg.ProducerID != "" && msg.SeqNum > 0 {
				p.producerState.Store(msg.ProducerID, &producerEntry{lastEpoch: msg.Epoch, lastSeq: msg.SeqNum, lastSeen: now})
			}
			next := msg.Offset + 1
			if next <= offset {
				next = offset + 1
			}
			offset = next
		}
		if len(msgs) < batchSize || offset >= durableTail {
			break
		}
	}
	p.signalProducerStateCheckpoint()
}

func (p *Partition) StartProducerStateMaintenance() {
	if p.producerStateCh == nil {
		return
	}
	p.producerStateWG.Add(1)
	go p.runProducerStateCheckpointLoop()
	go p.runProducerCleanup()
}

// FlushDisk forces all pending async writes to disk.
func (p *Partition) FlushDisk() {
	p.dh.Flush()
	p.persistHWMCheckpoint()
	p.persistProducerStateCheckpoint()
}

func (p *Partition) GetFirstOffset() uint64 {
	if p.dh == nil {
		return 0
	}
	return p.dh.GetFirstOffset()
}
func (p *Partition) GetLatestOffset() uint64 {
	if p.dh == nil {
		return 0
	}
	return p.dh.GetLatestOffset()
}

func (p *Partition) OffsetRange() PartitionOffsetRange {
	if p.dh == nil {
		return PartitionOffsetRange{}
	}

	p.mu.RLock()
	hwm := p.HWM
	p.mu.RUnlock()

	latest := hwm
	flushed := p.dh.GetFlushedOffset()
	if flushed < latest {
		latest = flushed
	}

	return PartitionOffsetRange{
		Earliest: p.dh.GetFirstOffset(),
		Latest:   latest,
		LEO:      p.LEO.Load(),
		HWM:      hwm,
	}
}
func (p *Partition) ID() int {
	return p.id
}

// NextOffset returns the next available offset in the partition (Log End Offset).
func (p *Partition) NextOffset() uint64 {
	return p.LEO.Load()
}

func (p *Partition) ReserveOffsets(count int) (uint64, error) {
	if count <= 0 {
		return p.LEO.Load(), nil
	}
	delta := uint64(count) // #nosec G115 -- count is positive before widening.
	for {
		current := p.LEO.Load()
		if delta > math.MaxUint64-current {
			return current, fmt.Errorf("offset reservation overflow: current=%d count=%d", current, count)
		}
		if p.LEO.CompareAndSwap(current, current+delta) {
			return current, nil
		}
	}
}

func (p *Partition) SetHWM(hwm uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.setHWMLocked(hwm) {
		p.NotifyNewMessage()
	}
}

func (p *Partition) setHWMLocked(hwm uint64) bool {
	if hwm <= p.HWM {
		return false
	}
	p.HWM = hwm
	p.signalHWMCheckpointLocked()
	return true
}

func (p *Partition) signalHWMCheckpointLocked() {
	if p.hwmCheckpointCh == nil {
		return
	}
	select {
	case p.hwmCheckpointCh <- struct{}{}:
	default:
	}
}

func (p *Partition) runHWMCheckpointLoop() {
	defer p.hwmCheckpointWG.Done()

	ticker := time.NewTicker(hwmCheckpointInterval)
	defer ticker.Stop()

	dirty := false
	for {
		select {
		case <-p.hwmCheckpointCh:
			dirty = true
		case <-ticker.C:
			if dirty {
				p.persistHWMCheckpoint()
				dirty = false
			}
		case <-p.closeCh:
			p.persistHWMCheckpoint()
			return
		}
	}
}

func (p *Partition) persistHWMCheckpoint() {
	p.mu.RLock()
	checkpointPath := p.hwmCheckpointPath
	hwm := p.HWM
	p.mu.RUnlock()

	if checkpointPath == "" {
		return
	}

	p.hwmCheckpointMu.Lock()
	defer p.hwmCheckpointMu.Unlock()

	tmp := checkpointPath + ".tmp"
	data := []byte(strconv.FormatUint(hwm, 10) + "\n")
	// #nosec G304 -- checkpoint path is derived from the broker-owned partition log directory.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		util.Warn("failed to open HWM checkpoint %s: %v", tmp, err)
		return
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		util.Warn("failed to write HWM checkpoint %s: %v", tmp, err)
		return
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		util.Warn("failed to sync HWM checkpoint %s: %v", tmp, err)
		return
	}
	if err := f.Close(); err != nil {
		util.Warn("failed to close HWM checkpoint %s: %v", tmp, err)
		return
	}
	if err := replaceCheckpointFile(tmp, checkpointPath); err != nil {
		util.Warn("failed to rename HWM checkpoint %s: %v", checkpointPath, err)
		return
	}
	syncParentDir(filepath.Dir(checkpointPath))
}

func syncParentDir(path string) {
	if runtime.GOOS == "windows" {
		return
	}
	// #nosec G304 -- checkpoint directory is derived from the broker-owned partition log directory.
	dir, err := os.Open(path)
	if err != nil {
		util.Warn("failed to open HWM checkpoint directory %s: %v", path, err)
		return
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		util.Warn("failed to sync HWM checkpoint directory %s: %v", path, err)
	}
}

func hwmCheckpointPath(dh types.StorageHandler, partitionID int) string {
	if dh == nil {
		return ""
	}
	segmentPath := dh.GetSegmentPath(0)
	if segmentPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(segmentPath), fmt.Sprintf("partition_%d.hwm", partitionID))
}

func loadHWMCheckpoint(path string) (uint64, bool) {
	if path == "" {
		return 0, false
	}
	// #nosec G304 -- checkpoint path is derived from the broker-owned partition log directory.
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	hwm, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		util.Warn("ignoring invalid HWM checkpoint %s: %v", path, err)
		return 0, false
	}
	return hwm, true
}

type producerStateCheckpoint map[string]producerStateCheckpointEntry

type producerStateCheckpointEntry struct {
	Epoch int64  `json:"epoch"`
	Seq   uint64 `json:"seq"`
}

func (p *Partition) signalProducerStateCheckpoint() {
	if p.producerStateCh == nil {
		return
	}
	select {
	case p.producerStateCh <- struct{}{}:
	default:
	}
}

func (p *Partition) runProducerStateCheckpointLoop() {
	defer p.producerStateWG.Done()

	ticker := time.NewTicker(producerStateCheckpointInterval)
	defer ticker.Stop()

	dirty := false
	for {
		select {
		case <-p.producerStateCh:
			dirty = true
		case <-ticker.C:
			if dirty {
				p.persistProducerStateCheckpoint()
				dirty = false
			}
		case <-p.closeCh:
			p.persistProducerStateCheckpoint()
			return
		}
	}
}

func (p *Partition) loadProducerStateCheckpoint() {
	if p.producerStatePath == "" {
		return
	}
	// #nosec G304 -- checkpoint path is derived from the broker-owned partition log directory.
	data, err := os.ReadFile(p.producerStatePath)
	if err != nil {
		return
	}
	var checkpoint producerStateCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		var legacy map[string]uint64
		if legacyErr := json.Unmarshal(data, &legacy); legacyErr != nil {
			util.Warn("ignoring invalid producer state checkpoint %s: %v", p.producerStatePath, err)
			return
		}
		checkpoint = make(producerStateCheckpoint, len(legacy))
		for producerID, lastSeq := range legacy {
			checkpoint[producerID] = producerStateCheckpointEntry{Seq: lastSeq}
		}
	}
	now := time.Now()
	for producerID, entry := range checkpoint {
		if producerID == "" || entry.Seq == 0 {
			continue
		}
		p.producerState.Store(producerID, &producerEntry{lastEpoch: entry.Epoch, lastSeq: entry.Seq, lastSeen: now})
	}
}

func (p *Partition) persistProducerStateCheckpoint() {
	if p.producerStatePath == "" {
		return
	}

	checkpoint := make(producerStateCheckpoint)
	p.producerState.Range(func(key, value any) bool {
		producerID, ok := key.(string)
		if !ok || producerID == "" {
			return true
		}
		entry, ok := value.(*producerEntry)
		if !ok || entry.lastSeq == 0 {
			return true
		}
		checkpoint[producerID] = producerStateCheckpointEntry{Epoch: entry.lastEpoch, Seq: entry.lastSeq}
		return true
	})

	p.producerStateMu.Lock()
	defer p.producerStateMu.Unlock()

	tmp := p.producerStatePath + ".tmp"
	data, err := json.Marshal(checkpoint)
	if err != nil {
		util.Warn("failed to marshal producer state checkpoint %s: %v", p.producerStatePath, err)
		return
	}
	data = append(data, '\n')

	// #nosec G304 -- checkpoint path is derived from the broker-owned partition log directory.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		util.Warn("failed to open producer state checkpoint %s: %v", tmp, err)
		return
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		util.Warn("failed to write producer state checkpoint %s: %v", tmp, err)
		return
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		util.Warn("failed to sync producer state checkpoint %s: %v", tmp, err)
		return
	}
	if err := f.Close(); err != nil {
		util.Warn("failed to close producer state checkpoint %s: %v", tmp, err)
		return
	}
	if err := replaceCheckpointFile(tmp, p.producerStatePath); err != nil {
		util.Warn("failed to rename producer state checkpoint %s: %v", p.producerStatePath, err)
		return
	}
	syncParentDir(filepath.Dir(p.producerStatePath))
}

func producerStateCheckpointPath(dh types.StorageHandler, partitionID int) string {
	if dh == nil {
		return ""
	}
	segmentPath := dh.GetSegmentPath(0)
	if segmentPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(segmentPath), fmt.Sprintf("partition_%d.producers", partitionID))
}

// GetHWM returns the high water mark in a thread-safe manner.
func (p *Partition) GetHWM() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.HWM
}

func (p *Partition) UpdateLEO(leo uint64) {
	p.LEO.Store(leo)
}

// Close shuts down the partition.
func (p *Partition) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.closeCh)
	p.mu.Unlock()
	p.hwmCheckpointWG.Wait()
	p.producerStateWG.Wait()
}

const hwmCheckpointInterval = 250 * time.Millisecond

const producerStateCheckpointInterval = 250 * time.Millisecond

const producerStateTTL = 30 * time.Minute

// cleanStaleProducers removes producer entries that have not been seen within the TTL.
func (p *Partition) cleanStaleProducers() {
	cutoff := time.Now().Add(-producerStateTTL)
	p.producerState.Range(func(key, value any) bool {
		if entry := value.(*producerEntry); entry.lastSeen.Before(cutoff) {
			p.producerState.Delete(key)
		}
		return true
	})
}

// runProducerCleanup periodically evicts stale producer state to bound memory usage.
func (p *Partition) runProducerCleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.cleanStaleProducers()
		case <-p.closeCh:
			return
		}
	}
}
