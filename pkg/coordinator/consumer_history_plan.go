package coordinator

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"reflect"

	"github.com/cursus-io/cursus/pkg/types"
)

// ConsumerHistoryCompactionPlan is a non-serializable certificate for replacing
// ONE atomic range (normally one closed segment). It is not a replay cache or a
// cluster activation gate. Only the owning coordinator and recovery lease may
// validate it. Cluster activation and storage installation are separate gates.
type ConsumerHistoryCompactionPlan struct {
	owner                         *Coordinator
	stamp                         consumerMetadataReplayStamp
	partition                     int
	first                         uint64
	end                           uint64
	bounds                        []consumerReplayIndexPartition
	removed                       []types.Message
	detached                      bool
	readRecords, readPayloadBytes int64
}

// ReadWork counts decoded source records and key/payload bytes, including
// repeated full/raw/filtered reads. These are not physical device I/O bytes.
func (p *ConsumerHistoryCompactionPlan) ReadWork() (int64, int64) {
	return p.readRecords, p.readPayloadBytes
}

func (p *ConsumerHistoryCompactionPlan) observeRead(messages []types.Message) {
	p.readRecords += int64(len(messages))
	for _, m := range messages {
		p.readPayloadBytes += int64(len(m.Key) + len(m.Payload))
	}
}

// RemovedRecords returns detached original records, in physical offset order.
// The storage installer must match the exact original range and generation,
// preserve original offsets and atomically replace just that range. A plan is
// NOT authorization to call the ordinary last-key-wins compactor.
func (p *ConsumerHistoryCompactionPlan) RemovedRecords() []types.Message {
	return append([]types.Message(nil), p.removed...)
}

// historyPlanReader deliberately does not expose the index interface. Both
// validation replays must read authoritative logs, even if a valid cache exists.
type historyPlanReader struct {
	ConsumerMetadataCommittedReader
	read func(string, int, uint64, int) ([]types.Message, error)
}

func (r *historyPlanReader) ReadCommittedConsumerMetadata(topic string, p int, next uint64, max int) ([]types.Message, error) {
	return r.read(topic, p, next, max)
}

// PrepareConsumerHistoryCompaction requires fixed ordinary read views for ALL
// metadata partitions, FSM transition exclusion and an exclusive recovery lease.
// It certifies a single half-open range. It never applies recovered state or
// writes a cache. The caller must separately prove a durable cluster activation
// and that this range is one atomically replaceable closed storage segment.
func (c *Coordinator) PrepareConsumerHistoryCompaction(source ConsumerMetadataIndexReader, partition int, first, end uint64) (*ConsumerHistoryCompactionPlan, error) {
	stamp, err := c.captureConsumerMetadataReplayStamp()
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, fmt.Errorf("consumer history certification requires a durable source")
	}
	count, err := source.ExistingPartitionCount(c.offsetTopic)
	if err != nil {
		return nil, err
	}
	if partition < 0 || partition >= count || first >= end {
		return nil, fmt.Errorf("invalid consumer history candidate range")
	}
	plan := &ConsumerHistoryCompactionPlan{owner: c, stamp: stamp, partition: partition, first: first, end: end}
	bounded := &boundedMetadataReader{bounds: make(map[int]metadataReadRange), read: source.ReadCommittedConsumerMetadata}
	selectors := make([]*indexedMetadataReader, count)
	digests := make([]hash.Hash, count)
	for p := 0; p < count; p++ {
		b, err := metadataBounds(source, c.offsetTopic, p)
		if err != nil {
			return nil, err
		}
		identity, err := source.ConsumerMetadataIndexIdentity(p)
		if err != nil {
			return nil, fmt.Errorf("consumer history source identity unavailable: %w", err)
		}
		if identity == "" {
			return nil, fmt.Errorf("consumer history source identity is empty")
		}
		bounded.bounds[p] = b
		plan.bounds = append(plan.bounds, consumerReplayIndexPartition{identity, b.first, b.end})
		selectors[p] = &indexedMetadataReader{eligible: true, selected: make(map[replayIndexKey]consumerReplayIndexRecord), epochs: make(map[string]uint64)}
		digests[p] = sha256.New()
	}
	b := bounded.bounds[partition]
	if first < b.first || end > b.end {
		return nil, fmt.Errorf("consumer history candidate exceeds committed range")
	}
	var candidates []types.Message
	// Match partition recovery's producer ordering, which is epoch/sequence/
	// offset, not simply physical arrival order. Also keep the last physical
	// producer record for legacy sequence-zero writers.
	producerHigh := make(map[string]types.Message)
	producerLast := make(map[string]uint64)
	fullReader := &historyPlanReader{ConsumerMetadataCommittedReader: source}
	fullReader.read = func(topic string, p int, next uint64, max int) ([]types.Message, error) {
		messages, err := bounded.ReadTopicPartition(topic, p, next, max)
		plan.observeRead(messages)
		if err != nil {
			return nil, err
		}
		for _, m := range messages {
			protected, err := protectedConsumerHistoryTransaction(m)
			if err != nil {
				return nil, err
			}
			if protected {
				continue
			}
			record, versioned, err := decodeConsumerMetadataRecord(m.Payload)
			if err != nil || !versioned || indexTransaction(m) || record.Type == ConsumerMetadataRecordTransactionalOffsetSnapshot || m.Key != consumerMetadataRecordKey(record) {
				return nil, fmt.Errorf("consumer history requires nontransactional versioned metadata at %d:%d", p, m.Offset)
			}
			if err := json.NewEncoder(digests[p]).Encode(m); err != nil {
				return nil, err
			}
			if p == partition && m.Offset >= first && m.Offset < end {
				candidates = append(candidates, m)
			}
			if p == partition && m.ProducerID != "" {
				producerLast[m.ProducerID] = m.Offset
				previous, exists := producerHigh[m.ProducerID]
				if m.SeqNum > 0 && (!exists || m.Epoch > previous.Epoch || m.Epoch == previous.Epoch && (m.SeqNum > previous.SeqNum || m.SeqNum == previous.SeqNum && m.Offset > previous.Offset)) {
					producerHigh[m.ProducerID] = m
				}
			}
		}
		return messages, selectors[p].observe(p, messages, false)
	}
	full, err := c.PrepareFullConsumerMetadataReplay(fullReader)
	if err != nil {
		return nil, err
	}
	for _, group := range full.scratch.groups {
		if group.RegistrationInferred || group.RegistrationEpoch == 0 {
			return nil, fmt.Errorf("consumer history contains inferred registration")
		}
	}
	for _, selector := range selectors {
		for group, epoch := range selector.epochs {
			if epoch > full.scratch.groupEpochs[group] {
				return nil, fmt.Errorf("consumer history contains an orphan future epoch")
			}
		}
	}
	// A committed reader can hide open/aborted transactions. Certify the raw
	// range independently, including all message metadata, before any deletion.
	raw := &boundedMetadataReader{bounds: bounded.bounds, read: func(_ string, p int, next uint64, max int) ([]types.Message, error) {
		return source.ReadRawConsumerMetadataIndex(p, next, max)
	}}
	for p, b := range plan.bounds {
		digest := sha256.New()
		for next := b.First; next < b.End; {
			messages, err := raw.ReadTopicPartition(c.offsetTopic, p, next, 1024)
			plan.observeRead(messages)
			if err != nil {
				return nil, err
			}
			if len(messages) == 0 {
				break
			}
			for _, m := range messages {
				next = m.Offset + 1
				protected, err := protectedConsumerHistoryTransaction(m)
				if err != nil {
					return nil, err
				}
				if protected {
					continue
				}
				if err := json.NewEncoder(digest).Encode(m); err != nil {
					return nil, err
				}
				next = m.Offset + 1
			}
		}
		if !reflect.DeepEqual(digest.Sum(nil), digests[p].Sum(nil)) {
			return nil, fmt.Errorf("consumer history raw and committed records differ")
		}
	}
	keep := make(map[uint64]bool)
	for _, entry := range selectors[partition].selected {
		keep[entry.Message.Offset] = true
	}
	for _, offset := range producerLast {
		keep[offset] = true
	}
	for _, message := range producerHigh {
		keep[message.Offset] = true
	}
	remove := make(map[uint64]bool)
	for _, m := range candidates {
		if !keep[m.Offset] {
			remove[m.Offset] = true
			plan.removed = append(plan.removed, m)
		}
	}
	filtered := &historyPlanReader{ConsumerMetadataCommittedReader: source}
	filtered.read = func(topic string, p int, next uint64, max int) ([]types.Message, error) {
		// Empty filtered pages must not prematurely end replay.
		for next < bounded.bounds[p].end {
			messages, err := bounded.ReadTopicPartition(topic, p, next, max)
			plan.observeRead(messages)
			if err != nil || len(messages) == 0 {
				return messages, err
			}
			next = messages[len(messages)-1].Offset + 1
			out := make([]types.Message, 0, len(messages))
			for _, m := range messages {
				if p != partition || !remove[m.Offset] {
					out = append(out, m)
				}
			}
			if len(out) != 0 {
				return out, nil
			}
		}
		return nil, nil
	}
	candidate, err := c.PrepareFullConsumerMetadataReplay(filtered)
	if err != nil {
		return nil, fmt.Errorf("consumer history candidate replay: %w", err)
	}
	if !reflect.DeepEqual(full.scratch.ExportState(), candidate.scratch.ExportState()) || !reflect.DeepEqual(full.scratch.metadataSources, candidate.scratch.metadataSources) {
		return nil, fmt.Errorf("consumer history candidate changes state or physical provenance")
	}
	if err := source.SyncConsumerMetadataIndex(); err != nil {
		return nil, err
	}
	if err := c.ValidateConsumerHistoryCompaction(source, plan); err != nil {
		return nil, err
	}
	return plan, nil
}

// Transactional offset intents do not supply ordinary snapshot authority:
// committed decisions materialize separate ordinary snapshots. Preserve every
// intent and control frame, including invisible open/aborted transactions. An
// ordinary lifecycle/offset record carrying physical transaction fields is an
// incompatible format and must not authorize deletion.
func protectedConsumerHistoryTransaction(m types.Message) (bool, error) {
	if m.TransactionMarker != types.TransactionMarkerNone {
		return true, validateCleanupTransactionControl(m)
	}
	r, versioned, err := decodeConsumerMetadataRecord(m.Payload)
	if err != nil {
		return false, err
	}
	if versioned && r.Type == ConsumerMetadataRecordTransactionalOffsetSnapshot {
		if indexTransaction(m) && (m.TransactionalID != r.TransactionalID || m.ProducerID != r.ProducerID || m.Epoch != r.ProducerEpoch) {
			return false, fmt.Errorf("consumer history transaction identity mismatch")
		}
		return true, nil
	}
	if indexTransaction(m) {
		return false, fmt.Errorf("consumer history requires versioned transaction intent")
	}
	return false, nil
}

// ValidateConsumerHistoryCompaction must run under the original FSM fence and
// lease, immediately before storage installation. It is necessary but not
// sufficient: storage must recheck the generation under its replacement lock.
func (c *Coordinator) ValidateConsumerHistoryCompaction(source ConsumerMetadataIndexReader, plan *ConsumerHistoryCompactionPlan) error {
	if source == nil || plan == nil || plan.owner != c {
		return fmt.Errorf("invalid consumer history certificate")
	}
	current, err := c.captureConsumerMetadataReplayStamp()
	if err != nil {
		return err
	}
	expected := plan.stamp
	if plan.detached {
		// The scanning lease belonged to the detached proof. Publication owns
		// a new live lease, but restore/compatibility generations must match.
		expected.lease = current.lease
	}
	if current != expected {
		return fmt.Errorf("consumer history authority changed")
	}
	count, err := source.ExistingPartitionCount(c.offsetTopic)
	if err != nil || count != len(plan.bounds) {
		return fmt.Errorf("consumer history partition count changed")
	}
	for p, old := range plan.bounds {
		b, err := metadataBounds(source, c.offsetTopic, p)
		if err != nil {
			return err
		}
		identity, err := source.ConsumerMetadataIndexIdentity(p)
		if err != nil || identity != old.Identity || b.first != old.First || b.end < old.End || !plan.detached && b.end != old.End {
			return fmt.Errorf("consumer history source changed")
		}
	}
	return nil
}
