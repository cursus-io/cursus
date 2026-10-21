package coordinator

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/types"
)

const (
	topicCleanupRecords = 128
	topicCleanupBytes   = 1 << 20
	topicCleanupGroups  = 64
	topicCleanupTopics  = 64
)

// ConsumerCleanupPartition identifies an immutable committed prefix. Identity
// must be comparable and change when the underlying partition is replaced.
type ConsumerCleanupPartition struct {
	Identity   any
	Generation uint64
	Start      uint64
	End        uint64
}

// ConsumerCleanupPage counts all storage work, including filtered records.
type ConsumerCleanupPage = types.ReplayPage

// ConsumerCleanupView is valid only inside WithConsumerCleanupView. The reader
// fences partition replacement/rollback until the callback returns.
type ConsumerCleanupView interface {
	Partitions() []ConsumerCleanupPartition
	Read(partition int, offset uint64, cursor types.ReplayCursor, maxRecords, maxBytes int) (ConsumerCleanupPage, error)
}

type consumerCleanupReader interface {
	WithConsumerCleanupView(func(ConsumerCleanupView) error) error
}

// TopicCleanupPending is structured progress for a committed topic intent that
// is waiting for local consumer metadata. It never contains record payloads.
type TopicCleanupPending struct {
	Topic     string `json:"topic"`
	Group     string `json:"group,omitempty"`
	Epoch     uint64 `json:"epoch,omitempty"`
	Partition int    `json:"partition"`
	Next      uint64 `json:"next"`
	End       uint64 `json:"end"`
	Records   uint64 `json:"records"`
	Bytes     uint64 `json:"bytes"`
	Reads     uint64 `json:"reads"`
	Segment   uint64 `json:"segment"`
	Position  uint64 `json:"position"`
	Reason    string `json:"reason"`
}

func (p *TopicCleanupPending) Error() string {
	return fmt.Sprintf("consumer cleanup pending topic=%q group=%q epoch=%d partition=%d next=%d end=%d records=%d bytes=%d reads=%d reason=%s",
		p.Topic, p.Group, p.Epoch, p.Partition, p.Next, p.End, p.Records, p.Bytes, p.Reads, p.Reason)
}

type cleanupEvidence struct {
	epoch     uint64
	liveEpoch uint64
	kind      string
	topic     string
	digest    [32]byte
}

type cleanupTarget struct {
	group      *GroupMetadata
	epoch      uint64
	generation int
	evidence   cleanupEvidence
}

type topicCleanupScan struct {
	current    func() bool
	targets    map[string]*cleanupTarget
	partitions []ConsumerCleanupPartition
	next       []uint64
	cursors    []types.ReplayCursor
	status     TopicCleanupPending
}

// ReconcileTopicGroups preserves standalone cleanup and makes distributed
// materialization replay committed tombstones before consulting stale members.
func (c *Coordinator) ReconcileTopicGroups(topicName string) ([]string, error) {
	return c.ReconcileTopicGroupsIf(topicName, func() bool { return true })
}

// CheckTopicGroupCleanup is the nonblocking Raft-Apply path. Both replay and
// new writes run in reconciliation: even waiting for a coordinator lock here
// could block a writer whose partition commit needs the same Raft apply loop.
func (c *Coordinator) CheckTopicGroupCleanup(topicName string) ([]string, error) {
	if c.standalone {
		return c.DeleteInactiveGroupsForTopic(topicName)
	}
	pending := &TopicCleanupPending{Topic: topicName, Reason: "consumer cleanup scheduled outside Raft apply"}
	if !c.mu.TryLock() {
		return nil, pending
	}
	defer c.mu.Unlock()
	for name, group := range c.groups {
		if group == nil {
			continue
		}
		if !group.mu.TryRLock() {
			return nil, pending
		}
		_, found := topicGroupReferenceLocked(name, group, topicName)
		epoch := group.RegistrationEpoch
		group.mu.RUnlock()
		if found {
			pending.Group, pending.Epoch = name, epoch
			return nil, pending
		}
	}
	return nil, nil
}

// ReconcileTopicGroupsIf validates the caller's topic intent after capturing
// group targets under lifecycleMu. The callback must not mutate coordinator
// state and may be retained to reclaim superseded scans on cache admission.
// New registrations cannot replace those targets until cleanup returns.
func (c *Coordinator) ReconcileTopicGroupsIf(topicName string, current func() bool) ([]string, error) {
	if c.standalone {
		if !current() {
			return nil, fmt.Errorf("topic cleanup intent changed")
		}
		return c.DeleteInactiveGroupsForTopic(topicName)
	}
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()

	// Ownership recovery may itself take lifecycleMu. Never invoke it while
	// holding lifecycleMu or the metadata read view.
	c.mu.RLock()
	checkOwner := c.groupOwnerChecker
	c.mu.RUnlock()
	owners := make(map[string]bool)
	references := c.TopicGroupReferences(topicName)
	for i, reference := range references {
		if i == topicCleanupGroups {
			break
		}
		if checkOwner != nil {
			owners[reference.Name] = checkOwner(reference.Name)
		}
	}

	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	references = c.TopicGroupReferences(topicName)
	if len(references) == 0 {
		delete(c.cleanupProgress, topicName)
		return nil, nil
	}
	for _, reference := range references {
		// Stale members may be covered by a committed deletion. The remaining
		// scope protections cannot be bypassed by follower reconciliation.
		reference.MemberCount = 0
		if err := reference.validateCleanup(topicName); err != nil {
			return nil, err
		}
	}
	reader, ok := c.topicHandler.(consumerCleanupReader)
	if !ok {
		return nil, &TopicCleanupPending{Topic: topicName, Reason: "committed metadata view unavailable"}
	}
	if c.cleanupProgress == nil {
		c.cleanupProgress = make(map[string]*topicCleanupScan)
	}
	scan := c.cleanupProgress[topicName]
	if !c.cleanupTargetsCurrent(scan, references) {
		if scan == nil && len(c.cleanupProgress) >= topicCleanupTopics {
			// Preserve unfinished scans so a large pending-topic set cannot
			// continually evict every cursor before it reaches the log tail.
			for name, candidate := range c.cleanupProgress {
				if (candidate.current != nil && !candidate.current()) ||
					(len(candidate.partitions) > 0 && candidate.matches(candidate.partitions)) {
					delete(c.cleanupProgress, name)
					break
				}
			}
			if len(c.cleanupProgress) >= topicCleanupTopics {
				return nil, &TopicCleanupPending{Topic: topicName, Reason: "waiting for cleanup scan capacity"}
			}
		}
		scan = c.newTopicCleanupScan(topicName, references)
		c.cleanupProgress[topicName] = scan
	}
	if !current() {
		delete(c.cleanupProgress, topicName)
		return nil, fmt.Errorf("topic cleanup intent changed")
	}
	scan.current = current
	var deleted []string
	err := reader.WithConsumerCleanupView(scan.read)
	if err == nil {
		// CommitOffset holds c.mu while consulting the topic manager. Acquire
		// locks in that same order when applying proof; never take c.mu from a
		// read view. Disk I/O above does not hold the global coordinator lock.
		c.mu.Lock()
		err = reader.WithConsumerCleanupView(func(view ConsumerCleanupView) error {
			if !scan.matches(view.Partitions()) {
				return fmt.Errorf("committed metadata view changed before cleanup apply")
			}
			for _, reference := range references {
				target := scan.targets[reference.Name]
				if target == nil {
					continue
				}
				group := c.groups[reference.Name]
				if group != target.group || group == nil {
					return fmt.Errorf("local group replaced during cleanup")
				}
				group.mu.Lock()
				evidence := target.evidence
				ref, found := topicGroupReferenceLocked(reference.Name, group, topicName)
				ref.MemberCount = 0
				valid := found && ref.validateCleanup(topicName) == nil &&
					group.RegistrationEpoch == target.epoch && group.Generation == target.generation &&
					!c.lifecyclePending[reference.Name] && evidence.kind == ConsumerMetadataRecordTombstone &&
					evidence.topic == topicName && evidence.epoch > group.RegistrationEpoch &&
					evidence.epoch > c.groupEpochs[reference.Name] && evidence.epoch > evidence.liveEpoch
				group.mu.Unlock()
				if !valid {
					continue
				}
				delete(c.groups, reference.Name)
				c.groupEpochs[reference.Name] = evidence.epoch
				delete(c.ownershipSince, reference.Name)
				deleted = append(deleted, reference.Name)
			}
			return nil
		})
		c.mu.Unlock()
	}
	if len(deleted) > 0 {
		c.updateOffsetPartitionCount()
		delete(c.cleanupProgress, topicName)
	}
	if err != nil {
		scan.status.Reason = err.Error()
		status := scan.status
		return deleted, &status
	}
	// Only a recovered owner can originate a new deletion. The configured
	// durable writer must additionally fence ownership at append time.
	for _, reference := range c.TopicGroupReferences(topicName) {
		if owners[reference.Name] {
			target := scan.targets[reference.Name]
			if target == nil || target.evidence.epoch > target.epoch || target.evidence.liveEpoch > target.epoch {
				continue
			}
			c.offsetRecordWriterMu.RLock()
			writerAvailable := c.offsetRecordWriter != nil
			c.offsetRecordWriterMu.RUnlock()
			if !writerAvailable {
				return deleted, &TopicCleanupPending{Topic: topicName, Group: reference.Name, Reason: "durable group writer unavailable"}
			}
			if err := c.deleteGroupWithFenceLocked(reference.Name, topicName, target.group); err != nil {
				scan.status.Group = reference.Name
				scan.status.Reason = err.Error()
				status := scan.status
				return deleted, &status
			}
			deleted = append(deleted, reference.Name)
		}
	}
	remaining := c.TopicGroupReferences(topicName)
	if len(remaining) == 0 {
		delete(c.cleanupProgress, topicName)
		return deleted, nil
	}
	scan.status.Group = remaining[0].Name
	if target := scan.targets[remaining[0].Name]; target != nil {
		scan.status.Epoch = target.epoch
	}
	scan.status.Reason = "waiting for a newer committed group tombstone"
	status := scan.status
	return deleted, &status
}

func (scan *topicCleanupScan) matches(partitions []ConsumerCleanupPartition) bool {
	if len(partitions) != len(scan.partitions) {
		return false
	}
	for i, partition := range partitions {
		if partition != scan.partitions[i] || scan.next[i] != partition.End {
			return false
		}
	}
	return true
}

func (c *Coordinator) cleanupTargetsCurrent(scan *topicCleanupScan, refs []TopicGroupReference) bool {
	if scan == nil || len(scan.targets) != min(len(refs), topicCleanupGroups) {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ref := range refs[:min(len(refs), topicCleanupGroups)] {
		target := scan.targets[ref.Name]
		group := c.groups[ref.Name]
		if target == nil || group == nil || target.group != group {
			return false
		}
		group.mu.RLock()
		same := group.RegistrationEpoch == target.epoch && group.Generation == target.generation
		group.mu.RUnlock()
		if !same {
			return false
		}
	}
	return true
}

func (c *Coordinator) newTopicCleanupScan(topicName string, refs []TopicGroupReference) *topicCleanupScan {
	scan := &topicCleanupScan{targets: make(map[string]*cleanupTarget), status: TopicCleanupPending{Topic: topicName}}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ref := range refs[:min(len(refs), topicCleanupGroups)] {
		group := c.groups[ref.Name]
		if group == nil {
			continue
		}
		group.mu.RLock()
		scan.targets[ref.Name] = &cleanupTarget{group: group, epoch: group.RegistrationEpoch, generation: group.Generation}
		group.mu.RUnlock()
	}
	return scan
}

func (scan *topicCleanupScan) read(view ConsumerCleanupView) error {
	partitions := view.Partitions()
	if len(partitions) == 0 {
		return fmt.Errorf("consumer metadata partitions unavailable")
	}
	reset := len(partitions) != len(scan.partitions)
	for i, partition := range partitions {
		if reset {
			break
		}
		old := scan.partitions[i]
		reset = partition.Identity != old.Identity || partition.Generation != old.Generation ||
			partition.Start != old.Start || partition.End < old.End
	}
	if reset {
		metrics.ConsumerCleanupReplayInvalidations.Inc()
		scan.next = make([]uint64, len(partitions))
		scan.cursors = make([]types.ReplayCursor, len(partitions))
		for i, partition := range partitions {
			scan.next[i] = partition.Start
		}
		for _, target := range scan.targets {
			target.evidence = cleanupEvidence{}
		}
	}
	scan.partitions = append(scan.partitions[:0], partitions...)
	records, bytes, pages := topicCleanupRecords, topicCleanupBytes, topicCleanupRecords
	for partition, bounds := range partitions {
		scan.status.Partition, scan.status.Next, scan.status.End = partition, scan.next[partition], bounds.End
		for scan.next[partition] < bounds.End {
			if records == 0 || bytes == 0 || pages == 0 {
				return fmt.Errorf("metadata scan budget exhausted")
			}
			page, err := view.Read(partition, scan.next[partition], scan.cursors[partition], records, bytes)
			pages--
			scan.status.Reads++
			metrics.ConsumerCleanupReplayReads.Inc()
			metrics.ConsumerCleanupReplayRecords.Add(float64(max(0, page.Records)))
			metrics.ConsumerCleanupReplayBytes.Add(float64(max(0, page.Bytes)))
			scan.status.Records += uint64(max(0, page.Records))
			scan.status.Bytes += uint64(max(0, page.Bytes))
			if err != nil {
				return err
			}
			if page.Records < 0 || page.Records > records || page.Bytes < 0 || page.Bytes > bytes ||
				page.Next < scan.next[partition] || page.Next > bounds.End ||
				(page.Next == scan.next[partition] && page.Cursor == scan.cursors[partition]) {
				return fmt.Errorf("invalid bounded metadata page")
			}
			for _, message := range page.Messages {
				if message.Offset < scan.next[partition] || message.Offset >= page.Next {
					return fmt.Errorf("metadata record outside committed page")
				}
				if err := scan.observe(message); err != nil {
					return err
				}
			}
			records -= page.Records
			bytes -= page.Bytes
			scan.next[partition] = page.Next
			scan.cursors[partition] = page.Cursor
			scan.status.Next = page.Next
			scan.status.Segment, scan.status.Position = page.Cursor.Segment, page.Cursor.Position
		}
	}
	return nil
}

func (scan *topicCleanupScan) observe(message types.Message) error {
	var identity struct {
		Group string `json:"group"`
	}
	if err := json.Unmarshal([]byte(message.Payload), &identity); err != nil {
		return fmt.Errorf("malformed consumer metadata")
	}
	target := scan.targets[identity.Group]
	if target == nil {
		// A forged group name must not hide a record under a tracked key.
		for name := range scan.targets {
			key := consumerMetadataRecordKey(ConsumerMetadataRecord{Type: ConsumerMetadataRecordTombstone, Group: name})
			if message.Key == key {
				return fmt.Errorf("consumer metadata key/group mismatch")
			}
		}
		return nil
	}
	scan.status.Group, scan.status.Epoch = identity.Group, target.epoch
	record, versioned, err := decodeConsumerMetadataRecord(message.Payload)
	if err != nil || !versioned || message.Key != consumerMetadataRecordKey(record) {
		return fmt.Errorf("invalid consumer metadata for group %q", identity.Group)
	}
	// Group lifecycle records are nontransactional. A transaction (even an
	// aborted/open one) can never supply deletion authority.
	if message.TransactionalID != "" || message.TransactionState != "" || message.TransactionMarker != "" ||
		message.ControlBatchType != "" || message.ControlBatchVersion != 0 || message.ControlBatchCoordinatorEpoch != 0 ||
		len(message.ControlBatchKey) != 0 || len(message.ControlBatchValue) != 0 {
		return fmt.Errorf("transactional consumer lifecycle evidence for group %q", identity.Group)
	}
	evidence := &target.evidence
	if record.Type != ConsumerMetadataRecordTombstone {
		evidence.liveEpoch = max(evidence.liveEpoch, record.Epoch)
	}
	if record.Type != ConsumerMetadataRecordRegistration && record.Type != ConsumerMetadataRecordTombstone {
		return nil
	}
	if record.Epoch < target.epoch {
		return nil
	}
	if record.Type == ConsumerMetadataRecordTombstone &&
		(record.Version != ConsumerMetadataRecordVersion || record.Topic != scan.status.Topic ||
			len(record.Topics) != 0 || record.TopicPattern != "" || len(record.TopicPartitions) != 0 ||
			record.Lifecycle != nil || len(record.Reservations) != 0 || len(record.ReservationDecisions) != 0) {
		return fmt.Errorf("invalid tombstone scope for group %q", identity.Group)
	}
	// Timestamp is not lifecycle identity (acknowledgement retries may differ).
	record.Timestamp = ConsumerMetadataRecord{}.Timestamp
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode lifecycle identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	if record.Epoch == evidence.epoch && digest != evidence.digest {
		return fmt.Errorf("conflicting lifecycle records for group %q epoch=%d", record.Group, record.Epoch)
	}
	if record.Epoch > evidence.epoch {
		evidence.epoch, evidence.kind, evidence.topic, evidence.digest = record.Epoch, record.Type, record.Topic, digest
	}
	return nil
}
