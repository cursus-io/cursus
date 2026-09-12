package coordinator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

const (
	ConsumerMetadataRecordVersion              = 1
	ConsumerMetadataRecordVersionSubscriptions = 2
	ConsumerMetadataRecordVersionTransactions  = 3
	ConsumerMetadataRecordVersionLifecycle     = 4

	ConsumerMetadataRecordRegistration                = "group_registration"
	ConsumerMetadataRecordOffsetSnapshot              = "offset_snapshot"
	ConsumerMetadataRecordTransactionalOffsetSnapshot = "transactional_offset_snapshot"
	ConsumerMetadataRecordTombstone                   = "group_tombstone"
	ConsumerMetadataRecordLifecycleSnapshot           = "group_lifecycle_snapshot"
)

// TopicOffsetSnapshot is a complete durable next-offset snapshot for one
// group/topic pair. A registration can carry legacy offsets while upgrading
// an older log that did not persist group lifecycle metadata.
type TopicOffsetSnapshot struct {
	Topic    string       `json:"topic"`
	Revision uint64       `json:"revision"`
	Offsets  []OffsetItem `json:"offsets"`
}

// GroupLifecycleMember is the durable, assignment-bearing part of a member.
// Leases are deliberately not persisted: a newly elected partition leader
// starts a fresh session window before it can expire a recovered member.
type GroupLifecycleMember struct {
	ID               string           `json:"id"`
	Assignments      []int            `json:"assignments,omitempty"`
	TopicAssignments []TopicPartition `json:"topic_assignments,omitempty"`
}

// GroupLifecycleSnapshot is the authoritative group state written to the
// group's __consumer_offsets partition. It intentionally excludes committed
// offsets: those retain their own compacted keys and monotonic revisions.
type GroupLifecycleSnapshot struct {
	TopicName       string                 `json:"topic,omitempty"`
	Topics          []string               `json:"topics,omitempty"`
	TopicPattern    string                 `json:"topic_pattern,omitempty"`
	TopicPartitions []TopicPartition       `json:"topic_partitions,omitempty"`
	Generation      int                    `json:"generation"`
	Members         []GroupLifecycleMember `json:"members"`
	Partitions      []int                  `json:"partitions,omitempty"`
	LastActivity    time.Time              `json:"last_activity,omitempty"`
	LastRebalance   time.Time              `json:"last_rebalance,omitempty"`
}

// ConsumerMetadataRecord is the versioned record stored in
// __consumer_offsets. Lifecycle epochs fence records from deleted or
// re-created groups, while offset revisions make replay independent of the
// physical internal-topic partition order.
type ConsumerMetadataRecord struct {
	Version          int                     `json:"version"`
	Type             string                  `json:"type"`
	Group            string                  `json:"group"`
	Topic            string                  `json:"topic,omitempty"`
	PartitionCount   int                     `json:"partition_count,omitempty"`
	Topics           []string                `json:"topics,omitempty"`
	TopicPattern     string                  `json:"topic_pattern,omitempty"`
	TopicPartitions  []TopicPartition        `json:"topic_partitions,omitempty"`
	Epoch            uint64                  `json:"epoch"`
	Revision         uint64                  `json:"revision,omitempty"`
	Offsets          []OffsetItem            `json:"offsets,omitempty"`
	InitialOffsets   []TopicOffsetSnapshot   `json:"initial_offsets,omitempty"`
	Timestamp        time.Time               `json:"timestamp"`
	TransactionalID  string                  `json:"transactional_id,omitempty"`
	ProducerID       string                  `json:"producer_id,omitempty"`
	ProducerEpoch    int64                   `json:"producer_epoch,omitempty"`
	CoordinatorEpoch int64                   `json:"coordinator_epoch,omitempty"`
	Lifecycle        *GroupLifecycleSnapshot `json:"lifecycle,omitempty"`
}

// ConsumerMetadataRecoveryStatus is safe to expose through readiness and
// metrics. Failure is intentionally retained after startup so a corrupt log
// cannot look like a healthy empty coordinator.
type ConsumerMetadataRecoveryStatus struct {
	Phase               string `json:"phase"`
	Ready               bool   `json:"ready"`
	Failure             string `json:"failure,omitempty"`
	RestoredGroups      int    `json:"restored_groups"`
	RestoredOffsets     int    `json:"restored_offsets"`
	ReplayedRecords     int    `json:"replayed_records"`
	RegistrationRecords int    `json:"registration_records"`
	OffsetRecords       int    `json:"offset_records"`
	LegacyRecords       int    `json:"legacy_records"`
	OrphanRecords       int    `json:"orphan_records"`
	CorruptRecords      int    `json:"corrupt_records"`
}

type lifecycleCandidate struct {
	record ConsumerMetadataRecord
}

type lifecycleSnapshotCandidate struct {
	record ConsumerMetadataRecord
}

type offsetCandidate struct {
	record ConsumerMetadataRecord
}

type legacyRecoveryState map[string]map[string]map[int]uint64

type consumerMetadataCandidates struct {
	lifecycles         map[string]lifecycleCandidate
	lifecycleSnapshots map[string]lifecycleSnapshotCandidate
	offsetSnapshots    map[string]offsetCandidate
}

func newConsumerMetadataCandidates() consumerMetadataCandidates {
	return consumerMetadataCandidates{
		lifecycles:         make(map[string]lifecycleCandidate),
		lifecycleSnapshots: make(map[string]lifecycleSnapshotCandidate),
		offsetSnapshots:    make(map[string]offsetCandidate),
	}
}

func (candidates *consumerMetadataCandidates) selectRecord(record ConsumerMetadataRecord, status *ConsumerMetadataRecoveryStatus) error {
	switch record.Type {
	case ConsumerMetadataRecordRegistration, ConsumerMetadataRecordTombstone:
		status.RegistrationRecords++
		candidate, exists := candidates.lifecycles[record.Group]
		switch {
		case !exists:
			candidates.lifecycles[record.Group] = lifecycleCandidate{record: record}
		case record.Epoch > candidate.record.Epoch:
			status.OrphanRecords++
			candidates.lifecycles[record.Group] = lifecycleCandidate{record: record}
		case record.Epoch < candidate.record.Epoch:
			status.OrphanRecords++
		case !sameConsumerMetadataRecord(record, candidate.record):
			status.CorruptRecords++
			return fmt.Errorf("conflicting lifecycle records group=%s epoch=%d", record.Group, record.Epoch)
		default:
			status.OrphanRecords++
		}
	case ConsumerMetadataRecordLifecycleSnapshot:
		if err := selectLifecycleSnapshot(candidates.lifecycleSnapshots, record, status); err != nil {
			return err
		}
	case ConsumerMetadataRecordOffsetSnapshot:
		status.OffsetRecords++
		identity := offsetCandidateIdentity(record)
		candidate, exists := candidates.offsetSnapshots[identity]
		if exists && !sameConsumerMetadataRecord(record, candidate.record) {
			status.CorruptRecords++
			return fmt.Errorf("conflicting offset snapshots group=%s topic=%s epoch=%d revision=%d", record.Group, record.Topic, record.Epoch, record.Revision)
		}
		if exists {
			status.OrphanRecords++
		} else {
			candidates.offsetSnapshots[identity] = offsetCandidate{record: record}
		}
	case ConsumerMetadataRecordTransactionalOffsetSnapshot:
		// Visibility is decided by the transaction state store. A committed
		// transaction is subsequently materialized as an ordinary snapshot.
		status.OffsetRecords++
	}
	return nil
}

func (c *Coordinator) RecoverySnapshot() ConsumerMetadataRecoveryStatus {
	if c == nil {
		return ConsumerMetadataRecoveryStatus{Phase: "unavailable", Failure: "coordinator unavailable"}
	}
	c.recoveryMu.RLock()
	defer c.recoveryMu.RUnlock()
	return c.recovery
}

func (c *Coordinator) RecoveryReadinessError() error {
	status := c.RecoverySnapshot()
	if status.Ready && status.Failure == "" {
		return nil
	}
	if status.Failure != "" {
		return errors.New(status.Failure)
	}
	return fmt.Errorf("consumer metadata recovery phase=%s", status.Phase)
}

func (c *Coordinator) markRecoveryComplete(status ConsumerMetadataRecoveryStatus) {
	status.Phase = "ready"
	status.Ready = true
	status.Failure = ""
	c.recoveryMu.Lock()
	c.recovery = status
	c.recoveryMu.Unlock()
}

func (c *Coordinator) setRecoveryFailure(err error) {
	status := c.RecoverySnapshot()
	c.setRecoveryFailureStatus(status, err)
}

func (c *Coordinator) setRecoveryFailureStatus(status ConsumerMetadataRecoveryStatus, err error) {
	status.Ready = false
	if status.Phase == "" || status.Phase == "ready" {
		status.Phase = "failed"
	}
	if err != nil {
		status.Failure = err.Error()
	}
	c.recoveryMu.Lock()
	c.recovery = status
	c.recoveryMu.Unlock()
}

func canonicalConsumerMetadataRecord(record ConsumerMetadataRecord) ConsumerMetadataRecord {
	record.Timestamp = record.Timestamp.UTC()
	record.Offsets = canonicalOffsetItems(record.Offsets)
	record.Topics = append([]string(nil), record.Topics...)
	sort.Strings(record.Topics)
	record.TopicPartitions = append([]TopicPartition(nil), record.TopicPartitions...)
	sort.Slice(record.TopicPartitions, func(i, j int) bool {
		if record.TopicPartitions[i].Topic != record.TopicPartitions[j].Topic {
			return record.TopicPartitions[i].Topic < record.TopicPartitions[j].Topic
		}
		return record.TopicPartitions[i].Partition < record.TopicPartitions[j].Partition
	})
	initial := make([]TopicOffsetSnapshot, len(record.InitialOffsets))
	copy(initial, record.InitialOffsets)
	for i := range initial {
		initial[i].Offsets = canonicalOffsetItems(initial[i].Offsets)
	}
	sort.Slice(initial, func(i, j int) bool { return initial[i].Topic < initial[j].Topic })
	record.InitialOffsets = initial
	if record.Lifecycle != nil {
		lifecycle := *record.Lifecycle
		lifecycle.LastActivity = lifecycle.LastActivity.UTC()
		lifecycle.LastRebalance = lifecycle.LastRebalance.UTC()
		lifecycle.Topics = append([]string(nil), lifecycle.Topics...)
		sort.Strings(lifecycle.Topics)
		lifecycle.TopicPartitions = append([]TopicPartition(nil), lifecycle.TopicPartitions...)
		sort.Slice(lifecycle.TopicPartitions, func(i, j int) bool {
			if lifecycle.TopicPartitions[i].Topic != lifecycle.TopicPartitions[j].Topic {
				return lifecycle.TopicPartitions[i].Topic < lifecycle.TopicPartitions[j].Topic
			}
			return lifecycle.TopicPartitions[i].Partition < lifecycle.TopicPartitions[j].Partition
		})
		lifecycle.Partitions = append([]int(nil), lifecycle.Partitions...)
		sort.Ints(lifecycle.Partitions)
		lifecycle.Members = append([]GroupLifecycleMember(nil), lifecycle.Members...)
		for i := range lifecycle.Members {
			lifecycle.Members[i].Assignments = append([]int(nil), lifecycle.Members[i].Assignments...)
			sort.Ints(lifecycle.Members[i].Assignments)
			lifecycle.Members[i].TopicAssignments = append([]TopicPartition(nil), lifecycle.Members[i].TopicAssignments...)
			sort.Slice(lifecycle.Members[i].TopicAssignments, func(a, b int) bool {
				left, right := lifecycle.Members[i].TopicAssignments[a], lifecycle.Members[i].TopicAssignments[b]
				if left.Topic != right.Topic {
					return left.Topic < right.Topic
				}
				return left.Partition < right.Partition
			})
		}
		sort.Slice(lifecycle.Members, func(i, j int) bool { return lifecycle.Members[i].ID < lifecycle.Members[j].ID })
		record.Lifecycle = &lifecycle
	}
	return record
}

func canonicalOffsetItems(offsets []OffsetItem) []OffsetItem {
	result := append([]OffsetItem(nil), offsets...)
	sort.Slice(result, func(i, j int) bool { return result[i].Partition < result[j].Partition })
	return result
}

func validateConsumerMetadataRecord(record ConsumerMetadataRecord) error {
	if record.Version != ConsumerMetadataRecordVersion && record.Version != ConsumerMetadataRecordVersionSubscriptions && record.Version != ConsumerMetadataRecordVersionTransactions && record.Version != ConsumerMetadataRecordVersionLifecycle {
		return fmt.Errorf("unsupported consumer metadata record version %d", record.Version)
	}
	if record.Group == "" || record.Epoch == 0 {
		return fmt.Errorf("consumer metadata record is missing group or epoch")
	}
	if record.Version == ConsumerMetadataRecordVersionTransactions && record.Type != ConsumerMetadataRecordTransactionalOffsetSnapshot {
		return fmt.Errorf("consumer metadata version 3 requires a transactional offset snapshot")
	}
	if record.Version == ConsumerMetadataRecordVersionLifecycle && record.Type != ConsumerMetadataRecordLifecycleSnapshot {
		return fmt.Errorf("consumer metadata version 4 requires a lifecycle snapshot")
	}
	switch record.Type {
	case ConsumerMetadataRecordRegistration:
		if hasConsumerMetadataTransactionFields(record) {
			return fmt.Errorf("group registration contains transaction fields")
		}
		if record.Version == ConsumerMetadataRecordVersionSubscriptions {
			return validateSubscriptionRegistration(record)
		}
		if record.Topic == "" || record.PartitionCount <= 0 {
			return fmt.Errorf("group registration is missing topic or partition count")
		}
		if record.Revision != 0 || len(record.Offsets) != 0 {
			return fmt.Errorf("group registration contains offset-record fields")
		}
		seenTopics := make(map[string]struct{}, len(record.InitialOffsets))
		for _, snapshot := range record.InitialOffsets {
			if snapshot.Topic == "" {
				return fmt.Errorf("group registration contains an empty offset topic")
			}
			if !groupTopicMatches(record.Topic, snapshot.Topic) {
				return fmt.Errorf("group registration offset topic %q does not match group topic %q", snapshot.Topic, record.Topic)
			}
			if _, exists := seenTopics[snapshot.Topic]; exists {
				return fmt.Errorf("group registration contains duplicate offset topic %q", snapshot.Topic)
			}
			seenTopics[snapshot.Topic] = struct{}{}
			if err := validateOffsetItems(snapshot.Offsets, record.PartitionCount); err != nil {
				return fmt.Errorf("group registration topic %q: %w", snapshot.Topic, err)
			}
		}
	case ConsumerMetadataRecordOffsetSnapshot:
		if record.Topic == "" || record.Revision == 0 || len(record.Offsets) == 0 {
			return fmt.Errorf("offset snapshot is missing topic, revision, or offsets")
		}
		if record.PartitionCount != 0 || len(record.InitialOffsets) != 0 {
			return fmt.Errorf("offset snapshot contains registration fields")
		}
		if err := validateOffsetItems(record.Offsets, 0); err != nil {
			return err
		}
		if record.TransactionalID != "" || record.ProducerID != "" || record.ProducerEpoch != 0 || record.CoordinatorEpoch != 0 {
			return fmt.Errorf("ordinary offset snapshot contains transaction fields")
		}
	case ConsumerMetadataRecordTransactionalOffsetSnapshot:
		if record.Version != ConsumerMetadataRecordVersionTransactions || record.Topic == "" || record.Revision == 0 || len(record.Offsets) == 0 {
			return fmt.Errorf("transactional offset snapshot is missing version, topic, revision, or offsets")
		}
		if record.TransactionalID == "" || record.ProducerID == "" || record.CoordinatorEpoch < 0 {
			return fmt.Errorf("transactional offset snapshot is missing transaction identity")
		}
		if record.PartitionCount != 0 || len(record.InitialOffsets) != 0 {
			return fmt.Errorf("transactional offset snapshot contains registration fields")
		}
		if err := validateOffsetItems(record.Offsets, 0); err != nil {
			return err
		}
	case ConsumerMetadataRecordTombstone:
		if hasConsumerMetadataTransactionFields(record) {
			return fmt.Errorf("group tombstone contains transaction fields")
		}
		if record.PartitionCount != 0 || record.Revision != 0 || len(record.Offsets) != 0 || len(record.InitialOffsets) != 0 {
			return fmt.Errorf("group tombstone contains live metadata fields")
		}
	case ConsumerMetadataRecordLifecycleSnapshot:
		if record.Version != ConsumerMetadataRecordVersionLifecycle || record.Lifecycle == nil {
			return fmt.Errorf("group lifecycle snapshot is missing version or state")
		}
		if record.Topic != "" || record.PartitionCount != 0 || len(record.Offsets) != 0 || len(record.InitialOffsets) != 0 || hasConsumerMetadataTransactionFields(record) {
			return fmt.Errorf("group lifecycle snapshot contains non-lifecycle fields")
		}
		if err := validateGroupLifecycleSnapshot(*record.Lifecycle); err != nil {
			return err
		}
		generation := record.Lifecycle.Generation
		if generation < 0 || record.Revision != uint64(generation) {
			return fmt.Errorf("group lifecycle snapshot revision does not match generation")
		}
	default:
		return fmt.Errorf("unsupported consumer metadata record type %q", record.Type)
	}
	return nil
}

func validateGroupLifecycleSnapshot(snapshot GroupLifecycleSnapshot) error {
	if snapshot.Generation < 0 || (snapshot.TopicName == "" && len(snapshot.Topics) == 0) {
		return fmt.Errorf("group lifecycle snapshot is missing subscription or has invalid generation")
	}
	seenMembers := make(map[string]struct{}, len(snapshot.Members))
	for _, member := range snapshot.Members {
		if member.ID == "" {
			return fmt.Errorf("group lifecycle snapshot contains empty member")
		}
		if _, exists := seenMembers[member.ID]; exists {
			return fmt.Errorf("group lifecycle snapshot contains duplicate member %q", member.ID)
		}
		seenMembers[member.ID] = struct{}{}
	}
	return nil
}

func hasConsumerMetadataTransactionFields(record ConsumerMetadataRecord) bool {
	return record.TransactionalID != "" || record.ProducerID != "" || record.ProducerEpoch != 0 || record.CoordinatorEpoch != 0
}

func validateSubscriptionRegistration(record ConsumerMetadataRecord) error {
	if len(record.Topics) == 0 || len(record.TopicPartitions) == 0 {
		return fmt.Errorf("group subscription registration is missing topics or partitions")
	}
	if record.PartitionCount != 0 || record.Revision != 0 || len(record.Offsets) != 0 {
		return fmt.Errorf("group subscription registration contains legacy or offset-record fields")
	}
	allowed := make(map[string]struct{}, len(record.Topics))
	for _, topicName := range record.Topics {
		if topicName == "" {
			return fmt.Errorf("group subscription registration contains an empty topic")
		}
		if _, exists := allowed[topicName]; exists {
			return fmt.Errorf("group subscription registration contains duplicate topic %q", topicName)
		}
		allowed[topicName] = struct{}{}
	}
	seen := make(map[TopicPartition]struct{}, len(record.TopicPartitions))
	for _, tp := range record.TopicPartitions {
		if _, ok := allowed[tp.Topic]; !ok || tp.Partition < 0 {
			return fmt.Errorf("invalid subscription partition topic=%q partition=%d", tp.Topic, tp.Partition)
		}
		if _, exists := seen[tp]; exists {
			return fmt.Errorf("duplicate subscription partition topic=%q partition=%d", tp.Topic, tp.Partition)
		}
		seen[tp] = struct{}{}
	}
	for _, snapshot := range record.InitialOffsets {
		if _, ok := allowed[snapshot.Topic]; !ok {
			return fmt.Errorf("group subscription offset topic %q is not subscribed", snapshot.Topic)
		}
		if err := validateOffsetItems(snapshot.Offsets, topicPartitionCount(record.TopicPartitions, snapshot.Topic)); err != nil {
			return fmt.Errorf("group subscription topic %q: %w", snapshot.Topic, err)
		}
	}
	return nil
}

func validateOffsetItems(offsets []OffsetItem, partitionCount int) error {
	seen := make(map[int]struct{}, len(offsets))
	for _, item := range offsets {
		if item.Partition < 0 {
			return fmt.Errorf("negative offset partition %d", item.Partition)
		}
		if partitionCount > 0 && item.Partition >= partitionCount {
			return fmt.Errorf("offset partition %d exceeds registration partition count %d", item.Partition, partitionCount)
		}
		if _, exists := seen[item.Partition]; exists {
			return fmt.Errorf("duplicate offset partition %d", item.Partition)
		}
		seen[item.Partition] = struct{}{}
	}
	return nil
}

func consumerMetadataRecordKey(record ConsumerMetadataRecord) string {
	identity := record.Group
	prefix := "group"
	if record.Type == ConsumerMetadataRecordLifecycleSnapshot {
		prefix = "lifecycle"
	}
	if record.Type == ConsumerMetadataRecordOffsetSnapshot || record.Type == ConsumerMetadataRecordTransactionalOffsetSnapshot {
		identity += "\x00" + record.Topic
		prefix = "offset"
	}
	if record.Type == ConsumerMetadataRecordTransactionalOffsetSnapshot {
		identity += "\x00" + record.TransactionalID + "\x00" + fmt.Sprint(record.ProducerEpoch)
		prefix = "txn-offset"
	}
	digest := sha256.Sum256([]byte(identity))
	return "cursus.consumer." + prefix + ".v1." + hex.EncodeToString(digest[:])
}

// ConsumerMetadataGroupPartitionKey is used only for selecting the internal
// topic partition. The record key remains record-specific so compaction never
// deletes a group's lifecycle record in favor of an offset snapshot.
func ConsumerMetadataGroupPartitionKey(group string) string {
	return "cursus.consumer.partition.v1." + group
}

func encodeConsumerMetadataRecord(record ConsumerMetadataRecord) ([]byte, string, error) {
	record = canonicalConsumerMetadataRecord(record)
	if err := validateConsumerMetadataRecord(record); err != nil {
		return nil, "", err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, "", fmt.Errorf("marshal consumer metadata record: %w", err)
	}
	return payload, consumerMetadataRecordKey(record), nil
}

func decodeConsumerMetadataRecord(payload string) (ConsumerMetadataRecord, bool, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &object); err != nil {
		return ConsumerMetadataRecord{}, false, err
	}
	versionRaw, versioned := object["version"]
	if !versioned {
		return ConsumerMetadataRecord{}, false, nil
	}
	var version int
	if err := json.Unmarshal(versionRaw, &version); err != nil {
		return ConsumerMetadataRecord{}, true, fmt.Errorf("decode consumer metadata version: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewBufferString(payload))
	decoder.DisallowUnknownFields()
	var record ConsumerMetadataRecord
	if err := decoder.Decode(&record); err != nil {
		return ConsumerMetadataRecord{}, true, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ConsumerMetadataRecord{}, true, fmt.Errorf("consumer metadata record has trailing content")
	}
	record = canonicalConsumerMetadataRecord(record)
	if version != record.Version {
		return ConsumerMetadataRecord{}, true, fmt.Errorf("consumer metadata version mismatch")
	}
	if err := validateConsumerMetadataRecord(record); err != nil {
		return ConsumerMetadataRecord{}, true, err
	}
	return record, true, nil
}

// DecodeConsumerMetadataRecord is a read-only decoder used by the storage
// maintenance CLI. The bool is false for a valid legacy offset payload.
func DecodeConsumerMetadataRecord(payload string) (ConsumerMetadataRecord, bool, error) {
	return decodeConsumerMetadataRecord(payload)
}

// EncodeConsumerMetadataRecord is used by the broker's routed internal-topic
// writer. It preserves the same strict validation as recovery.
func EncodeConsumerMetadataRecord(record ConsumerMetadataRecord) ([]byte, string, error) {
	return encodeConsumerMetadataRecord(record)
}

// DecodeLegacyOffsetPayload decodes the pre-v1 single and bulk offset JSON
// formats without changing coordinator state.
func DecodeLegacyOffsetPayload(payload string) (string, string, []OffsetItem, error) {
	return parseOffsetLogPayload(payload)
}

func sameConsumerMetadataRecord(left, right ConsumerMetadataRecord) bool {
	left.Timestamp = time.Time{}
	right.Timestamp = time.Time{}
	left = canonicalConsumerMetadataRecord(left)
	right = canonicalConsumerMetadataRecord(right)
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return bytes.Equal(leftJSON, rightJSON)
}

func (c *Coordinator) writeConsumerMetadataRecord(record ConsumerMetadataRecord) error {
	c.offsetRecordWriterMu.RLock()
	writer := c.offsetRecordWriter
	c.offsetRecordWriterMu.RUnlock()
	if writer != nil {
		return writer(record)
	}
	if !c.standalone {
		return nil
	}
	payload, key, err := encodeConsumerMetadataRecord(record)
	if err != nil {
		return err
	}
	return c.publishOffsetMessage(&types.Message{Payload: string(payload), Key: key})
}

func lifecycleSnapshot(group *GroupMetadata) *GroupLifecycleSnapshot {
	if group == nil {
		return nil
	}
	snapshot := &GroupLifecycleSnapshot{
		TopicName:       group.TopicName,
		Topics:          append([]string(nil), group.Topics...),
		TopicPattern:    group.TopicPattern,
		TopicPartitions: append([]TopicPartition(nil), group.TopicPartitions...),
		Generation:      group.Generation,
		Members:         make([]GroupLifecycleMember, 0, len(group.Members)),
		Partitions:      append([]int(nil), group.Partitions...),
		LastActivity:    group.LastActivity,
		LastRebalance:   group.LastRebalance,
	}
	for _, member := range group.Members {
		snapshot.Members = append(snapshot.Members, GroupLifecycleMember{
			ID:               member.ID,
			Assignments:      append([]int(nil), member.Assignments...),
			TopicAssignments: append([]TopicPartition(nil), member.TopicAssignments...),
		})
	}
	return snapshot
}

func (c *Coordinator) writeGroupLifecycleSnapshot(groupName string, epoch uint64, group *GroupMetadata) error {
	if group == nil || group.Generation < 0 {
		return fmt.Errorf("group lifecycle snapshot has invalid generation")
	}
	return c.writeConsumerMetadataRecord(ConsumerMetadataRecord{
		Version:   ConsumerMetadataRecordVersionLifecycle,
		Type:      ConsumerMetadataRecordLifecycleSnapshot,
		Group:     groupName,
		Epoch:     epoch,
		Revision:  uint64(group.Generation),
		Lifecycle: lifecycleSnapshot(group),
		Timestamp: time.Now().UTC(),
	})
}

func registrationInitialOffsets(group *GroupMetadata) []TopicOffsetSnapshot {
	topics := make([]string, 0, len(group.Offsets))
	for topicName := range group.Offsets {
		topics = append(topics, topicName)
	}
	sort.Strings(topics)
	result := make([]TopicOffsetSnapshot, 0, len(topics))
	for _, topicName := range topics {
		partitions := group.Offsets[topicName]
		items := make([]OffsetItem, 0, len(partitions))
		for partition, offset := range partitions {
			items = append(items, OffsetItem{Partition: partition, Offset: offset})
		}
		result = append(result, TopicOffsetSnapshot{
			Topic:    topicName,
			Revision: group.OffsetRevisions[topicName],
			Offsets:  canonicalOffsetItems(items),
		})
	}
	return result
}

func (c *Coordinator) writeGroupRegistration(groupName, topicName string, partitionCount int, epoch uint64, initial []TopicOffsetSnapshot) error {
	return c.writeConsumerMetadataRecord(ConsumerMetadataRecord{
		Version:        ConsumerMetadataRecordVersion,
		Type:           ConsumerMetadataRecordRegistration,
		Group:          groupName,
		Topic:          topicName,
		PartitionCount: partitionCount,
		Epoch:          epoch,
		InitialOffsets: initial,
		Timestamp:      time.Now().UTC(),
	})
}

func (c *Coordinator) writeGroupSubscriptionRegistration(groupName string, topics []string, pattern string, partitionCounts map[string]int, epoch uint64, initial []TopicOffsetSnapshot) error {
	partitions := make([]TopicPartition, 0)
	for _, topicName := range topics {
		for partition := 0; partition < partitionCounts[topicName]; partition++ {
			partitions = append(partitions, TopicPartition{Topic: topicName, Partition: partition})
		}
	}
	return c.writeConsumerMetadataRecord(ConsumerMetadataRecord{
		Version:         ConsumerMetadataRecordVersionSubscriptions,
		Type:            ConsumerMetadataRecordRegistration,
		Group:           groupName,
		Topics:          append([]string(nil), topics...),
		TopicPattern:    pattern,
		TopicPartitions: partitions,
		Epoch:           epoch,
		InitialOffsets:  initial,
		Timestamp:       time.Now().UTC(),
	})
}

func (c *Coordinator) writeOffsetSnapshot(groupName, topicName string, epoch, revision uint64, offsets []OffsetItem) error {
	record := ConsumerMetadataRecord{
		Version:   ConsumerMetadataRecordVersion,
		Type:      ConsumerMetadataRecordOffsetSnapshot,
		Group:     groupName,
		Topic:     topicName,
		Epoch:     epoch,
		Revision:  revision,
		Offsets:   canonicalOffsetItems(offsets),
		Timestamp: time.Now().UTC(),
	}
	return c.writeConsumerMetadataRecord(record)
}

func (c *Coordinator) writeGroupTombstone(groupName, topicName string, epoch uint64) error {
	return c.writeConsumerMetadataRecord(ConsumerMetadataRecord{
		Version:   ConsumerMetadataRecordVersion,
		Type:      ConsumerMetadataRecordTombstone,
		Group:     groupName,
		Topic:     topicName,
		Epoch:     epoch,
		Timestamp: time.Now().UTC(),
	})
}

func (c *Coordinator) recoverConsumerMetadata(reader OffsetLogReader) (ConsumerMetadataRecoveryStatus, error) {
	const batchSize = 1024
	status := ConsumerMetadataRecoveryStatus{Phase: "consumer_metadata_scan"}
	candidates := newConsumerMetadataCandidates()
	legacy := make(legacyRecoveryState)
	legacyRecordCounts := make(map[string]int)

	for partition := 0; partition < c.offsetTopicPartitionCount; partition++ {
		next := uint64(0)
		if provider, ok := c.topicHandler.(offsetLogStartProvider); ok {
			earliest, err := provider.EarliestTopicOffset(c.offsetTopic, partition)
			if err != nil {
				return status, fmt.Errorf("inspect internal metadata log start partition=%d: %w", partition, err)
			}
			if earliest > 0 && !c.migrationAuthoritative && c.standalone {
				status.OrphanRecords++
				return status, fmt.Errorf("internal metadata partition %d starts at offset %d; explicit migration selection is required", partition, earliest)
			}
			next = earliest
		}
		for {
			messages, err := reader.ReadTopicPartition(c.offsetTopic, partition, next, batchSize)
			if err != nil {
				status.CorruptRecords++
				return status, fmt.Errorf("read internal metadata partition=%d offset=%d: %w", partition, next, err)
			}
			if len(messages) == 0 {
				break
			}
			previous := next
			for _, message := range messages {
				if message.Offset < previous {
					status.CorruptRecords++
					return status, fmt.Errorf("internal metadata partition %d returned offset %d before %d", partition, message.Offset, previous)
				}
				previous = message.Offset + 1
				status.ReplayedRecords++
				if message.TransactionMarker != types.TransactionMarkerNone {
					continue
				}

				record, versioned, decodeErr := decodeConsumerMetadataRecord(message.Payload)
				if decodeErr != nil {
					status.CorruptRecords++
					if !c.standalone {
						continue
					}
					return status, fmt.Errorf("decode internal metadata partition=%d offset=%d: %w", partition, message.Offset, decodeErr)
				}
				if versioned {
					if message.Key != consumerMetadataRecordKey(record) {
						status.CorruptRecords++
						if !c.standalone {
							continue
						}
						return status, fmt.Errorf("internal metadata key mismatch partition=%d offset=%d", partition, message.Offset)
					}
					if err := candidates.selectRecord(record, &status); err != nil {
						return status, fmt.Errorf("select internal metadata partition=%d offset=%d: %w", partition, message.Offset, err)
					}
					continue
				}

				groupName, topicName, offsets, parseErr := parseOffsetLogPayload(message.Payload)
				if parseErr != nil {
					status.CorruptRecords++
					if !c.standalone {
						continue
					}
					return status, fmt.Errorf("decode legacy offset partition=%d offset=%d: %w", partition, message.Offset, parseErr)
				}
				status.LegacyRecords++
				legacyRecordCounts[groupName]++
				if legacy[groupName] == nil {
					legacy[groupName] = make(map[string]map[int]uint64)
				}
				if legacy[groupName][topicName] == nil {
					legacy[groupName][topicName] = make(map[int]uint64)
				}
				for _, item := range offsets {
					if item.Partition < 0 {
						status.CorruptRecords++
						if !c.standalone {
							continue
						}
						return status, fmt.Errorf("legacy offset contains negative partition %d", item.Partition)
					}
					current, exists := legacy[groupName][topicName][item.Partition]
					if !exists || item.Offset > current {
						legacy[groupName][topicName][item.Partition] = item.Offset
					}
				}
			}
			if previous <= next {
				status.CorruptRecords++
				return status, fmt.Errorf("internal metadata reader made no progress partition=%d offset=%d", partition, next)
			}
			next = previous
			if len(messages) < batchSize {
				break
			}
		}
	}

	if c.migrationAuthoritative {
		for _, count := range legacyRecordCounts {
			status.OrphanRecords += count
		}
		legacy = make(legacyRecoveryState)
		legacyRecordCounts = make(map[string]int)
	}
	for _, raw := range c.migrationRecords {
		record := canonicalConsumerMetadataRecord(raw)
		if err := validateConsumerMetadataRecord(record); err != nil {
			status.CorruptRecords++
			return status, fmt.Errorf("invalid selected migration record for group %q: %w", record.Group, err)
		}
		if err := candidates.selectRecord(record, &status); err != nil {
			return status, fmt.Errorf("select migration record for group %q: %w", record.Group, err)
		}
	}

	status.Phase = "group_registration_replay"
	groups, groupEpochs, orphanCount, err := materializeConsumerMetadata(candidates.lifecycles, candidates.lifecycleSnapshots, candidates.offsetSnapshots, legacy, legacyRecordCounts, &status)
	status.OrphanRecords += orphanCount
	if err != nil {
		status.CorruptRecords++
		return status, err
	}
	c.mu.Lock()
	// origin/main placed membership in GROUP_SYNC. Its offsets records have no
	// v4 lifecycle snapshot, so retain that restored FSM membership while
	// replacing only the committed-offset view. Once a v4 snapshot exists it
	// is authoritative and this compatibility path is deliberately bypassed.
	for groupName, existing := range c.groups {
		recovered := groups[groupName]
		if recovered == nil || existing == nil {
			continue
		}
		if snapshot, ok := candidates.lifecycleSnapshots[groupName]; ok && snapshot.record.Epoch == recovered.RegistrationEpoch {
			continue
		}
		existing.mu.RLock()
		recovered.Generation = existing.Generation
		recovered.LastRebalance = existing.LastRebalance
		recovered.Members = make(map[string]*MemberMetadata, len(existing.Members))
		for memberID, member := range existing.Members {
			recovered.Members[memberID] = &MemberMetadata{ID: member.ID, LastHeartbeat: member.LastHeartbeat, Assignments: append([]int(nil), member.Assignments...), TopicAssignments: append([]TopicPartition(nil), member.TopicAssignments...)}
		}
		existing.mu.RUnlock()
	}
	c.groups = groups
	c.groupEpochs = groupEpochs
	c.ownershipSince = make(map[string]time.Time)
	c.mu.Unlock()

	status.RestoredGroups = len(groups)
	for _, group := range groups {
		for _, offsets := range group.Offsets {
			status.RestoredOffsets += len(offsets)
		}
	}
	status.Phase = "committed_offset_replay"
	return status, nil
}

func selectLifecycleSnapshot(candidates map[string]lifecycleSnapshotCandidate, record ConsumerMetadataRecord, status *ConsumerMetadataRecoveryStatus) error {
	candidate, exists := candidates[record.Group]
	switch {
	case !exists || record.Epoch > candidate.record.Epoch || (record.Epoch == candidate.record.Epoch && record.Revision > candidate.record.Revision):
		if exists && status != nil {
			status.OrphanRecords++
		}
		candidates[record.Group] = lifecycleSnapshotCandidate{record: record}
	case record.Epoch == candidate.record.Epoch && record.Revision == candidate.record.Revision && !sameConsumerMetadataRecord(record, candidate.record):
		if status != nil {
			status.CorruptRecords++
		}
		return fmt.Errorf("conflicting lifecycle snapshots group=%s epoch=%d generation=%d", record.Group, record.Epoch, record.Revision)
	default:
		if status != nil {
			status.OrphanRecords++
		}
	}
	return nil
}

func materializeConsumerMetadata(
	lifecycles map[string]lifecycleCandidate,
	lifecycleSnapshots map[string]lifecycleSnapshotCandidate,
	offsetSnapshots map[string]offsetCandidate,
	legacy legacyRecoveryState,
	legacyRecordCounts map[string]int,
	status *ConsumerMetadataRecoveryStatus,
) (map[string]*GroupMetadata, map[string]uint64, int, error) {
	groups := make(map[string]*GroupMetadata)
	groupEpochs := make(map[string]uint64)
	orphans := 0

	lifecycleNames := make([]string, 0, len(lifecycles))
	for groupName := range lifecycles {
		lifecycleNames = append(lifecycleNames, groupName)
	}
	sort.Strings(lifecycleNames)
	for _, groupName := range lifecycleNames {
		record := lifecycles[groupName].record
		groupEpochs[groupName] = record.Epoch
		if record.Type == ConsumerMetadataRecordTombstone {
			continue
		}
		partitions := make([]int, record.PartitionCount)
		for partition := range partitions {
			partitions[partition] = partition
		}
		group := &GroupMetadata{
			TopicName:         record.Topic,
			Topics:            append([]string(nil), record.Topics...),
			TopicPattern:      record.TopicPattern,
			TopicPartitions:   append([]TopicPartition(nil), record.TopicPartitions...),
			Members:           make(map[string]*MemberMetadata),
			Partitions:        partitions,
			Offsets:           make(map[string]map[int]uint64),
			RegistrationEpoch: record.Epoch,
			OffsetRevisions:   make(map[string]uint64),
		}
		if record.Version == ConsumerMetadataRecordVersionSubscriptions {
			group.TopicName = subscriptionDisplayName(record.Topics, record.TopicPattern)
		}
		for _, snapshot := range record.InitialOffsets {
			group.Offsets[snapshot.Topic] = offsetItemsToMap(snapshot.Offsets)
			group.OffsetRevisions[snapshot.Topic] = snapshot.Revision
		}
		groups[groupName] = group
	}
	for groupName, candidate := range lifecycleSnapshots {
		group := groups[groupName]
		if group == nil || group.RegistrationEpoch != candidate.record.Epoch {
			orphans++
			continue
		}
		lifecycle := candidate.record.Lifecycle
		if lifecycle == nil || lifecycle.TopicName != group.TopicName || !slices.Equal(lifecycle.Topics, group.Topics) || lifecycle.TopicPattern != group.TopicPattern || !slices.Equal(lifecycle.TopicPartitions, group.TopicPartitions) || !slices.Equal(lifecycle.Partitions, group.Partitions) {
			return nil, nil, orphans, fmt.Errorf("lifecycle snapshot does not match registration group=%s", groupName)
		}
		group.Generation = lifecycle.Generation
		group.LastActivity = lifecycle.LastActivity
		if group.LastActivity.IsZero() {
			// Version-4 snapshots written before last_activity was added still
			// carry the timestamp of the same lifecycle transition.
			group.LastActivity = lifecycle.LastRebalance
		}
		if group.LastActivity.IsZero() {
			group.LastActivity = candidate.record.Timestamp
		}
		group.LastRebalance = lifecycle.LastRebalance
		group.Members = make(map[string]*MemberMetadata, len(lifecycle.Members))
		for _, member := range lifecycle.Members {
			group.Members[member.ID] = &MemberMetadata{ID: member.ID, LastHeartbeat: time.Now(), Assignments: append([]int(nil), member.Assignments...), TopicAssignments: append([]TopicPartition(nil), member.TopicAssignments...)}
		}
	}

	versionedGroups := make(map[string]struct{})
	for _, candidate := range offsetSnapshots {
		versionedGroups[candidate.record.Group] = struct{}{}
	}
	legacyNames := make([]string, 0, len(legacy))
	for groupName := range legacy {
		legacyNames = append(legacyNames, groupName)
	}
	sort.Strings(legacyNames)
	for _, groupName := range legacyNames {
		if _, hasLifecycle := lifecycles[groupName]; hasLifecycle {
			orphans += legacyRecordCounts[groupName]
			continue
		}
		if _, hasVersionedSnapshot := versionedGroups[groupName]; hasVersionedSnapshot {
			// Versioned snapshots carry a durable registration epoch. Let the
			// inferred-group path below materialize the highest epoch instead of
			// pinning recovery to an epoch-zero legacy shell.
			orphans += legacyRecordCounts[groupName]
			continue
		}
		topics := make([]string, 0, len(legacy[groupName]))
		for topicName := range legacy[groupName] {
			topics = append(topics, topicName)
		}
		sort.Strings(topics)
		if len(topics) != 1 {
			return nil, nil, orphans, fmt.Errorf("legacy group %q has offsets for %d topics and no durable registration", groupName, len(topics))
		}
		topicName := topics[0]
		partitionCount := 0
		for partition := range legacy[groupName][topicName] {
			if partition+1 > partitionCount {
				partitionCount = partition + 1
			}
		}
		partitions := make([]int, partitionCount)
		for partition := range partitions {
			partitions[partition] = partition
		}
		groups[groupName] = &GroupMetadata{
			TopicName:       topicName,
			Members:         make(map[string]*MemberMetadata),
			Partitions:      partitions,
			Offsets:         map[string]map[int]uint64{topicName: clonePartitionOffsets(legacy[groupName][topicName])},
			OffsetRevisions: make(map[string]uint64),
		}
	}

	if status != nil {
		status.Phase = "committed_offset_replay"
	}
	offsetRecords := make([]ConsumerMetadataRecord, 0, len(offsetSnapshots))
	for _, candidate := range offsetSnapshots {
		offsetRecords = append(offsetRecords, candidate.record)
	}
	sort.Slice(offsetRecords, func(i, j int) bool {
		left, right := offsetRecords[i], offsetRecords[j]
		if left.Group != right.Group {
			return left.Group < right.Group
		}
		if left.Topic != right.Topic {
			return left.Topic < right.Topic
		}
		if left.Epoch != right.Epoch {
			return left.Epoch < right.Epoch
		}
		return left.Revision < right.Revision
	})
	// origin/main distributed clusters persisted group membership in Raft and
	// may therefore contain offset snapshots without a registration record.
	// Materialize a compatibility shell so those offsets remain readable until
	// the group is next registered through the v4 lifecycle path.
	type inferredGroup struct {
		epoch  uint64
		topics map[string]int
	}
	inferred := make(map[string]*inferredGroup)
	for _, record := range offsetRecords {
		if groups[record.Group] != nil {
			continue
		}
		if fencedEpoch, exists := groupEpochs[record.Group]; exists && fencedEpoch >= record.Epoch {
			continue
		}
		entry := inferred[record.Group]
		if entry == nil || record.Epoch > entry.epoch {
			entry = &inferredGroup{epoch: record.Epoch, topics: make(map[string]int)}
			inferred[record.Group] = entry
		}
		if record.Epoch != entry.epoch {
			continue
		}
		for _, item := range record.Offsets {
			if item.Partition+1 > entry.topics[record.Topic] {
				entry.topics[record.Topic] = item.Partition + 1
			}
		}
	}
	for groupName, entry := range inferred {
		topics := make([]string, 0, len(entry.topics))
		for topicName := range entry.topics {
			topics = append(topics, topicName)
		}
		sort.Strings(topics)
		partitions := make([]TopicPartition, 0)
		for _, topicName := range topics {
			for partition := 0; partition < entry.topics[topicName]; partition++ {
				partitions = append(partitions, TopicPartition{Topic: topicName, Partition: partition})
			}
		}
		group := &GroupMetadata{Topics: topics, TopicPartitions: partitions, Members: make(map[string]*MemberMetadata), Offsets: make(map[string]map[int]uint64), RegistrationEpoch: entry.epoch, OffsetRevisions: make(map[string]uint64)}
		if len(topics) == 1 {
			group.TopicName = topics[0]
			group.Partitions = makePartitions(entry.topics[topics[0]])
		} else {
			group.TopicName = subscriptionDisplayName(topics, "")
		}
		groups[groupName] = group
		groupEpochs[groupName] = entry.epoch
	}
	for _, record := range offsetRecords {
		group := groups[record.Group]
		if group == nil || group.RegistrationEpoch != record.Epoch {
			orphans++
			continue
		}
		if !groupAcceptsTopic(group, record.Topic) {
			return nil, nil, orphans, fmt.Errorf("offset snapshot topic %q does not match group %q topic %q", record.Topic, record.Group, group.TopicName)
		}
		currentRevision := group.OffsetRevisions[record.Topic]
		if record.Revision < currentRevision {
			orphans++
			continue
		}
		nextOffsets := offsetItemsToMap(record.Offsets)
		partitionCount := groupTopicPartitionCount(group, record.Topic)
		for partition := range nextOffsets {
			if partition >= partitionCount {
				return nil, nil, orphans, fmt.Errorf("offset snapshot partition %d exceeds group %q topic %q partition count %d", partition, record.Group, record.Topic, partitionCount)
			}
		}
		currentOffsets := group.Offsets[record.Topic]
		if record.Revision == currentRevision && currentRevision != 0 {
			if !samePartitionOffsets(currentOffsets, nextOffsets) {
				return nil, nil, orphans, fmt.Errorf("conflicting offset snapshots during replay group=%s topic=%s revision=%d", record.Group, record.Topic, record.Revision)
			}
			orphans++
			continue
		}
		for partition, current := range currentOffsets {
			recovered, exists := nextOffsets[partition]
			if !exists {
				return nil, nil, orphans, fmt.Errorf("offset snapshot dropped committed key during replay group=%s topic=%s partition=%d", record.Group, record.Topic, partition)
			}
			if recovered < current {
				return nil, nil, orphans, fmt.Errorf("offset regression during replay group=%s topic=%s partition=%d current=%d recovered=%d", record.Group, record.Topic, partition, current, recovered)
			}
		}
		group.Offsets[record.Topic] = nextOffsets
		group.OffsetRevisions[record.Topic] = record.Revision
	}
	return groups, groupEpochs, orphans, nil
}

func offsetCandidateIdentity(record ConsumerMetadataRecord) string {
	return fmt.Sprintf("%s\x00%s\x00%020d\x00%020d", record.Group, record.Topic, record.Epoch, record.Revision)
}

func samePartitionOffsets(left, right map[int]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for partition, offset := range left {
		if candidate, exists := right[partition]; !exists || candidate != offset {
			return false
		}
	}
	return true
}

func offsetItemsToMap(items []OffsetItem) map[int]uint64 {
	result := make(map[int]uint64, len(items))
	for _, item := range items {
		result[item.Partition] = item.Offset
	}
	return result
}

func clonePartitionOffsets(source map[int]uint64) map[int]uint64 {
	result := make(map[int]uint64, len(source))
	for partition, offset := range source {
		result[partition] = offset
	}
	return result
}
