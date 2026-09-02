package coordinator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

const (
	ConsumerMetadataRecordVersion = 1

	ConsumerMetadataRecordRegistration   = "group_registration"
	ConsumerMetadataRecordOffsetSnapshot = "offset_snapshot"
	ConsumerMetadataRecordTombstone      = "group_tombstone"
)

// TopicOffsetSnapshot is a complete durable next-offset snapshot for one
// group/topic pair.
type TopicOffsetSnapshot struct {
	Topic    string       `json:"topic"`
	Revision uint64       `json:"revision"`
	Offsets  []OffsetItem `json:"offsets"`
}

// ConsumerMetadataRecord is the versioned record stored in
// __consumer_offsets. Lifecycle epochs fence records from deleted or
// re-created groups, while offset revisions make replay independent of the
// physical internal-topic partition order.
type ConsumerMetadataRecord struct {
	Version        int                   `json:"version"`
	Type           string                `json:"type"`
	Group          string                `json:"group"`
	Topic          string                `json:"topic,omitempty"`
	PartitionCount int                   `json:"partition_count,omitempty"`
	Epoch          uint64                `json:"epoch"`
	Revision       uint64                `json:"revision,omitempty"`
	Offsets        []OffsetItem          `json:"offsets,omitempty"`
	InitialOffsets []TopicOffsetSnapshot `json:"initial_offsets,omitempty"`
	Timestamp      time.Time             `json:"timestamp"`
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
	OrphanRecords       int    `json:"orphan_records"`
	CorruptRecords      int    `json:"corrupt_records"`
}

type lifecycleCandidate struct {
	record ConsumerMetadataRecord
}

type offsetCandidate struct {
	record ConsumerMetadataRecord
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
	initial := make([]TopicOffsetSnapshot, len(record.InitialOffsets))
	copy(initial, record.InitialOffsets)
	for i := range initial {
		initial[i].Offsets = canonicalOffsetItems(initial[i].Offsets)
	}
	sort.Slice(initial, func(i, j int) bool { return initial[i].Topic < initial[j].Topic })
	record.InitialOffsets = initial
	return record
}

func canonicalOffsetItems(offsets []OffsetItem) []OffsetItem {
	result := append([]OffsetItem(nil), offsets...)
	sort.Slice(result, func(i, j int) bool { return result[i].Partition < result[j].Partition })
	return result
}

func validateConsumerMetadataRecord(record ConsumerMetadataRecord) error {
	if record.Version != ConsumerMetadataRecordVersion {
		return fmt.Errorf("unsupported consumer metadata record version %d", record.Version)
	}
	if record.Group == "" || record.Epoch == 0 {
		return fmt.Errorf("consumer metadata record is missing group or epoch")
	}
	switch record.Type {
	case ConsumerMetadataRecordRegistration:
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
	case ConsumerMetadataRecordTombstone:
		if record.PartitionCount != 0 || record.Revision != 0 || len(record.Offsets) != 0 || len(record.InitialOffsets) != 0 {
			return fmt.Errorf("group tombstone contains live metadata fields")
		}
	default:
		return fmt.Errorf("unsupported consumer metadata record type %q", record.Type)
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
	if record.Type == ConsumerMetadataRecordOffsetSnapshot {
		identity += "\x00" + record.Topic
		prefix = "offset"
	}
	digest := sha256.Sum256([]byte(identity))
	return "cursus.consumer." + prefix + ".v1." + hex.EncodeToString(digest[:])
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
	if !c.standalone {
		return nil
	}
	payload, key, err := encodeConsumerMetadataRecord(record)
	if err != nil {
		return err
	}
	return c.publishOffsetMessage(&types.Message{Payload: string(payload), Key: key})
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

func (c *Coordinator) writeOffsetSnapshot(groupName, topicName string, epoch, revision uint64, offsets []OffsetItem) error {
	return c.writeConsumerMetadataRecord(ConsumerMetadataRecord{
		Version:   ConsumerMetadataRecordVersion,
		Type:      ConsumerMetadataRecordOffsetSnapshot,
		Group:     groupName,
		Topic:     topicName,
		Epoch:     epoch,
		Revision:  revision,
		Offsets:   canonicalOffsetItems(offsets),
		Timestamp: time.Now().UTC(),
	})
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
	lifecycles := make(map[string]lifecycleCandidate)
	offsetSnapshots := make(map[string]offsetCandidate)

	for partition := 0; partition < c.offsetTopicPartitionCount; partition++ {
		next := uint64(0)
		if provider, ok := c.topicHandler.(offsetLogStartProvider); ok {
			earliest, err := provider.EarliestTopicOffset(c.offsetTopic, partition)
			if err != nil {
				return status, fmt.Errorf("inspect internal metadata log start partition=%d: %w", partition, err)
			}
			if earliest > 0 && !c.migrationAuthoritative {
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

				record, versioned, decodeErr := decodeConsumerMetadataRecord(message.Payload)
				if decodeErr != nil {
					status.CorruptRecords++
					return status, fmt.Errorf("decode internal metadata partition=%d offset=%d: %w", partition, message.Offset, decodeErr)
				}
				if versioned {
					if message.Key != consumerMetadataRecordKey(record) {
						status.CorruptRecords++
						return status, fmt.Errorf("internal metadata key mismatch partition=%d offset=%d", partition, message.Offset)
					}
					switch record.Type {
					case ConsumerMetadataRecordRegistration, ConsumerMetadataRecordTombstone:
						status.RegistrationRecords++
						candidate, exists := lifecycles[record.Group]
						switch {
						case !exists:
							lifecycles[record.Group] = lifecycleCandidate{record: record}
						case record.Epoch > candidate.record.Epoch:
							status.OrphanRecords++
							lifecycles[record.Group] = lifecycleCandidate{record: record}
						case record.Epoch < candidate.record.Epoch:
							status.OrphanRecords++
						case !sameConsumerMetadataRecord(record, candidate.record):
							status.CorruptRecords++
							return status, fmt.Errorf("conflicting lifecycle records group=%s epoch=%d", record.Group, record.Epoch)
						default:
							status.OrphanRecords++
						}
					case ConsumerMetadataRecordOffsetSnapshot:
						status.OffsetRecords++
						identity := offsetCandidateIdentity(record)
						candidate, exists := offsetSnapshots[identity]
						if exists && !sameConsumerMetadataRecord(record, candidate.record) {
							status.CorruptRecords++
							return status, fmt.Errorf("conflicting offset snapshots group=%s topic=%s epoch=%d revision=%d", record.Group, record.Topic, record.Epoch, record.Revision)
						}
						if exists {
							status.OrphanRecords++
						} else {
							offsetSnapshots[identity] = offsetCandidate{record: record}
						}
					}
					continue
				}

				if c.migrationAuthoritative {
					status.OrphanRecords++
					continue
				}
				status.CorruptRecords++
				return status, fmt.Errorf("unversioned internal metadata partition=%d offset=%d; clean bootstrap required", partition, message.Offset)
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

	for _, raw := range c.migrationRecords {
		record := canonicalConsumerMetadataRecord(raw)
		if err := validateConsumerMetadataRecord(record); err != nil {
			status.CorruptRecords++
			return status, fmt.Errorf("invalid selected migration record for group %q: %w", record.Group, err)
		}
		switch record.Type {
		case ConsumerMetadataRecordRegistration, ConsumerMetadataRecordTombstone:
			status.RegistrationRecords++
			candidate, exists := lifecycles[record.Group]
			switch {
			case !exists:
				lifecycles[record.Group] = lifecycleCandidate{record: record}
			case record.Epoch > candidate.record.Epoch:
				status.OrphanRecords++
				lifecycles[record.Group] = lifecycleCandidate{record: record}
			case record.Epoch < candidate.record.Epoch:
				status.OrphanRecords++
			case !sameConsumerMetadataRecord(record, candidate.record):
				status.CorruptRecords++
				return status, fmt.Errorf("migration conflicts with lifecycle record group=%s epoch=%d", record.Group, record.Epoch)
			default:
				status.OrphanRecords++
			}
		case ConsumerMetadataRecordOffsetSnapshot:
			status.OffsetRecords++
			identity := offsetCandidateIdentity(record)
			candidate, exists := offsetSnapshots[identity]
			if exists && !sameConsumerMetadataRecord(record, candidate.record) {
				status.CorruptRecords++
				return status, fmt.Errorf("migration conflicts with offset snapshot group=%s topic=%s", record.Group, record.Topic)
			}
			if exists {
				status.OrphanRecords++
			} else {
				offsetSnapshots[identity] = offsetCandidate{record: record}
			}
		}
	}

	status.Phase = "group_registration_replay"
	groups, groupEpochs, orphanCount, err := materializeConsumerMetadata(lifecycles, offsetSnapshots, &status)
	status.OrphanRecords += orphanCount
	if err != nil {
		status.CorruptRecords++
		return status, err
	}
	c.mu.Lock()
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

func materializeConsumerMetadata(
	lifecycles map[string]lifecycleCandidate,
	offsetSnapshots map[string]offsetCandidate,
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
			Members:           make(map[string]*MemberMetadata),
			Partitions:        partitions,
			Offsets:           make(map[string]map[int]uint64),
			RegistrationEpoch: record.Epoch,
			OffsetRevisions:   make(map[string]uint64),
		}
		for _, snapshot := range record.InitialOffsets {
			group.Offsets[snapshot.Topic] = offsetItemsToMap(snapshot.Offsets)
			group.OffsetRevisions[snapshot.Topic] = snapshot.Revision
		}
		groups[groupName] = group
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
	for _, record := range offsetRecords {
		group := groups[record.Group]
		if group == nil || group.RegistrationEpoch != record.Epoch {
			orphans++
			continue
		}
		if !groupTopicMatches(group.TopicName, record.Topic) {
			return nil, nil, orphans, fmt.Errorf("offset snapshot topic %q does not match group %q topic %q", record.Topic, record.Group, group.TopicName)
		}
		currentRevision := group.OffsetRevisions[record.Topic]
		if record.Revision < currentRevision {
			orphans++
			continue
		}
		nextOffsets := offsetItemsToMap(record.Offsets)
		for partition := range nextOffsets {
			if partition >= len(group.Partitions) {
				return nil, nil, orphans, fmt.Errorf("offset snapshot partition %d exceeds group %q partition count %d", partition, record.Group, len(group.Partitions))
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
