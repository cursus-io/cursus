package coordinator

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func calculateOffsetPartitionCount(groupCount int) int {
	return min(max(groupCount/10, 4), 50)
}

func (c *Coordinator) CommitOffset(groupName, topic string, partition int, offset uint64) error {
	util.Debug("Committing offset: group='%s', topic='%s', partition=%d, offset=%d", groupName, topic, partition, offset)

	c.mu.RLock()
	if c.lifecyclePending[groupName] {
		c.mu.RUnlock()
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}
	gm := c.groups[groupName]
	if gm == nil {
		c.mu.RUnlock()
		return fmt.Errorf("group '%s' not found", groupName)
	}
	gm.mu.Lock()
	c.mu.RUnlock()
	defer gm.mu.Unlock()
	return c.commitOffsetForGroupLocked(gm, groupName, topic, partition, offset)
}

func (c *Coordinator) commitOffsetForGroup(gm *GroupMetadata, groupName, topic string, partition int, offset uint64) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	return c.commitOffsetForGroupLocked(gm, groupName, topic, partition, offset)
}

func (c *Coordinator) commitOffsetForGroupLocked(gm *GroupMetadata, groupName, topic string, partition int, offset uint64) error {
	if err := validateGroupTopicLocked(gm, groupName, topic); err != nil {
		return err
	}

	if partition < 0 || partition >= len(gm.Partitions) {
		return fmt.Errorf("invalid partition %d for group=%s partition_count=%d", partition, groupName, len(gm.Partitions))
	}
	if current, ok := gm.getOffsetSafe(topic, partition); ok && offset < current {
		return fmt.Errorf("offset regression for group=%s topic=%s partition=%d: current=%d attempted=%d", groupName, topic, partition, current, offset)
	}
	if c.standalone {
		if gm.RegistrationEpoch == 0 {
			return fmt.Errorf("group %q requires durable registration before offset commit", groupName)
		}
		if current, ok := gm.getOffsetSafe(topic, partition); ok && offset == current {
			return nil
		}
		items := mergedOffsetSnapshot(gm, topic, []OffsetItem{{Partition: partition, Offset: offset}})
		revision := gm.OffsetRevisions[topic] + 1
		if err := c.writeOffsetSnapshot(groupName, topic, gm.RegistrationEpoch, revision, items); err != nil {
			return err
		}
		gm.storeOffset(topic, partition, offset)
		if gm.OffsetRevisions == nil {
			gm.OffsetRevisions = make(map[string]uint64)
		}
		gm.OffsetRevisions[topic] = revision
		return nil
	}

	offsetMsg := OffsetCommitMessage{
		Group:     groupName,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
	}
	payload, err := json.Marshal(offsetMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal offset commit: %w", err)
	}
	if err := c.publishOffsetMessage(&types.Message{
		ProducerID: "broker",
		Payload:    string(payload),
		Key:        groupName + "-" + topic + "-" + strconv.Itoa(partition),
	}); err != nil {
		return err
	}
	return gm.storeOffsetMonotonic(groupName, topic, partition, offset)
}

func (c *Coordinator) CommitOffsetsBulk(groupName, topic string, offsets []OffsetItem) error {
	if len(offsets) == 0 {
		return nil
	}

	c.mu.RLock()
	if c.lifecyclePending[groupName] {
		c.mu.RUnlock()
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}
	gm := c.groups[groupName]
	if gm == nil {
		c.mu.RUnlock()
		return fmt.Errorf("group '%s' not found", groupName)
	}
	gm.mu.Lock()
	c.mu.RUnlock()
	defer gm.mu.Unlock()
	return c.commitOffsetsBulkForGroupLocked(gm, groupName, topic, offsets)
}

// ValidateAndCommitOffsetsBulk keeps the membership generation stable until the
// durable offset record and all in-memory offsets have been applied.
func (c *Coordinator) ValidateAndCommitOffsetsBulk(groupName, topic, memberID string, generation int, offsets []OffsetItem) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lifecyclePending[groupName] {
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}

	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		return fmt.Errorf("%s", errResp)
	}
	group := c.groups[groupName]
	for _, item := range offsets {
		if !contains(group.Members[memberID].Assignments, item.Partition) {
			return fmt.Errorf("ERROR: NOT_OWNER partition=%d member=%s group=%s generation=%d", item.Partition, memberID, groupName, generation)
		}
	}
	return c.commitOffsetsBulkForGroup(group, groupName, topic, offsets)
}

func (c *Coordinator) commitOffsetsBulkForGroup(gm *GroupMetadata, groupName, topic string, offsets []OffsetItem) error {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	return c.commitOffsetsBulkForGroupLocked(gm, groupName, topic, offsets)
}

func (c *Coordinator) commitOffsetsBulkForGroupLocked(gm *GroupMetadata, groupName, topic string, offsets []OffsetItem) error {
	if err := validateOffsetBatchLocked(gm, groupName, topic, offsets); err != nil {
		return err
	}
	for _, item := range offsets {
		if item.Partition < 0 || item.Partition >= len(gm.Partitions) {
			return fmt.Errorf("invalid partition %d for group=%s partition_count=%d", item.Partition, groupName, len(gm.Partitions))
		}
	}
	if c.standalone {
		if gm.RegistrationEpoch == 0 {
			return fmt.Errorf("group %q requires durable registration before offset commit", groupName)
		}
		changed := false
		for _, item := range offsets {
			current, exists := gm.getOffsetSafe(topic, item.Partition)
			if !exists || current != item.Offset {
				changed = true
				break
			}
		}
		if !changed {
			return nil
		}
		items := mergedOffsetSnapshot(gm, topic, offsets)
		revision := gm.OffsetRevisions[topic] + 1
		if err := c.writeOffsetSnapshot(groupName, topic, gm.RegistrationEpoch, revision, items); err != nil {
			return err
		}
		for _, item := range offsets {
			gm.storeOffset(topic, item.Partition, item.Offset)
		}
		if gm.OffsetRevisions == nil {
			gm.OffsetRevisions = make(map[string]uint64)
		}
		gm.OffsetRevisions[topic] = revision
		return nil
	}

	bulkMsg := BulkOffsetMsg{Group: groupName, Topic: topic, Offsets: offsets, Timestamp: time.Now()}
	payload, err := json.Marshal(bulkMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}
	if err := c.publishOffsetMessage(&types.Message{
		ProducerID: "broker",
		Payload:    string(payload),
		Key:        fmt.Sprintf("%s-%s-bulk", groupName, topic),
	}); err != nil {
		return err
	}
	for _, item := range offsets {
		gm.storeOffset(topic, item.Partition, item.Offset)
	}
	return nil
}

func (c *Coordinator) ApplyOffsetUpdateFromFSM(groupName, topic string, offsets []OffsetItem) error {
	if groupName == "" || topic == "" {
		return fmt.Errorf("invalid group or topic name")
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	// FSM replay may arrive before RegisterGroup, so we need to get-or-create.
	c.mu.Lock()
	gm, ok := c.groups[groupName]
	if !ok {
		gm = &GroupMetadata{
			TopicName: topic,
			Members:   make(map[string]*MemberMetadata),
			Offsets:   make(map[string]map[int]uint64),
		}
		c.groups[groupName] = gm
	} else if gm.TopicName == "" {
		gm.TopicName = topic
		if gm.Members == nil {
			gm.Members = make(map[string]*MemberMetadata)
		}
	}
	c.mu.Unlock()

	gm.mu.Lock()
	defer gm.mu.Unlock()
	if gm.Offsets == nil {
		gm.Offsets = make(map[string]map[int]uint64)
	}
	if err := validateOffsetBatchLocked(gm, groupName, topic, offsets); err != nil {
		return err
	}
	for _, item := range offsets {
		gm.storeOffset(topic, item.Partition, item.Offset)
	}
	return nil
}

// ApplyFencedOffsetUpdateFromFSM validates ownership at metadata-log apply time,
// closing the gap between command handling and the replicated state transition.
func (c *Coordinator) ApplyFencedOffsetUpdateFromFSM(
	groupName, topic, memberID string,
	generation int,
	offsets []OffsetItem,
) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lifecyclePending[groupName] {
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}

	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		return fmt.Errorf("%s", errResp)
	}
	group := c.groups[groupName]
	for _, item := range offsets {
		if !contains(group.Members[memberID].Assignments, item.Partition) {
			return fmt.Errorf("ERROR: NOT_OWNER partition=%d member=%s group=%s generation=%d", item.Partition, memberID, groupName, generation)
		}
	}

	group.mu.Lock()
	defer group.mu.Unlock()
	if err := validateOffsetBatchLocked(group, groupName, topic, offsets); err != nil {
		return err
	}
	for _, item := range offsets {
		group.storeOffset(topic, item.Partition, item.Offset)
	}
	return nil
}

func validateOffsetBatchLocked(group *GroupMetadata, groupName, topic string, offsets []OffsetItem) error {
	if err := validateGroupTopicLocked(group, groupName, topic); err != nil {
		return err
	}
	seen := make(map[int]struct{}, len(offsets))
	for _, item := range offsets {
		if _, exists := seen[item.Partition]; exists {
			return fmt.Errorf("ERROR: duplicate_partition partition=%d group=%s topic=%s", item.Partition, groupName, topic)
		}
		seen[item.Partition] = struct{}{}
		if current, exists := group.getOffsetSafe(topic, item.Partition); exists && item.Offset < current {
			return fmt.Errorf(
				"offset regression for group=%s topic=%s partition=%d: current=%d attempted=%d",
				groupName, topic, item.Partition, current, item.Offset,
			)
		}
	}
	return nil
}

func validateGroupTopicLocked(group *GroupMetadata, groupName, topic string) error {
	if group.TopicName != "" && !groupTopicMatches(group.TopicName, topic) {
		return fmt.Errorf("topic mismatch for group=%s (existing: %s, requested: %s)", groupName, group.TopicName, topic)
	}
	return nil
}

func groupTopicMatches(pattern, topic string) bool {
	if pattern == topic {
		return true
	}
	if !strings.ContainsAny(pattern, "*?") {
		return false
	}
	escaped := regexp.QuoteMeta(pattern)
	expression := strings.ReplaceAll(strings.ReplaceAll(escaped, `\*`, ".*"), `\?`, ".")
	matcher, err := regexp.Compile("^" + expression + "$")
	return err == nil && matcher.MatchString(topic)
}

func mergedOffsetSnapshot(group *GroupMetadata, topic string, updates []OffsetItem) []OffsetItem {
	merged := make(map[int]uint64)
	if current := group.Offsets[topic]; current != nil {
		for partition, offset := range current {
			merged[partition] = offset
		}
	}
	for _, item := range updates {
		merged[item.Partition] = item.Offset
	}
	items := make([]OffsetItem, 0, len(merged))
	for partition, offset := range merged {
		items = append(items, OffsetItem{Partition: partition, Offset: offset})
	}
	return canonicalOffsetItems(items)
}

func (c *Coordinator) publishOffsetMessage(msg *types.Message) error {
	if sp, ok := c.topicHandler.(syncPublisher); ok {
		return sp.PublishWithAck(c.offsetTopic, msg)
	}
	if err := c.topicHandler.Publish(c.offsetTopic, msg); err != nil {
		return err
	}
	if flusher, ok := c.topicHandler.(interface{ Flush() }); ok {
		flusher.Flush()
	}
	return nil
}

func (c *Coordinator) LoadOffsetsFromLog(reader OffsetLogReader) error {
	status, err := c.recoverConsumerMetadata(reader)
	if err != nil {
		c.setRecoveryFailureStatus(status, err)
		return err
	}
	c.markRecoveryComplete(status)
	if status.ReplayedRecords > 0 {
		util.Info("Coordinator: restored groups=%d offsets=%d records=%d legacy=%d orphan=%d from %q", status.RestoredGroups, status.RestoredOffsets, status.ReplayedRecords, status.LegacyRecords, status.OrphanRecords, c.offsetTopic)
	}
	return nil
}

// loadDistributedOffsetsFromLog preserves the cluster coordinator's legacy
// best-effort replay contract. Standalone recovery is intentionally stricter:
// it requires complete lifecycle metadata and rejects retained-prefix gaps.
func (c *Coordinator) loadDistributedOffsetsFromLog(reader OffsetLogReader) (ConsumerMetadataRecoveryStatus, error) {
	const batchSize = 1024
	status := ConsumerMetadataRecoveryStatus{Phase: "committed_offset_replay"}
	var firstParseErr error
	recovered := make(map[string]map[string]map[int]uint64)

	for partition := 0; partition < c.offsetTopicPartitionCount; partition++ {
		next := uint64(0)
		for {
			messages, err := reader.ReadTopicPartition(c.offsetTopic, partition, next, batchSize)
			readOffset := next
			if err != nil {
				util.Warn("Coordinator: error reading distributed offset log partition=%d offset=%d: %v", partition, next, err)
				break
			}
			if len(messages) == 0 {
				break
			}

			for _, message := range messages {
				candidate := message.Offset + 1
				if candidate > next {
					next = candidate
				}
				groupName, topicName, offsets, parseErr := parseOffsetLogPayload(message.Payload)
				if parseErr != nil {
					status.CorruptRecords++
					if firstParseErr == nil {
						firstParseErr = parseErr
					}
					continue
				}
				status.ReplayedRecords++
				status.LegacyRecords++
				if recovered[groupName] == nil {
					recovered[groupName] = make(map[string]map[int]uint64)
				}
				if recovered[groupName][topicName] == nil {
					recovered[groupName][topicName] = make(map[int]uint64)
				}
				for _, item := range offsets {
					current, exists := recovered[groupName][topicName][item.Partition]
					if !exists || item.Offset > current {
						recovered[groupName][topicName][item.Partition] = item.Offset
					}
				}
			}
			if len(messages) < batchSize {
				break
			}
			if next <= readOffset {
				util.Warn("Coordinator: distributed offset log reader made no progress at partition=%d offset=%d", partition, readOffset)
				break
			}
		}
	}

	if status.CorruptRecords > 0 {
		util.Warn("Coordinator: skipped %d invalid distributed offset log records; first error: %v", status.CorruptRecords, firstParseErr)
	}
	groupNames := make([]string, 0, len(recovered))
	for groupName := range recovered {
		groupNames = append(groupNames, groupName)
	}
	sort.Strings(groupNames)
	for _, groupName := range groupNames {
		topics := make([]string, 0, len(recovered[groupName]))
		for topicName := range recovered[groupName] {
			topics = append(topics, topicName)
		}
		sort.Strings(topics)
		for _, topicName := range topics {
			partitionOffsets := recovered[groupName][topicName]
			partitions := make([]int, 0, len(partitionOffsets))
			for partition := range partitionOffsets {
				partitions = append(partitions, partition)
			}
			sort.Ints(partitions)
			offsets := make([]OffsetItem, 0, len(partitions))
			for _, partition := range partitions {
				offsets = append(offsets, OffsetItem{Partition: partition, Offset: partitionOffsets[partition]})
			}
			if err := c.ApplyOffsetUpdateFromFSM(groupName, topicName, offsets); err != nil {
				return status, fmt.Errorf("restore distributed group=%s topic=%s offsets: %w", groupName, topicName, err)
			}
			status.RestoredOffsets += len(offsets)
		}
	}
	status.RestoredGroups = len(groupNames)
	if status.ReplayedRecords > 0 {
		util.Info("Coordinator: loaded %d distributed committed offset records from %q", status.ReplayedRecords, c.offsetTopic)
	}
	return status, nil
}

func parseOffsetLogPayload(payload string) (string, string, []OffsetItem, error) {
	var bulk BulkOffsetMsg
	if err := json.Unmarshal([]byte(payload), &bulk); err == nil && bulk.Group != "" && bulk.Topic != "" && len(bulk.Offsets) > 0 {
		return bulk.Group, bulk.Topic, bulk.Offsets, nil
	}

	var single OffsetCommitMessage
	if err := json.Unmarshal([]byte(payload), &single); err != nil {
		return "", "", nil, err
	}
	if single.Group == "" || single.Topic == "" {
		return "", "", nil, fmt.Errorf("missing group or topic")
	}
	return single.Group, single.Topic, []OffsetItem{{
		Partition: single.Partition,
		Offset:    single.Offset,
	}}, nil
}

func (c *Coordinator) GetOffset(groupName, topic string, partition int) (uint64, bool) {
	gm := c.getGroupSafe(groupName)
	if gm == nil {
		return 0, false
	}

	gm.mu.RLock()
	defer gm.mu.RUnlock()
	return gm.getOffsetSafe(topic, partition)
}

// updateOffsetPartitionCount updates the number of partitions for the internal offset topic.
func (c *Coordinator) updateOffsetPartitionCount() {
	c.mu.RLock()
	groupCount := len(c.groups)
	currentCount := c.offsetTopicPartitionCount
	c.mu.RUnlock()

	newCount := calculateOffsetPartitionCount(groupCount)
	if newCount == currentCount {
		return
	}

	c.mu.Lock()
	c.offsetTopicPartitionCount = newCount
	topicName := c.offsetTopic
	c.mu.Unlock()

	go func() {
		if err := c.topicHandler.CreateTopic(topicName, newCount, false, false); err != nil {
			util.Error("Failed to scale offset topic '%s' to %d partitions: %v", topicName, newCount, err)
			return
		}
		util.Info("Offset topic '%s' partitions scaled to %d", topicName, newCount)
	}()
}

func (c *Coordinator) ValidateAndCommit(groupName, topic string, partition int, offset uint64, generation int, memberID string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lifecyclePending[groupName] {
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}
	group := c.groups[groupName]
	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		return fmt.Errorf("%s", errResp)
	}
	if !contains(group.Members[memberID].Assignments, partition) {
		return fmt.Errorf("ERROR: NOT_OWNER partition=%d member=%s group=%s generation=%d", partition, memberID, groupName, generation)
	}
	return c.commitOffsetForGroup(group, groupName, topic, partition, offset)
}

func (c *Coordinator) getGroupUnsafe(name string) *GroupMetadata {
	return c.groups[name]
}

func (c *Coordinator) ValidateOwnershipAtomic(groupName, memberID string, generation int, partition int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group := c.getGroupUnsafe(groupName)
	if group == nil {
		util.Debug("failed to validate ownership for partition %d: Group '%s' not found.", partition, groupName)
		return false
	}

	member := group.Members[memberID]
	if member == nil {
		util.Debug("failed to validate ownership for partition %d: Member '%s' not found in group '%s'.", partition, memberID, groupName)
		return false
	}

	if group.Generation != generation {
		util.Debug("failed to validate ownership  for partition %d: Generation mismatch. Group Gen: %d, Request Gen: %d.", partition, group.Generation, generation)
		return false
	}

	isAssigned := false
	for _, assigned := range member.Assignments {
		if assigned == partition {
			isAssigned = true
			break
		}
	}

	if !isAssigned {
		util.Debug("failed to validate ownership for partition %d: Partition not assigned to member '%s'. Assignments: %v", partition, memberID, member.Assignments)
		return false
	}

	return true
}
