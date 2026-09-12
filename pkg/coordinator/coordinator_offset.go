package coordinator

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func calculateOffsetPartitionCount(groupCount int) int {
	return min(max(groupCount/10, 4), 50)
}

func (c *Coordinator) authoritativeOffsetWritesEnabled() bool {
	c.offsetRecordWriterMu.RLock()
	writerConfigured := c.offsetRecordWriter != nil
	c.offsetRecordWriterMu.RUnlock()
	return c.standalone || writerConfigured
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
	if c.standalone {
		// Standalone mode never rebuilds c.groups from a replicated log, so keep
		// the historic non-blocking publish behavior for local durable writes.
		c.mu.RUnlock()
		defer gm.mu.Unlock()
		return c.commitOffsetForGroupLocked(gm, groupName, topic, partition, offset)
	}
	// Keep the group map read lock through the durable write. A reload replaces
	// c.groups wholesale; releasing this lock after taking only the old group
	// mutex would allow an acknowledged commit to update a detached object.
	defer c.mu.RUnlock()
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

	partitionCount := groupTopicPartitionCount(gm, topic)
	if partition < 0 || partition >= partitionCount {
		return fmt.Errorf("invalid partition %d for group=%s topic=%s partition_count=%d", partition, groupName, topic, partitionCount)
	}
	if current, ok := gm.getOffsetSafe(topic, partition); ok && offset < current {
		return fmt.Errorf("offset regression for group=%s topic=%s partition=%d: current=%d attempted=%d", groupName, topic, partition, current, offset)
	}
	if c.authoritativeOffsetWritesEnabled() {
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
	// Legacy FSM replay reaches this path without installing a writer.
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
	if c.standalone {
		// See CommitOffset: standalone mode has no distributed-map reload.
		c.mu.RUnlock()
		defer gm.mu.Unlock()
		return c.commitOffsetsBulkForGroupLocked(gm, groupName, topic, offsets)
	}
	// See CommitOffset: reload must not swap the group map while this durable
	// bulk append is still committing into the selected group object.
	defer c.mu.RUnlock()
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
		if !memberOwnsTopicPartition(group, group.Members[memberID], topic, item.Partition) {
			return fmt.Errorf("ERROR: NOT_OWNER topic=%s partition=%d member=%s group=%s generation=%d", topic, item.Partition, memberID, groupName, generation)
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
	partitionCount := groupTopicPartitionCount(gm, topic)
	for _, item := range offsets {
		if item.Partition < 0 || item.Partition >= partitionCount {
			return fmt.Errorf("invalid partition %d for group=%s topic=%s partition_count=%d", item.Partition, groupName, topic, partitionCount)
		}
	}
	if c.authoritativeOffsetWritesEnabled() {
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
	// Legacy FSM replay reaches this path without installing a writer.
	for _, item := range offsets {
		gm.storeOffset(topic, item.Partition, item.Offset)
	}
	return nil
}

func memberOwnsTopicPartition(group *GroupMetadata, member *MemberMetadata, topic string, partition int) bool {
	for _, assignment := range member.TopicAssignments {
		if assignment.Topic == topic && assignment.Partition == partition {
			return true
		}
	}
	return len(member.TopicAssignments) == 0 && groupTopicMatches(group.TopicName, topic) && contains(member.Assignments, partition)
}

// ValidateAndCommitTopicOffsetsBulk validates and applies a multi-topic offset
// set under one group membership fence. All new writes append complete
// snapshots to __consumer_offsets before exposing the in-memory update.
func (c *Coordinator) ValidateAndCommitTopicOffsetsBulk(groupName, memberID string, generation int, offsets map[string][]OffsetItem) error {
	return c.ValidateAndCommitTopicOffsetsBulkForEpoch(groupName, memberID, generation, 0, offsets)
}

func (c *Coordinator) ValidateAndCommitTopicOffsetsBulkForEpoch(groupName, memberID string, generation int, registrationEpoch uint64, offsets map[string][]OffsetItem) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lifecyclePending[groupName] {
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}
	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		return fmt.Errorf("%s", errResp)
	}
	group := c.groups[groupName]
	if registrationEpoch != 0 && group.RegistrationEpoch != registrationEpoch {
		return fmt.Errorf("ERROR: group_epoch_mismatch group=%s expected=%d actual=%d", groupName, registrationEpoch, group.RegistrationEpoch)
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	topics := make([]string, 0, len(offsets))
	for topic := range offsets {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	for _, topic := range topics {
		if err := validateOffsetBatchLocked(group, groupName, topic, offsets[topic]); err != nil {
			return err
		}
		count := groupTopicPartitionCount(group, topic)
		for _, item := range offsets[topic] {
			if item.Partition < 0 || item.Partition >= count {
				return fmt.Errorf("invalid partition %d for group=%s topic=%s partition_count=%d", item.Partition, groupName, topic, count)
			}
			if !memberOwnsTopicPartition(group, group.Members[memberID], topic, item.Partition) {
				return fmt.Errorf("ERROR: NOT_OWNER topic=%s partition=%d member=%s group=%s generation=%d", topic, item.Partition, memberID, groupName, generation)
			}
		}
	}
	durableWrites := c.authoritativeOffsetWritesEnabled()
	if durableWrites {
		if group.RegistrationEpoch == 0 {
			return fmt.Errorf("group %q requires durable registration before offset commit", groupName)
		}
		for _, topic := range topics {
			items := mergedOffsetSnapshot(group, topic, offsets[topic])
			if err := c.writeOffsetSnapshot(groupName, topic, group.RegistrationEpoch, group.OffsetRevisions[topic]+1, items); err != nil {
				return err
			}
			for _, item := range offsets[topic] {
				group.storeOffset(topic, item.Partition, item.Offset)
			}
			group.OffsetRevisions[topic]++
		}
		return nil
	}
	for _, topic := range topics {
		for _, item := range offsets[topic] {
			group.storeOffset(topic, item.Partition, item.Offset)
		}
	}
	return nil
}

// MaterializeCommittedTransactionOffsets advances the durable ordinary
// snapshot after the transaction decision is committed. During this short
// materialization window GetOffset is covered by TransactionalOffsetResolver.
func (c *Coordinator) MaterializeCommittedTransactionOffsets(groupName string, registrationEpoch uint64, offsets map[string][]OffsetItem) error {
	if len(offsets) == 0 {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]
	if group == nil {
		return fmt.Errorf("group %q not found", groupName)
	}
	if group.RegistrationEpoch != registrationEpoch {
		return fmt.Errorf("ERROR: group_epoch_mismatch group=%s expected=%d actual=%d", groupName, registrationEpoch, group.RegistrationEpoch)
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	topics := make([]string, 0, len(offsets))
	for topic := range offsets {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	for _, topic := range topics {
		if err := validateOffsetBatchLocked(group, groupName, topic, offsets[topic]); err != nil {
			return err
		}
		items := mergedOffsetSnapshot(group, topic, offsets[topic])
		if err := c.writeOffsetSnapshot(groupName, topic, registrationEpoch, group.OffsetRevisions[topic]+1, items); err != nil {
			return err
		}
	}
	for _, topic := range topics {
		for _, item := range offsets[topic] {
			group.storeOffset(topic, item.Partition, item.Offset)
		}
		group.OffsetRevisions[topic]++
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

// ApplyFencedTopicOffsetUpdateFromFSM is the replay-only compatibility path
// for historical BATCH_OFFSET records. It never emits a new offset record.
func (c *Coordinator) ApplyFencedTopicOffsetUpdateFromFSM(groupName, memberID string, generation int, registrationEpoch uint64, offsets map[string][]OffsetItem) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if errResp := c.validateMemberGenerationLocked(groupName, memberID, generation); errResp != "" {
		return fmt.Errorf("%s", errResp)
	}
	group := c.groups[groupName]
	if registrationEpoch != 0 && group.RegistrationEpoch != registrationEpoch {
		return fmt.Errorf("ERROR: group_epoch_mismatch group=%s expected=%d actual=%d", groupName, registrationEpoch, group.RegistrationEpoch)
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	for topic, items := range offsets {
		if err := validateOffsetBatchLocked(group, groupName, topic, items); err != nil {
			return err
		}
		for _, item := range items {
			if !memberOwnsTopicPartition(group, group.Members[memberID], topic, item.Partition) {
				return fmt.Errorf("ERROR: NOT_OWNER topic=%s partition=%d member=%s group=%s generation=%d", topic, item.Partition, memberID, groupName, generation)
			}
		}
	}
	for topic, items := range offsets {
		for _, item := range items {
			group.storeOffset(topic, item.Partition, item.Offset)
		}
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
	if !groupAcceptsTopic(group, topic) {
		return fmt.Errorf("topic mismatch for group=%s (existing: %s, requested: %s)", groupName, group.TopicName, topic)
	}
	return nil
}

func groupAcceptsTopic(group *GroupMetadata, topic string) bool {
	if group == nil {
		return false
	}
	if len(group.Topics) > 0 {
		for _, subscribed := range group.Topics {
			if subscribed == topic {
				return true
			}
		}
		return false
	}
	return group.TopicName == "" || groupTopicMatches(group.TopicName, topic)
}

func topicPartitionCount(partitions []TopicPartition, topic string) int {
	count := 0
	for _, tp := range partitions {
		if tp.Topic == topic && tp.Partition+1 > count {
			count = tp.Partition + 1
		}
	}
	return count
}

func groupTopicPartitionCount(group *GroupMetadata, topic string) int {
	if len(group.TopicPartitions) > 0 {
		return topicPartitionCount(group.TopicPartitions, topic)
	}
	return len(group.Partitions)
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

// loadDistributedOffsetsFromLog restores authoritative versioned snapshots and
// group lifecycle snapshots. All of those records share the group-selected
// __consumer_offsets partition, so a partition leader can rebuild its local
// fencing state without consulting controller Raft.
func (c *Coordinator) loadDistributedOffsetsFromLog(reader OffsetLogReader) (ConsumerMetadataRecoveryStatus, error) {
	return c.recoverConsumerMetadata(reader)
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
	base, baseOK := gm.getOffsetSafe(topic, partition)
	epoch := gm.RegistrationEpoch
	gm.mu.RUnlock()
	c.mu.RLock()
	resolver := c.transactionalOffsets
	c.mu.RUnlock()
	if resolver != nil {
		if committed, ok := resolver.CommittedOffset(groupName, topic, partition, epoch); ok && (!baseOK || committed > base) {
			return committed, true
		}
	}
	return base, baseOK
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
