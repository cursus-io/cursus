package coordinator

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/cursus-io/cursus/util"
)

// RegisterGroup creates a new consumer group for a topic. Standalone success
// means the versioned registration record has been synchronously persisted.
func (c *Coordinator) RegisterGroup(topicName, groupName string, partitionCount int) error {
	if topicName == "" || groupName == "" {
		return fmt.Errorf("topic and group names must not be empty")
	}
	if partitionCount <= 0 {
		return fmt.Errorf("invalid partition count: %d", partitionCount)
	}

	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	now := time.Now()

	c.mu.Lock()
	existing := c.groups[groupName]
	if existing != nil {
		existing.mu.Lock()
		if existing.TopicName != "" && existing.TopicName != topicName {
			existing.mu.Unlock()
			c.mu.Unlock()
			return fmt.Errorf("topic mismatch (existing: %s, requested: %s)", existing.TopicName, topicName)
		}
		if len(existing.Partitions) != 0 && len(existing.Partitions) != partitionCount {
			current := len(existing.Partitions)
			existing.mu.Unlock()
			c.mu.Unlock()
			return fmt.Errorf("partition count mismatch (existing: %d, requested: %d)", current, partitionCount)
		}
		if existing.Members == nil {
			existing.Members = make(map[string]*MemberMetadata)
		}
		if existing.Offsets == nil {
			existing.Offsets = make(map[string]map[int]uint64)
		}
		if existing.OffsetRevisions == nil {
			existing.OffsetRevisions = make(map[string]uint64)
		}
		if existing.RegistrationEpoch != 0 {
			if existing.RegistrationEpoch > c.groupEpochs[groupName] {
				c.groupEpochs[groupName] = existing.RegistrationEpoch
			}
			existing.TopicName = topicName
			existing.LastActivity = now
			if len(existing.Partitions) == 0 {
				existing.Partitions = makePartitions(partitionCount)
			}
			existing.mu.Unlock()
			c.mu.Unlock()
			return nil
		}

		epoch := c.groupEpochs[groupName] + 1
		if epoch == 0 {
			existing.mu.Unlock()
			c.mu.Unlock()
			return fmt.Errorf("group lifecycle epoch overflow")
		}
		initial := registrationInitialOffsets(existing)
		for _, snapshot := range initial {
			if !groupTopicMatches(topicName, snapshot.Topic) {
				existing.mu.Unlock()
				c.mu.Unlock()
				return fmt.Errorf("group %q has legacy offsets for mismatched topic %q", groupName, snapshot.Topic)
			}
		}
		if c.lifecyclePending == nil {
			c.lifecyclePending = make(map[string]bool)
		}
		c.lifecyclePending[groupName] = true
		existing.mu.Unlock()
		c.mu.Unlock()

		err := c.writeGroupRegistration(groupName, topicName, partitionCount, epoch, initial)

		c.mu.Lock()
		delete(c.lifecyclePending, groupName)
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("persist group registration: %w", err)
		}
		if c.groups[groupName] != existing {
			c.mu.Unlock()
			return fmt.Errorf("group %q changed during durable registration", groupName)
		}
		existing.mu.Lock()
		existing.RegistrationEpoch = epoch
		existing.TopicName = topicName
		existing.LastActivity = now
		if len(existing.Partitions) == 0 {
			existing.Partitions = makePartitions(partitionCount)
		}
		existing.mu.Unlock()
		c.groupEpochs[groupName] = epoch
		c.mu.Unlock()
		return nil
	}

	epoch := c.groupEpochs[groupName] + 1
	if epoch == 0 {
		c.mu.Unlock()
		return fmt.Errorf("group lifecycle epoch overflow")
	}
	group := &GroupMetadata{
		TopicName:         topicName,
		Members:           make(map[string]*MemberMetadata),
		Partitions:        makePartitions(partitionCount),
		Offsets:           make(map[string]map[int]uint64),
		LastActivity:      now,
		RegistrationEpoch: epoch,
		OffsetRevisions:   make(map[string]uint64),
	}
	if c.lifecyclePending == nil {
		c.lifecyclePending = make(map[string]bool)
	}
	c.lifecyclePending[groupName] = true
	c.mu.Unlock()

	err := c.writeGroupRegistration(groupName, topicName, partitionCount, epoch, nil)

	c.mu.Lock()
	delete(c.lifecyclePending, groupName)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("persist group registration: %w", err)
	}
	if c.groups[groupName] != nil {
		c.mu.Unlock()
		return fmt.Errorf("group %q changed during durable registration", groupName)
	}
	c.groups[groupName] = group
	c.groupEpochs[groupName] = epoch
	c.mu.Unlock()

	c.updateOffsetPartitionCount()
	util.Info("🆕 Group '%s' registered for topic '%s' (%d partitions)", groupName, topicName, partitionCount)
	return nil
}

// RegisterGroupSubscription durably registers a multi-topic or pattern-backed
// group. The supplied topics are the concrete expansion used for assignment.
func (c *Coordinator) RegisterGroupSubscription(groupName string, topics []string, pattern string, partitionCounts map[string]int) error {
	canonicalTopics, topicPartitions, err := canonicalSubscription(topics, pattern, partitionCounts)
	if err != nil {
		return err
	}

	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	existing := c.groups[groupName]
	if existing != nil {
		if !sameSubscription(existing, canonicalTopics, pattern, topicPartitions) {
			c.mu.Unlock()
			return fmt.Errorf("subscription mismatch for group %q", groupName)
		}
		if existing.RegistrationEpoch != 0 {
			c.mu.Unlock()
			return nil
		}
	}
	epoch := c.groupEpochs[groupName] + 1
	if epoch == 0 {
		c.mu.Unlock()
		return fmt.Errorf("group lifecycle epoch overflow")
	}
	initial := []TopicOffsetSnapshot(nil)
	if existing != nil {
		initial = registrationInitialOffsets(existing)
	}
	if c.lifecyclePending == nil {
		c.lifecyclePending = make(map[string]bool)
	}
	c.lifecyclePending[groupName] = true
	c.mu.Unlock()

	err = c.writeGroupSubscriptionRegistration(groupName, canonicalTopics, pattern, partitionCounts, epoch, initial)

	c.mu.Lock()
	delete(c.lifecyclePending, groupName)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("persist group subscription: %w", err)
	}
	if existing == nil {
		existing = &GroupMetadata{
			Members:         make(map[string]*MemberMetadata),
			Offsets:         make(map[string]map[int]uint64),
			OffsetRevisions: make(map[string]uint64),
		}
		c.groups[groupName] = existing
	}
	existing.TopicName = subscriptionDisplayName(canonicalTopics, pattern)
	existing.Topics = append([]string(nil), canonicalTopics...)
	existing.TopicPattern = pattern
	existing.TopicPartitions = append([]TopicPartition(nil), topicPartitions...)
	existing.RegistrationEpoch = epoch
	existing.LastActivity = time.Now()
	c.groupEpochs[groupName] = epoch
	c.mu.Unlock()
	c.updateOffsetPartitionCount()
	return nil
}

func canonicalSubscription(topics []string, pattern string, partitionCounts map[string]int) ([]string, []TopicPartition, error) {
	if len(topics) == 0 {
		return nil, nil, fmt.Errorf("subscription must resolve at least one topic")
	}
	canonical := append([]string(nil), topics...)
	sort.Strings(canonical)
	unique := canonical[:0]
	for _, topicName := range canonical {
		if topicName == "" {
			return nil, nil, fmt.Errorf("subscription contains empty topic")
		}
		if len(unique) > 0 && unique[len(unique)-1] == topicName {
			return nil, nil, fmt.Errorf("duplicate subscription topic %q", topicName)
		}
		unique = append(unique, topicName)
	}
	partitions := make([]TopicPartition, 0)
	for _, topicName := range unique {
		count := partitionCounts[topicName]
		if count <= 0 {
			return nil, nil, fmt.Errorf("invalid partition count for topic %q", topicName)
		}
		for partition := 0; partition < count; partition++ {
			partitions = append(partitions, TopicPartition{Topic: topicName, Partition: partition})
		}
	}
	return append([]string(nil), unique...), partitions, nil
}

func sameSubscription(group *GroupMetadata, topics []string, pattern string, partitions []TopicPartition) bool {
	return group != nil && slices.Equal(group.Topics, topics) && group.TopicPattern == pattern && slices.Equal(group.TopicPartitions, partitions)
}

func subscriptionDisplayName(topics []string, pattern string) string {
	if pattern != "" {
		return pattern
	}
	if len(topics) == 1 {
		return topics[0]
	}
	return ""
}

func makePartitions(partitionCount int) []int {
	partitions := make([]int, partitionCount)
	for partition := range partitions {
		partitions[partition] = partition
	}
	return partitions
}

// DeleteGroup persists a lifecycle tombstone before removing in-memory state.
// Repeated deletion of an already deleted group is idempotent.
func (c *Coordinator) DeleteGroup(groupName string) error {
	if groupName == "" {
		return fmt.Errorf("group name must not be empty")
	}

	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	group := c.groups[groupName]
	if group == nil {
		c.mu.Unlock()
		return nil
	}
	group.mu.Lock()
	if len(group.Members) != 0 {
		group.mu.Unlock()
		c.mu.Unlock()
		return fmt.Errorf("cannot delete group %q with active members", groupName)
	}
	epoch := c.groupEpochs[groupName]
	if group.RegistrationEpoch > epoch {
		epoch = group.RegistrationEpoch
	}
	epoch++
	if epoch == 0 {
		group.mu.Unlock()
		c.mu.Unlock()
		return fmt.Errorf("group lifecycle epoch overflow")
	}
	topicName := group.TopicName
	if c.lifecyclePending == nil {
		c.lifecyclePending = make(map[string]bool)
	}
	c.lifecyclePending[groupName] = true
	group.mu.Unlock()
	c.mu.Unlock()

	err := c.writeGroupTombstone(groupName, topicName, epoch)

	c.mu.Lock()
	delete(c.lifecyclePending, groupName)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("persist group tombstone: %w", err)
	}
	if c.groups[groupName] != group {
		c.mu.Unlock()
		return fmt.Errorf("group %q changed during durable deletion", groupName)
	}
	delete(c.groups, groupName)
	c.groupEpochs[groupName] = epoch
	delete(c.ownershipSince, groupName)
	c.mu.Unlock()
	c.updateOffsetPartitionCount()
	return nil
}

// AddConsumer registers a new consumer in the group and triggers a rebalance.
func (c *Coordinator) AddConsumer(groupName, consumerID string) ([]int, error) {
	c.mu.Lock()
	if c.lifecyclePending[groupName] {
		c.mu.Unlock()
		return nil, fmt.Errorf("group lifecycle update in progress")
	}
	group := c.groups[groupName]
	if group == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("group not found")
	}

	now := time.Now()
	group.Members[consumerID] = &MemberMetadata{
		ID:            consumerID,
		LastHeartbeat: now,
	}
	group.Generation++

	c.rebalanceRange(groupName)
	assignments := append([]int(nil), group.Members[consumerID].Assignments...)
	gen := group.Generation
	c.mu.Unlock()

	util.Info("✅ Consumer '%s' joined (Generation: %d, Assignments: %v)", consumerID, gen, assignments)
	return assignments, nil
}

// RemoveConsumer unregisters a consumer and triggers a rebalance.
func (c *Coordinator) RemoveConsumer(groupName, consumerID string) error {
	c.mu.Lock()
	memberCount, generation, err := c.removeConsumerLocked(groupName, consumerID)
	c.mu.Unlock()
	if err != nil {
		return err
	}

	c.updateOffsetPartitionCount()
	util.Info("Consumer '%s' left group '%s' (generation=%d remaining=%d)", consumerID, groupName, generation, memberCount)
	return nil
}

// RemoveConsumerForGeneration prevents a stale session from removing a current
// group member after ownership has moved to a newer generation.
func (c *Coordinator) RemoveConsumerForGeneration(groupName, consumerID string, generation int) error {
	c.mu.Lock()
	if errResp := c.validateMemberGenerationLocked(groupName, consumerID, generation); errResp != "" {
		c.mu.Unlock()
		return fmt.Errorf("%s", errResp)
	}
	memberCount, newGeneration, err := c.removeConsumerLocked(groupName, consumerID)
	c.mu.Unlock()
	if err != nil {
		return err
	}

	c.updateOffsetPartitionCount()
	util.Info("Consumer '%s' left group '%s' (generation=%d remaining=%d)", consumerID, groupName, newGeneration, memberCount)
	return nil
}

// ExpireConsumers removes all members that crossed the same timeout boundary in
// one generation change. expectedGeneration makes metadata-log replay and retries
// harmless after a concurrent membership change.
func (c *Coordinator) ExpireConsumers(groupName string, expectedGeneration int, consumerIDs []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		return fmt.Errorf("ERROR: group_not_found group=%s", groupName)
	}
	if group.Generation != expectedGeneration {
		return fmt.Errorf("ERROR: GEN_MISMATCH current=%d requested=%d group=%s", group.Generation, expectedGeneration, groupName)
	}

	removed := 0
	for _, consumerID := range consumerIDs {
		if _, exists := group.Members[consumerID]; exists {
			delete(group.Members, consumerID)
			removed++
		}
	}
	if removed == 0 {
		return nil
	}
	group.Generation++
	c.rebalanceRange(groupName)
	return nil
}

func (c *Coordinator) removeConsumerLocked(groupName, consumerID string) (int, int, error) {
	group := c.groups[groupName]
	if group == nil {
		return 0, 0, fmt.Errorf("group not found")
	}
	if _, exists := group.Members[consumerID]; !exists {
		return len(group.Members), group.Generation, fmt.Errorf("ERROR: member_not_found member=%s group=%s", consumerID, groupName)
	}
	delete(group.Members, consumerID)
	group.Generation++
	c.rebalanceRange(groupName)
	return len(group.Members), group.Generation, nil
}

// Rebalance forces a rebalance for a consumer group.
func (c *Coordinator) Rebalance(groupName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebalanceRange(groupName)
}

// rebalanceRange redistributes partitions among consumers using range-based assignment.
func (c *Coordinator) rebalanceRange(groupName string) {
	group := c.groups[groupName]
	if group == nil {
		util.Error("❌ Cannot rebalance: group '%s' not found", groupName)
		return
	}

	members := make([]string, 0, len(group.Members))
	for id := range group.Members {
		members = append(members, id)
	}
	sort.Strings(members)
	now := time.Now()
	group.LastActivity = now
	group.LastRebalance = now

	if len(members) == 0 {
		util.Warn("⚠️ No active members in group '%s', skipping rebalance", groupName)
		return
	}

	if len(group.TopicPartitions) > 0 {
		rebalanceTopicPartitions(group, members)
		group.LastRebalance = time.Now()
		return
	}

	pCount := len(group.Partitions)
	mCount := len(members)
	partitionsPerConsumer := pCount / mCount
	remainder := pCount % mCount

	partitionIdx := 0
	for i, memberID := range members {
		count := partitionsPerConsumer
		if i < remainder {
			count++
		}

		var newAssignments []int
		if partitionIdx < pCount {
			end := partitionIdx + count
			if end > pCount {
				end = pCount
			}
			// Copy the slice to avoid sharing the backing array with group.Partitions
			newAssignments = make([]int, end-partitionIdx)
			copy(newAssignments, group.Partitions[partitionIdx:end])
		}

		group.Members[memberID].Assignments = newAssignments
		partitionIdx += len(newAssignments)

		util.Info("📋 Assigned %v to %s", newAssignments, memberID)
	}
}

func rebalanceTopicPartitions(group *GroupMetadata, members []string) {
	pCount := len(group.TopicPartitions)
	mCount := len(members)
	partitionsPerConsumer := pCount / mCount
	remainder := pCount % mCount
	partitionIdx := 0
	for i, memberID := range members {
		count := partitionsPerConsumer
		if i < remainder {
			count++
		}
		end := partitionIdx + count
		if end > pCount {
			end = pCount
		}
		member := group.Members[memberID]
		member.Assignments = nil
		member.TopicAssignments = append([]TopicPartition(nil), group.TopicPartitions[partitionIdx:end]...)
		partitionIdx = end
	}
}
