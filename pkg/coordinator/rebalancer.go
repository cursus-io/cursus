package coordinator

import (
	"fmt"
	"sort"
	"time"

	"github.com/cursus-io/cursus/util"
)

// RegisterGroup creates a new consumer group for a topic.
func (c *Coordinator) RegisterGroup(topicName, groupName string, partitionCount int) error {
	c.mu.Lock()
	if partitionCount <= 0 {
		c.mu.Unlock()
		return fmt.Errorf("invalid partition count: %d", partitionCount)
	}

	if existing, exists := c.groups[groupName]; exists {
		currPartitions := len(existing.Partitions)
		if currPartitions == 0 {
			partitions := make([]int, partitionCount)
			for i := 0; i < partitionCount; i++ {
				partitions[i] = i
			}
			existing.TopicName = topicName
			existing.Partitions = partitions
			if existing.Members == nil {
				existing.Members = make(map[string]*MemberMetadata)
			}
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		if currPartitions != partitionCount {
			return fmt.Errorf("partition count mismatch (existing: %d, requested: %d)", currPartitions, partitionCount)
		}
		return nil
	}

	partitions := make([]int, partitionCount)
	for i := 0; i < partitionCount; i++ {
		partitions[i] = i
	}

	c.groups[groupName] = &GroupMetadata{
		TopicName:  topicName,
		Members:    make(map[string]*MemberMetadata),
		Partitions: partitions,
		Offsets:    make(map[string]map[int]uint64),
	}
	c.mu.Unlock()

	c.updateOffsetPartitionCount()
	util.Info("🆕 Group '%s' registered for topic '%s' (%d partitions)", groupName, topicName, partitionCount)
	return nil
}

// AddConsumer registers a new consumer in the group and triggers a rebalance.
func (c *Coordinator) AddConsumer(groupName, consumerID string) ([]int, error) {
	c.mu.Lock()
	group := c.groups[groupName]
	if group == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("group not found")
	}

	group.Members[consumerID] = &MemberMetadata{
		ID:            consumerID,
		LastHeartbeat: time.Now(),
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

	if len(members) == 0 {
		util.Warn("⚠️ No active members in group '%s', skipping rebalance", groupName)
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
	group.LastRebalance = time.Now()
}
