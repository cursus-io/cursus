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
	group := c.groups[groupName]
	if group == nil {
		c.mu.Unlock()
		return fmt.Errorf("group not found")
	}

	delete(group.Members, consumerID)
	group.Generation++
	c.rebalanceRange(groupName)

	memberCount := len(group.Members)
	gen := group.Generation
	c.mu.Unlock()

	c.updateOffsetPartitionCount()
	util.Info("👋 Consumer '%s' left group '%s' (New Gen: %d, Remaining: %d)", consumerID, groupName, gen, memberCount)
	return nil
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
			newAssignments = group.Partitions[partitionIdx:end]
		}

		group.Members[memberID].Assignments = newAssignments
		partitionIdx += len(newAssignments)

		util.Info("📋 Assigned %v to %s", newAssignments, memberID)
	}
	group.LastRebalance = time.Now()
}
