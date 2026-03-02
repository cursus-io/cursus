package coordinator

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func calculateOffsetPartitionCount(groupCount int) int {
	return min(max(groupCount/10, 4), 50)
}

func (c *Coordinator) storeOffsetInMemory(group, topic string, partition int, offset uint64) {
	if _, ok := c.offsets[group]; !ok {
		c.offsets[group] = make(map[string]map[int]uint64)
	}
	if _, ok := c.offsets[group][topic]; !ok {
		c.offsets[group][topic] = make(map[int]uint64)
	}
	c.offsets[group][topic][partition] = offset
}

func (c *Coordinator) CommitOffset(group, topic string, partition int, offset uint64) error {
	util.Debug("Committing offset: group='%s', topic='%s', partition=%d, offset=%d", group, topic, partition, offset)

	offsetMsg := OffsetCommitMessage{
		Group:     group,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(offsetMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal offset commit: %w", err)
	}

	if err := c.topicHandler.Publish(c.offsetTopic, &types.Message{
		ProducerID: "broker",
		Payload:    string(payload),
		Key:        fmt.Sprintf("%s-%s-%d", group, topic, partition),
	}); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.storeOffsetInMemory(group, topic, partition, offset)
	return nil
}

func (c *Coordinator) CommitOffsetsBulk(group, topic string, offsets []OffsetItem) error {
	if len(offsets) == 0 {
		return nil
	}

	bulkMsg := BulkOffsetMsg{
		Group:     group,
		Topic:     topic,
		Offsets:   offsets,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(bulkMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}

	if err := c.topicHandler.Publish(c.offsetTopic, &types.Message{
		ProducerID: "broker",
		Payload:    string(payload),
		Key:        fmt.Sprintf("%s-%s-bulk", group, topic),
	}); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, item := range offsets {
		c.storeOffsetInMemory(group, topic, item.Partition, item.Offset)
	}

	return nil
}

func (c *Coordinator) ApplyOffsetUpdateFromFSM(group, topic string, offsets []OffsetItem) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if group == "" || topic == "" {
		return fmt.Errorf("invalid group or topic name")
	}

	if _, ok := c.offsets[group]; !ok {
		c.offsets[group] = make(map[string]map[int]uint64)
	}
	if _, ok := c.offsets[group][topic]; !ok {
		c.offsets[group][topic] = make(map[int]uint64)
	}

	for _, item := range offsets {
		c.offsets[group][topic][item.Partition] = item.Offset
	}

	return nil
}

func (c *Coordinator) GetOffset(group, topic string, partition int) (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getOffsetSafe(group, topic, partition)
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
		c.topicHandler.CreateTopic(topicName, newCount)
		util.Info("✅ Offset topic '%s' partitions scaled to %d", topicName, newCount)
	}()
}

func (c *Coordinator) ValidateAndCommit(groupName, topic string, partition int, offset uint64, generation int, memberID string) error {
	c.mu.RLock()
	group := c.groups[groupName]

	if group == nil || group.Members[memberID] == nil {
		c.mu.RUnlock()
		return fmt.Errorf("group/member not found")
	}
	if group.Generation != generation {
		c.mu.RUnlock()
		return fmt.Errorf("generation mismatch")
	}
	if !contains(group.Members[memberID].Assignments, partition) {
		c.mu.RUnlock()
		return fmt.Errorf("not partition owner")
	}
	c.mu.RUnlock()

	offsetMsg := OffsetCommitMessage{
		Group:     groupName,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(offsetMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal offset: %w", err)
	}

	err = c.topicHandler.Publish(c.offsetTopic, &types.Message{
		ProducerID: "broker",
		Payload:    string(payload),
		Key:        fmt.Sprintf("%s-%s-%d", groupName, topic, partition),
	})

	if err != nil {
		return fmt.Errorf("failed to publish offset: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	g, ok := c.groups[groupName]
	if ok && g.Generation == generation {
		c.storeOffsetInMemory(groupName, topic, partition, offset)
		return nil
	}

	util.Warn("offset published but not stored in memory: Generation changed (%d -> %d) for group %s", generation, g.Generation, groupName)
	return fmt.Errorf("generation changed during commit")
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
