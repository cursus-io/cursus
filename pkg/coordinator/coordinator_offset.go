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

func (c *Coordinator) CommitOffset(groupName, topic string, partition int, offset uint64) error {
	util.Debug("Committing offset: group='%s', topic='%s', partition=%d, offset=%d", groupName, topic, partition, offset)

	gm := c.getGroupSafe(groupName)
	if gm == nil {
		return fmt.Errorf("group '%s' not found", groupName)
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

	if err := c.topicHandler.Publish(c.offsetTopic, &types.Message{
		ProducerID: "broker",
		Payload:    string(payload),
		Key:        fmt.Sprintf("%s-%s-%d", groupName, topic, partition),
	}); err != nil {
		return err
	}

	gm.mu.Lock()
	gm.storeOffset(topic, partition, offset)
	gm.mu.Unlock()

	return nil
}

func (c *Coordinator) CommitOffsetsBulk(groupName, topic string, offsets []OffsetItem) error {
	if len(offsets) == 0 {
		return nil
	}

	gm := c.getGroupSafe(groupName)
	if gm == nil {
		return fmt.Errorf("group '%s' not found", groupName)
	}

	bulkMsg := BulkOffsetMsg{
		Group:     groupName,
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
		Key:        fmt.Sprintf("%s-%s-bulk", groupName, topic),
	}); err != nil {
		return err
	}

	gm.mu.Lock()
	for _, item := range offsets {
		gm.storeOffset(topic, item.Partition, item.Offset)
	}
	gm.mu.Unlock()

	return nil
}

func (c *Coordinator) ApplyOffsetUpdateFromFSM(groupName, topic string, offsets []OffsetItem) error {
	if groupName == "" || topic == "" {
		return fmt.Errorf("invalid group or topic name")
	}

	// FSM replay may arrive before RegisterGroup, so we need to get-or-create.
	c.mu.Lock()
	gm, ok := c.groups[groupName]
	if !ok {
		gm = &GroupMetadata{
			Offsets: make(map[string]map[int]uint64),
		}
		c.groups[groupName] = gm
	}
	c.mu.Unlock()

	gm.mu.Lock()
	if gm.Offsets == nil {
		gm.Offsets = make(map[string]map[int]uint64)
	}
	if _, exists := gm.Offsets[topic]; !exists {
		gm.Offsets[topic] = make(map[int]uint64)
	}
	for _, item := range offsets {
		gm.Offsets[topic][item.Partition] = item.Offset
	}
	gm.mu.Unlock()

	return nil
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
	// Validate membership under global lock (membership is managed globally)
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

	// Store offset under per-group lock
	group.mu.Lock()
	prevOffset, hadPrev := group.getOffsetSafe(topic, partition)
	group.storeOffset(topic, partition, offset)
	group.mu.Unlock()

	offsetMsg := OffsetCommitMessage{
		Group:     groupName,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(offsetMsg)
	if err != nil {
		c.rollbackOffset(group, topic, partition, prevOffset, hadPrev)
		return fmt.Errorf("failed to marshal offset: %w", err)
	}

	err = c.topicHandler.Publish(c.offsetTopic, &types.Message{
		ProducerID: "broker",
		Payload:    string(payload),
		Key:        fmt.Sprintf("%s-%s-%d", groupName, topic, partition),
	})

	if err != nil {
		c.rollbackOffset(group, topic, partition, prevOffset, hadPrev)
		return fmt.Errorf("failed to publish offset: %w", err)
	}

	return nil
}

func (c *Coordinator) rollbackOffset(gm *GroupMetadata, topic string, partition int, prevOffset uint64, hadPrev bool) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if hadPrev {
		gm.storeOffset(topic, partition, prevOffset)
	} else if partitions, ok := gm.Offsets[topic]; ok {
		delete(partitions, partition)
	}
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
