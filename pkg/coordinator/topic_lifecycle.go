package coordinator

import (
	"fmt"
	"sort"
)

// TopicGroupReference describes consumer-group state tied to a topic.
type TopicGroupReference struct {
	Name        string
	MemberCount int
}

// TopicGroupReferences returns a detached, deterministic view of groups that
// own or retain offsets for topicName.
func (c *Coordinator) TopicGroupReferences(topicName string) []TopicGroupReference {
	if c == nil || topicName == "" {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	references := make([]TopicGroupReference, 0)
	for name, group := range c.groups {
		if group == nil {
			continue
		}
		group.mu.RLock()
		_, hasOffsets := group.Offsets[topicName]
		if group.TopicName == topicName || hasOffsets {
			references = append(references, TopicGroupReference{Name: name, MemberCount: len(group.Members)})
		}
		group.mu.RUnlock()
	}
	sort.Slice(references, func(i, j int) bool { return references[i].Name < references[j].Name })
	return references
}

// DeleteInactiveGroupsForTopic writes normal lifecycle tombstones before
// removing groups. Any active member fails the entire preflight.
func (c *Coordinator) DeleteInactiveGroupsForTopic(topicName string) ([]string, error) {
	references := c.TopicGroupReferences(topicName)
	for _, reference := range references {
		if reference.MemberCount != 0 {
			return nil, fmt.Errorf(
				"topic %q has active consumer group %q with %d member(s)",
				topicName,
				reference.Name,
				reference.MemberCount,
			)
		}
	}

	deleted := make([]string, 0, len(references))
	for _, reference := range references {
		if err := c.DeleteGroup(reference.Name); err != nil {
			return deleted, fmt.Errorf("delete consumer group %q for topic %q: %w", reference.Name, topicName, err)
		}
		deleted = append(deleted, reference.Name)
	}
	return deleted, nil
}
