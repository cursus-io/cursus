package coordinator

import (
	"fmt"
	"sort"
	"strings"
)

// TopicGroupReference describes consumer-group state tied to a topic.
type TopicGroupReference struct {
	Name                string
	MemberCount         int
	PendingReservations int
	Shared              bool
	Pattern             bool
}

// TopicGroupReferences returns a detached, deterministic view of groups that
// subscribe to or retain state for topicName, including before the first commit.
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
		if reference, found := topicGroupReferenceLocked(name, group, topicName); found {
			references = append(references, reference)
		}
		group.mu.RUnlock()
	}
	sort.Slice(references, func(i, j int) bool { return references[i].Name < references[j].Name })
	return references
}

// topicGroupReferenceLocked requires the group lock. A whole-group tombstone is
// safe only when every retained reference belongs to the topic being removed.
func topicGroupReferenceLocked(name string, group *GroupMetadata, topicName string) (TopicGroupReference, bool) {
	reference := TopicGroupReference{
		Name:                name,
		MemberCount:         len(group.Members),
		PendingReservations: len(group.OffsetReservations),
		Pattern:             group.TopicPattern != "" || strings.ContainsAny(group.TopicName, "*?"),
	}
	found := false
	visit := func(name string) {
		if name == topicName {
			found = true
		} else if name != "" {
			reference.Shared = true
		}
	}
	if len(group.Topics) == 0 {
		if strings.ContainsAny(group.TopicName, "*?") {
			found = groupTopicMatches(group.TopicName, topicName)
		} else {
			visit(group.TopicName)
		}
	}
	for _, name := range group.Topics {
		visit(name)
	}
	for _, partition := range group.TopicPartitions {
		visit(partition.Topic)
	}
	for name := range group.Offsets {
		visit(name)
	}
	for name := range group.OffsetRevisions {
		visit(name)
	}
	for _, reservation := range group.OffsetReservations {
		for _, offset := range reservation.Offsets {
			visit(offset.Topic)
		}
	}
	return reference, found
}

func (reference TopicGroupReference) validateCleanup(topicName string) error {
	switch {
	case reference.MemberCount != 0:
		return fmt.Errorf("topic %q has active consumer group %q with %d member(s)", topicName, reference.Name, reference.MemberCount)
	case reference.PendingReservations != 0:
		return fmt.Errorf("consumer group %q has pending transaction offset reservations for topic cleanup %q", reference.Name, topicName)
	case reference.Pattern:
		return fmt.Errorf("consumer group %q has a pattern subscription; topic %q cleanup cannot delete the entire group", reference.Name, topicName)
	case reference.Shared:
		return fmt.Errorf("consumer group %q retains subscriptions or offsets outside topic %q", reference.Name, topicName)
	default:
		return nil
	}
}

// ValidateTopicGroupCleanup checks admission without mutating group state.
// Actual cleanup repeats these checks after the topic intent has committed.
func (c *Coordinator) ValidateTopicGroupCleanup(topicName string) error {
	for _, reference := range c.TopicGroupReferences(topicName) {
		if err := reference.validateCleanup(topicName); err != nil {
			return err
		}
	}
	return nil
}

// DeleteInactiveGroupsForTopic writes normal lifecycle tombstones before
// removing groups confined to this topic. All references must pass preflight.
func (c *Coordinator) DeleteInactiveGroupsForTopic(topicName string) ([]string, error) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	references := c.TopicGroupReferences(topicName)
	for _, reference := range references {
		if err := reference.validateCleanup(topicName); err != nil {
			return nil, err
		}
	}

	deleted := make([]string, 0, len(references))
	for _, reference := range references {
		if err := c.deleteGroupLocked(reference.Name, topicName); err != nil {
			return deleted, fmt.Errorf("delete consumer group %q for topic %q: %w", reference.Name, topicName, err)
		}
		deleted = append(deleted, reference.Name)
	}
	return deleted, nil
}
