package coordinator

import (
	"fmt"
	"slices"
)

// PreserveConsumerReplayClocks selects only partitions whose ownership proof
// remains unchanged. Restore/startup and newly acquired partitions must not
// inherit old session windows. No clock is persisted in a replay index.
func (c *Coordinator) PreserveConsumerReplayClocks(p *PreparedConsumerMetadataReplay, route ConsumerMetadataRoute, unchanged map[int]bool) error {
	if p == nil || p.owner != c || p.scratch == nil || p.scope != nil || route == nil {
		return fmt.Errorf("invalid replay clock preservation request")
	}
	if err := c.validateConsumerMetadataReplayStamp(p.stamp); err != nil {
		return err
	}
	p.preserveClocks = make(map[string]bool)
	for name := range p.scratch.groups {
		partition, err := route(name)
		if err != nil {
			return err
		}
		if unchanged[partition] {
			p.preserveClocks[name] = true
		}
	}
	return nil
}

func sameRecoveredSession(a, b *GroupMetadata) bool {
	if a.RegistrationEpoch != b.RegistrationEpoch || a.Generation != b.Generation || a.TopicName != b.TopicName || a.TopicPattern != b.TopicPattern || !a.LastRebalance.Equal(b.LastRebalance) || !slices.Equal(a.Topics, b.Topics) || !slices.Equal(a.TopicPartitions, b.TopicPartitions) || !slices.Equal(a.Partitions, b.Partitions) || len(a.Members) != len(b.Members) {
		return false
	}
	for id, member := range a.Members {
		next := b.Members[id]
		if member == nil || next == nil || !slices.Equal(member.Assignments, next.Assignments) || !slices.Equal(member.TopicAssignments, next.TopicAssignments) {
			return false
		}
	}
	return true
}
