package topic

import "sort"

// PartitionRuntimeSnapshot is a point-in-time view used by broker observability.
type PartitionRuntimeSnapshot struct {
	Topic         string
	Partition     int
	LogStart      uint64
	LogEnd        uint64
	HighWatermark uint64
}

// RuntimeSnapshot is a point-in-time view of topics and partitions.
type RuntimeSnapshot struct {
	TopicCount int
	Partitions []PartitionRuntimeSnapshot
}

// RuntimeSnapshot returns a race-safe copy of broker topic state.
func (tm *TopicManager) RuntimeSnapshot() RuntimeSnapshot {
	if tm == nil {
		return RuntimeSnapshot{}
	}

	tm.mu.RLock()
	topics := make([]*Topic, 0, len(tm.topics))
	for _, current := range tm.topics {
		topics = append(topics, current)
	}
	tm.mu.RUnlock()

	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	snapshot := RuntimeSnapshot{TopicCount: len(topics)}
	for _, current := range topics {
		current.mu.RLock()
		partitions := append([]*Partition(nil), current.Partitions...)
		current.mu.RUnlock()

		for _, partition := range partitions {
			if partition == nil {
				continue
			}
			snapshot.Partitions = append(snapshot.Partitions, PartitionRuntimeSnapshot{
				Topic:         current.Name,
				Partition:     partition.ID(),
				LogStart:      partition.GetFirstOffset(),
				LogEnd:        partition.NextOffset(),
				HighWatermark: partition.GetHWM(),
			})
		}
	}

	return snapshot
}
