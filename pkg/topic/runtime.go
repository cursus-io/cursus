package topic

import (
	"errors"
	"fmt"
	"sort"
)

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
	TopicCount                      int
	Partitions                      []PartitionRuntimeSnapshot
	MetadataLoadFailure             string
	MetadataOrphanTopicCount        int
	MetadataDurabilityWarning       string
	MetadataDurabilityWarningsTotal uint64
}

// MetadataReadinessError reports durable topic metadata conditions that need
// operator attention. It is safe to call concurrently with topic mutations.
func (tm *TopicManager) MetadataReadinessError() error {
	if tm == nil {
		return fmt.Errorf("topic metadata state unavailable")
	}

	tm.mu.RLock()
	loadFailure := tm.metadataLoadFailure
	orphanCount := tm.metadataOrphanTopicCount
	durabilityWarning := tm.metadataDurabilityWarning
	tm.mu.RUnlock()

	var readinessErr error
	if loadFailure != "" {
		readinessErr = errors.Join(readinessErr, fmt.Errorf("manifest load failure: %s", loadFailure))
	}
	if orphanCount > 0 {
		readinessErr = errors.Join(readinessErr, fmt.Errorf("%d orphan topic(s) confirmed", orphanCount))
	}
	if durabilityWarning != "" {
		readinessErr = errors.Join(readinessErr, fmt.Errorf("metadata durability warning: %s", durabilityWarning))
	}
	return readinessErr
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
	metadataLoadFailure := tm.metadataLoadFailure
	metadataOrphanTopicCount := tm.metadataOrphanTopicCount
	metadataDurabilityWarning := tm.metadataDurabilityWarning
	metadataDurabilityWarningsTotal := tm.metadataDurabilityWarningsTotal
	tm.mu.RUnlock()

	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	snapshot := RuntimeSnapshot{
		TopicCount:                      len(topics),
		MetadataLoadFailure:             metadataLoadFailure,
		MetadataOrphanTopicCount:        metadataOrphanTopicCount,
		MetadataDurabilityWarning:       metadataDurabilityWarning,
		MetadataDurabilityWarningsTotal: metadataDurabilityWarningsTotal,
	}
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
