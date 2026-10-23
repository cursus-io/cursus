package fsm

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
)

// Called with f.mu held, from serialized Raft apply; never sample node-local
// coordinator state or local storage HWM to reconstruct an historical boundary.
func (f *BrokerFSM) captureTopicCleanupBoundaryLocked(target topic.Definition) {
	b := &coordinator.TopicCleanupBoundary{TopicInstance: target.InstanceID, TopicEpoch: target.LifecycleEpoch}
	b.LegacyGroupsAbsent = f.cd.HistoricalConsumerGroupsAbsent()
	d := f.topicState[config.ConsumerOffsetsTopicName]
	if d != nil {
		b.MetadataInstance, b.MetadataEpoch = d.InstanceID, d.LifecycleEpoch
		for p := 0; p < d.Partitions; p++ {
			m := f.partitionMetadata[fmt.Sprintf("%s-%d", d.Name, p)]
			if m == nil || !m.CommittedHWMKnown || m.LifecycleEpoch != d.LifecycleEpoch {
				b.Ends = nil
				break
			}
			b.Ends = append(b.Ends, m.CommittedHWM)
		}
	}
	if f.topicCleanupBoundaries == nil {
		f.topicCleanupBoundaries = make(map[string]*coordinator.TopicCleanupBoundary)
	}
	f.topicCleanupBoundaries[target.Name] = b
}

func (f *BrokerFSM) topicCleanupBoundaryLocked(d *topic.Definition) *coordinator.TopicCleanupBoundary {
	if d == nil {
		return nil
	}
	b := f.topicCleanupBoundaries[d.Name]
	if b != nil && b.TopicInstance == d.InstanceID && b.TopicEpoch == d.LifecycleEpoch {
		return b.Clone()
	}
	return &coordinator.TopicCleanupBoundary{TopicInstance: d.InstanceID, TopicEpoch: d.LifecycleEpoch}
}

func copyTopicCleanupBoundaries(src map[string]*coordinator.TopicCleanupBoundary) map[string]*coordinator.TopicCleanupBoundary {
	dst := make(map[string]*coordinator.TopicCleanupBoundary, len(src))
	for name, b := range src {
		dst[name] = b.Clone()
	}
	return dst
}

func (f *BrokerFSM) snapshotTopicCleanupBoundariesLocked() map[string]*coordinator.TopicCleanupBoundary {
	// Older snapshot readers reject unknown fields. Include offline registered
	// brokers: they may return and install this snapshot during a rolling upgrade.
	for _, broker := range f.brokers {
		if broker.LifecycleProtocol < TruncateCleanupBoundaryProtocolVersion {
			return nil
		}
	}
	return copyTopicCleanupBoundaries(f.topicCleanupBoundaries)
}

func validateTopicCleanupBoundaries(src map[string]*coordinator.TopicCleanupBoundary, topics map[string]*topic.Definition, metadata map[string]*PartitionMetadata) error {
	for name, b := range src {
		d := topics[name]
		if b == nil || d == nil || b.TopicEpoch != d.LifecycleEpoch || b.TopicInstance != d.InstanceID {
			return fmt.Errorf("invalid truncate cleanup boundary for %q", name)
		}
		if len(b.Ends) == 0 {
			continue
		}
		internal := topics[config.ConsumerOffsetsTopicName]
		if internal == nil || b.MetadataInstance != internal.InstanceID || b.MetadataEpoch != internal.LifecycleEpoch || len(b.Ends) != internal.Partitions {
			return fmt.Errorf("invalid consumer metadata identity in truncate boundary for %q", name)
		}
		for p, end := range b.Ends {
			m := metadata[fmt.Sprintf("%s-%d", internal.Name, p)]
			if m == nil || !m.CommittedHWMKnown || m.LifecycleEpoch != b.MetadataEpoch || end > m.CommittedHWM {
				return fmt.Errorf("truncate boundary exceeds committed consumer metadata for %q", name)
			}
		}
	}
	return nil
}
