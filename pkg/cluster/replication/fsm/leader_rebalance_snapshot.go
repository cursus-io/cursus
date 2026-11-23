package fsm

import "github.com/cursus-io/cursus/pkg/topic"

// LeaderRebalanceSnapshot is a detached, point-in-time view of the durable
// topology used to plan preferred leader elections.
type LeaderRebalanceSnapshot struct {
	Topics     map[string]topic.Definition
	Partitions map[string]PartitionMetadata
	Brokers    map[string]BrokerInfo
}

// GetLeaderRebalanceSnapshot copies every planner input under one FSM read
// lock so a plan never combines topology from different applied indexes.
func (f *BrokerFSM) GetLeaderRebalanceSnapshot() LeaderRebalanceSnapshot {
	snapshot := LeaderRebalanceSnapshot{
		Topics:     make(map[string]topic.Definition),
		Partitions: make(map[string]PartitionMetadata),
		Brokers:    make(map[string]BrokerInfo),
	}
	if f == nil {
		return snapshot
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	for name, definition := range f.topicState {
		if definition != nil {
			snapshot.Topics[name] = *copyTopicDefinition(definition)
		}
	}
	for key, metadata := range f.partitionMetadata {
		if metadata == nil {
			continue
		}
		copy := *metadata
		copy.Replicas = append([]string(nil), metadata.Replicas...)
		copy.ISR = append([]string(nil), metadata.ISR...)
		copy.RecoveryReplicas = append([]string(nil), metadata.RecoveryReplicas...)
		snapshot.Partitions[key] = copy
	}
	for id, broker := range f.brokers {
		if broker != nil {
			snapshot.Brokers[id] = *broker
		}
	}
	return snapshot
}
