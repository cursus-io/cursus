package fsm

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

const LeaderElectionTopologyFenceVersionCurrent = 1

type LeaderElectionCommand struct {
	ReqID                      string   `json:"req_id,omitempty"`
	Topic                      string   `json:"topic"`
	Partition                  int      `json:"partition"`
	Broker                     string   `json:"broker"`
	ExpectedLeaderEpoch        int      `json:"expected_leader_epoch"`
	TopologyFenceVersion       int      `json:"topology_fence_version,omitempty"`
	ExpectedLifecycleEpoch     uint64   `json:"expected_lifecycle_epoch,omitempty"`
	ExpectedDefinitionRevision uint64   `json:"expected_definition_revision,omitempty"`
	ExpectedReplicas           []string `json:"expected_replicas,omitempty"`
}

type LeaderElectionResult struct {
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	PreviousLeader string `json:"previous_leader"`
	Leader         string `json:"leader"`
	LeaderEpoch    int    `json:"leader_epoch"`
	Changed        bool   `json:"changed"`
}

func (f *BrokerFSM) applyLeaderElectionCommand(jsonData string) interface{} {
	var cmd LeaderElectionCommand
	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		return fmt.Errorf("unmarshal leader election: %w", err)
	}
	if strings.TrimSpace(cmd.Topic) == "" {
		return fmt.Errorf("leader election topic is required")
	}
	if cmd.Partition < 0 {
		return fmt.Errorf("leader election partition must be non-negative")
	}
	if strings.TrimSpace(cmd.Broker) == "" {
		return fmt.Errorf("leader election broker is required")
	}
	fenced := false
	switch cmd.TopologyFenceVersion {
	case 0:
		// Version zero is reserved for committed entries written before topology
		// fencing existed. Replay their original leader-epoch-only contract.
		if cmd.ExpectedLifecycleEpoch != 0 || cmd.ExpectedDefinitionRevision != 0 || len(cmd.ExpectedReplicas) != 0 {
			return fmt.Errorf("unversioned leader election contains topology fence fields")
		}
	case LeaderElectionTopologyFenceVersionCurrent:
		fenced = true
		if cmd.ExpectedLifecycleEpoch == 0 {
			return fmt.Errorf("leader election expected lifecycle epoch is required")
		}
		if cmd.ExpectedDefinitionRevision == 0 {
			return fmt.Errorf("leader election expected definition revision is required")
		}
		if len(cmd.ExpectedReplicas) == 0 {
			return fmt.Errorf("leader election expected replicas are required")
		}
	default:
		return fmt.Errorf("unsupported leader election topology fence version %d", cmd.TopologyFenceVersion)
	}

	key := fmt.Sprintf("%s-%d", cmd.Topic, cmd.Partition)
	f.mu.Lock()
	defer f.mu.Unlock()

	metadata := f.partitionMetadata[key]
	if metadata == nil {
		return fmt.Errorf("partition metadata %s not found", key)
	}
	if fenced {
		definition := f.topicState[cmd.Topic]
		if definition == nil {
			return fmt.Errorf("topic definition %s not found", cmd.Topic)
		}
		if definition.LifecycleEpoch != cmd.ExpectedLifecycleEpoch || metadata.LifecycleEpoch != cmd.ExpectedLifecycleEpoch {
			return fmt.Errorf(
				"stale lifecycle epoch for %s: topic=%d partition=%d expected=%d",
				key, definition.LifecycleEpoch, metadata.LifecycleEpoch, cmd.ExpectedLifecycleEpoch,
			)
		}
		if definition.Revision != cmd.ExpectedDefinitionRevision {
			return fmt.Errorf("stale definition revision for %s: current=%d expected=%d", key, definition.Revision, cmd.ExpectedDefinitionRevision)
		}
		if definition.ReplicationFactor != len(cmd.ExpectedReplicas) {
			return fmt.Errorf("stale replication factor for %s: current=%d expected_replicas=%d", key, definition.ReplicationFactor, len(cmd.ExpectedReplicas))
		}
		if !slices.Equal(metadata.Replicas, cmd.ExpectedReplicas) {
			return fmt.Errorf("stale replica assignment for %s: current=%v expected=%v", key, metadata.Replicas, cmd.ExpectedReplicas)
		}
		if len(metadata.RecoveryReplicas) != 0 {
			return fmt.Errorf("replica recovery pending for %s: brokers=%v", key, metadata.RecoveryReplicas)
		}
	}
	broker := f.brokers[cmd.Broker]
	if broker == nil {
		return fmt.Errorf("broker %s not found", cmd.Broker)
	}
	if !strings.EqualFold(broker.Status, "active") {
		return fmt.Errorf("broker %s is not active", cmd.Broker)
	}
	if !containsBroker(metadata.Replicas, cmd.Broker) {
		return fmt.Errorf("broker %s is not a replica of %s", cmd.Broker, key)
	}
	if !containsBroker(metadata.ISR, cmd.Broker) {
		return fmt.Errorf("broker %s is not in the ISR of %s", cmd.Broker, key)
	}

	result := LeaderElectionResult{
		Topic:          cmd.Topic,
		Partition:      cmd.Partition,
		PreviousLeader: metadata.Leader,
		Leader:         metadata.Leader,
		LeaderEpoch:    metadata.LeaderEpoch,
	}
	if metadata.Leader == cmd.Broker {
		return result
	}
	if cmd.ExpectedLeaderEpoch != metadata.LeaderEpoch {
		return fmt.Errorf("stale leader epoch for %s: current=%d expected=%d", key, metadata.LeaderEpoch, cmd.ExpectedLeaderEpoch)
	}

	metadata.Leader = cmd.Broker
	metadata.LeaderEpoch++
	result.Leader = metadata.Leader
	result.LeaderEpoch = metadata.LeaderEpoch
	result.Changed = true
	return result
}

func containsBroker(brokers []string, target string) bool {
	for _, broker := range brokers {
		if broker == target {
			return true
		}
	}
	return false
}
