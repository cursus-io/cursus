package fsm

import (
	"encoding/json"
	"fmt"
	"strings"
)

type LeaderElectionCommand struct {
	ReqID               string `json:"req_id,omitempty"`
	Topic               string `json:"topic"`
	Partition           int    `json:"partition"`
	Broker              string `json:"broker"`
	ExpectedLeaderEpoch int    `json:"expected_leader_epoch"`
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

	key := fmt.Sprintf("%s-%d", cmd.Topic, cmd.Partition)
	f.mu.Lock()
	defer f.mu.Unlock()

	metadata := f.partitionMetadata[key]
	if metadata == nil {
		return fmt.Errorf("partition metadata %s not found", key)
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
