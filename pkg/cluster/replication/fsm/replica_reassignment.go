package fsm

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

// ReplicaReassignmentCommand changes one durable partition assignment. Moving
// a replica is a two-step operation: add one target, wait for ISR catch-up,
// then remove the old replica.
type ReplicaReassignmentCommand struct {
	Topic            string   `json:"topic"`
	Partition        int      `json:"partition"`
	LifecycleEpoch   uint64   `json:"lifecycle_epoch"`
	Leader           string   `json:"leader"`
	LeaderEpoch      int      `json:"leader_epoch"`
	ExpectedReplicas []string `json:"expected_replicas"`
	TargetReplicas   []string `json:"target_replicas"`
}

func (f *BrokerFSM) applyReplicaReassignmentCommand(jsonData string) interface{} {
	var command ReplicaReassignmentCommand
	if err := decodeStrictJSON([]byte(jsonData), &command); err != nil {
		return fmt.Errorf("decode replica reassignment: %w", err)
	}
	if err := topic.ValidateName(command.Topic); err != nil {
		return fmt.Errorf("invalid topic name: %w", err)
	}
	if command.Partition < 0 {
		return fmt.Errorf("invalid partition %d", command.Partition)
	}
	if command.LifecycleEpoch == 0 || command.Leader == "" || command.LeaderEpoch <= 0 {
		return fmt.Errorf("replica reassignment requires lifecycle and leader fences")
	}
	if err := validateDistinctReplicaIDs(command.ExpectedReplicas, "expected"); err != nil {
		return err
	}
	if err := validateDistinctReplicaIDs(command.TargetReplicas, "target"); err != nil {
		return err
	}

	key := command.Topic + "-" + strconv.Itoa(command.Partition)
	f.mu.Lock()
	defer f.mu.Unlock()

	definition := f.topicState[command.Topic]
	if definition == nil {
		return fmt.Errorf("topic %q not found", command.Topic)
	}
	if command.Partition >= definition.Partitions {
		return fmt.Errorf("partition %d outside topic %q partition count %d", command.Partition, command.Topic, definition.Partitions)
	}
	if len(command.TargetReplicas) < definition.ReplicationFactor || len(command.TargetReplicas) > definition.ReplicationFactor+1 {
		return fmt.Errorf(
			"target replica count %d must be replication factor %d or temporary catch-up size %d for topic %q",
			len(command.TargetReplicas), definition.ReplicationFactor, definition.ReplicationFactor+1, command.Topic,
		)
	}
	metadata := f.partitionMetadata[key]
	if metadata == nil {
		return fmt.Errorf("partition metadata %q not found", key)
	}
	if len(metadata.RecoveryReplicas) > 0 {
		return fmt.Errorf("replica recovery pending for %s", key)
	}
	if metadata.LifecycleEpoch != command.LifecycleEpoch || definition.LifecycleEpoch != command.LifecycleEpoch {
		return fmt.Errorf(
			"stale lifecycle epoch for %s: definition=%d partition=%d requested=%d",
			key, definition.LifecycleEpoch, metadata.LifecycleEpoch, command.LifecycleEpoch,
		)
	}
	if metadata.Leader != command.Leader || metadata.LeaderEpoch != command.LeaderEpoch {
		return fmt.Errorf(
			"stale leader fence for %s: current=%s/%d requested=%s/%d",
			key, metadata.Leader, metadata.LeaderEpoch, command.Leader, command.LeaderEpoch,
		)
	}
	if !containsReplica(command.TargetReplicas, metadata.Leader) {
		return fmt.Errorf("target replicas for %s do not contain current leader %q", key, metadata.Leader)
	}
	for _, replica := range command.TargetReplicas {
		broker := f.brokers[replica]
		if broker == nil || !strings.EqualFold(broker.Status, "active") {
			return fmt.Errorf("target replica %q for %s is not a durable active broker", replica, key)
		}
		if broker.LifecycleProtocol < ReplicaReassignmentProtocolVersion {
			return fmt.Errorf(
				"replica reassignment requires broker protocol %d; broker %q advertises %d",
				ReplicaReassignmentProtocolVersion, replica, broker.LifecycleProtocol,
			)
		}
	}
	if equalReplicaIDs(metadata.Replicas, command.TargetReplicas) {
		return nil
	}
	if !equalReplicaIDs(metadata.Replicas, command.ExpectedReplicas) {
		return fmt.Errorf(
			"stale replica assignment for %s: current=%v expected=%v",
			key, metadata.Replicas, command.ExpectedReplicas,
		)
	}
	if err := validateDistinctReplicaIDs(metadata.Replicas, "current"); err != nil {
		return fmt.Errorf("partition %s: %w", key, err)
	}
	adding, removing := false, false
	for _, replica := range command.TargetReplicas {
		adding = adding || !containsReplica(metadata.Replicas, replica)
	}
	for _, replica := range metadata.Replicas {
		removing = removing || !containsReplica(command.TargetReplicas, replica)
	}
	if adding && removing {
		return fmt.Errorf("replica reassignment for %s must add and remove in separate steps", key)
	}
	if removing {
		if len(command.TargetReplicas) != definition.ReplicationFactor {
			return fmt.Errorf("replica removal for %s must restore replication factor %d", key, definition.ReplicationFactor)
		}
		for _, replica := range command.TargetReplicas {
			if !containsReplica(metadata.ISR, replica) {
				return fmt.Errorf("target replica %q for %s has not joined ISR", replica, key)
			}
		}
	}
	for _, replica := range metadata.ISR {
		if !containsReplica(metadata.Replicas, replica) {
			return fmt.Errorf("partition %s ISR member %q is absent from current replicas", key, replica)
		}
	}

	updated := *metadata
	updated.Replicas = append([]string(nil), command.TargetReplicas...)
	updated.ISR = make([]string, 0, len(command.TargetReplicas))
	for _, replica := range command.TargetReplicas {
		if containsReplica(metadata.ISR, replica) {
			updated.ISR = append(updated.ISR, replica)
		}
	}
	f.partitionMetadata[key] = &updated
	util.Info("FSM: Reassigned replicas for %s from %v to %v", key, metadata.Replicas, updated.Replicas)
	return nil
}

func validateDistinctReplicaIDs(replicas []string, label string) error {
	if len(replicas) == 0 {
		return fmt.Errorf("%s replica list is empty", label)
	}
	seen := make(map[string]struct{}, len(replicas))
	for _, replica := range replicas {
		if strings.TrimSpace(replica) == "" {
			return fmt.Errorf("%s replica list contains an empty broker ID", label)
		}
		if _, duplicate := seen[replica]; duplicate {
			return fmt.Errorf("%s replica list contains duplicate broker %q", label, replica)
		}
		seen[replica] = struct{}{}
	}
	return nil
}

func containsReplica(replicas []string, wanted string) bool {
	for _, replica := range replicas {
		if replica == wanted {
			return true
		}
	}
	return false
}

func equalReplicaIDs(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
