package controller

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"context"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

const topologyReconcileInterval = time.Second

// StartTopologyReconciler repairs one legacy underfilled assignment per pass.
// Data catch-up and ISR admission remain in their existing independently
// fenced loops after the target replica set is durable.
func (cc *ClusterController) StartTopologyReconciler(ctx context.Context) {
	if cc == nil || cc.RaftManager == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(topologyReconcileInterval)
		defer ticker.Stop()
		for {
			if err := cc.RunTopologyReconciliationOnce(); err != nil {
				util.Debug("topology reconciliation pending: %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// RunTopologyReconciliationOnce expands the first durable partition whose
// distinct replica set is shorter than its topic definition.
func (cc *ClusterController) RunTopologyReconciliationOnce() error {
	if cc == nil || cc.RaftManager == nil || cc.RaftManager.GetFSM() == nil {
		return fmt.Errorf("topology reconciliation requires raft FSM")
	}
	if !cc.RaftManager.IsLeader() {
		return nil
	}
	state := cc.RaftManager.GetFSM()
	keys := state.GetAllPartitionKeys()
	sort.Strings(keys)

	for _, key := range keys {
		topicName, partition, err := splitPartitionKey(key)
		if err != nil {
			return err
		}
		definition, found := state.GetTopicDefinition(topicName)
		if !found {
			return fmt.Errorf("partition %s has no durable topic definition", key)
		}
		metadata := state.GetPartitionMetadata(key)
		if metadata == nil {
			return fmt.Errorf("partition metadata %s is unavailable", key)
		}
		if err := validateCurrentReplicaAssignment(key, metadata); err != nil {
			return err
		}
		if len(metadata.Replicas) >= definition.ReplicationFactor {
			continue
		}

		active, err := reassignmentCapableBrokers(cc.RaftManager)
		if err != nil {
			return err
		}
		if len(active) < definition.ReplicationFactor {
			return fmt.Errorf(
				"partition %s requires %d active brokers; only %d are reassignment-capable",
				key, definition.ReplicationFactor, len(active),
			)
		}
		for _, replica := range metadata.Replicas {
			if !containsString(active, replica) {
				return fmt.Errorf("partition %s current replica %q is not active and reassignment-capable", key, replica)
			}
		}

		ring := util.NewConsistentHashRing(150, nil)
		ring.Add(active...)
		target := append([]string(nil), metadata.Replicas...)
		for _, candidate := range ring.GetN(key, len(active)) {
			if !containsString(target, candidate) {
				target = append(target, candidate)
			}
			if len(target) == definition.ReplicationFactor {
				break
			}
		}
		if len(target) != definition.ReplicationFactor {
			return fmt.Errorf("partition %s could not build target replica set", key)
		}

		command := fsm.ReplicaReassignmentCommand{
			Topic: topicName, Partition: partition,
			LifecycleEpoch: metadata.LifecycleEpoch,
			Leader:         metadata.Leader, LeaderEpoch: metadata.LeaderEpoch,
			ExpectedReplicas: append([]string(nil), metadata.Replicas...),
			TargetReplicas:   target,
		}
		payload, err := json.Marshal(command)
		if err != nil {
			return fmt.Errorf("encode replica reassignment for %s: %w", key, err)
		}
		if err := cc.RaftManager.ApplyCommand("REPLICA_REASSIGN", payload); err != nil {
			return fmt.Errorf("commit replica reassignment for %s: %w", key, err)
		}
		return nil
	}
	return nil
}

func reassignmentCapableBrokers(manager RaftManager) ([]string, error) {
	configuration := manager.GetConfiguration()
	if configuration == nil {
		return nil, fmt.Errorf("raft voter configuration is unavailable")
	}
	if err := configuration.Error(); err != nil {
		return nil, fmt.Errorf("read raft voter configuration: %w", err)
	}
	state := manager.GetFSM()
	active := make([]string, 0)
	for _, server := range configuration.Configuration().Servers {
		if server.Suffrage != raft.Voter {
			continue
		}
		broker := state.GetBroker(string(server.ID))
		if broker == nil || !strings.EqualFold(broker.Status, "active") {
			return nil, fmt.Errorf("raft voter %s is not durably active", server.ID)
		}
		if broker.LifecycleProtocol < fsm.ReplicaReassignmentProtocolVersion {
			return nil, fmt.Errorf(
				"raft voter %s advertises broker protocol %d; replica reassignment requires %d",
				server.ID, broker.LifecycleProtocol, fsm.ReplicaReassignmentProtocolVersion,
			)
		}
		active = append(active, broker.ID)
	}
	sort.Strings(active)
	return active, nil
}

func validateCurrentReplicaAssignment(key string, metadata *fsm.PartitionMetadata) error {
	if metadata.Leader == "" || !containsString(metadata.Replicas, metadata.Leader) {
		return fmt.Errorf("partition %s leader %q is absent from replicas %v", key, metadata.Leader, metadata.Replicas)
	}
	seen := make(map[string]struct{}, len(metadata.Replicas))
	for _, replica := range metadata.Replicas {
		if strings.TrimSpace(replica) == "" {
			return fmt.Errorf("partition %s contains an empty replica ID", key)
		}
		if _, duplicate := seen[replica]; duplicate {
			return fmt.Errorf("partition %s contains duplicate replica %q", key, replica)
		}
		seen[replica] = struct{}{}
	}
	for _, replica := range metadata.ISR {
		if _, configured := seen[replica]; !configured {
			return fmt.Errorf("partition %s ISR member %q is absent from replicas", key, replica)
		}
	}
	return nil
}

func splitPartitionKey(key string) (string, int, error) {
	separator := strings.LastIndexByte(key, '-')
	if separator < 1 || separator+1 >= len(key) {
		return "", 0, fmt.Errorf("invalid partition metadata key %q", key)
	}
	partition, err := strconv.Atoi(key[separator+1:])
	if err != nil || partition < 0 {
		return "", 0, fmt.Errorf("invalid partition metadata key %q", key)
	}
	return key[:separator], partition, nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
