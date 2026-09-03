package controller

import (
	"fmt"

	replicationFSM "github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
)

func (ch *CommandHandler) distributedCompactionAllowed(topicName string, partitionID int) (bool, string) {
	if ch == nil || ch.TopicManager == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil {
		return false, "cluster_metadata_unavailable"
	}
	fsmRef := ch.Cluster.RaftManager.GetFSM()
	if fsmRef == nil {
		return false, "cluster_metadata_unavailable"
	}
	metadata := fsmRef.GetPartitionMetadata(fmt.Sprintf("%s-%d", topicName, partitionID))
	if metadata == nil || !metadata.CommittedHWMKnown {
		return false, "partition_metadata_unavailable"
	}
	definition, found := fsmRef.GetTopicDefinition(topicName)
	if !found {
		return false, "topic_definition_unavailable"
	}
	if definition.LifecycleEpoch != metadata.LifecycleEpoch {
		return false, "topic_lifecycle_mismatch"
	}
	if !config.HasCleanupPolicy(definition.Policy.CleanupPolicy, config.CleanupPolicyCompact) {
		return false, "topic_policy_mismatch"
	}
	localBroker := ch.Cluster.BrokerID()
	if localBroker == "" || !containsReplica(metadata.Replicas, localBroker) || !containsReplica(metadata.ISR, localBroker) {
		return false, "local_replica_not_in_sync"
	}
	if len(metadata.ISR) != len(metadata.Replicas) {
		return false, "replica_not_caught_up"
	}
	for _, replica := range metadata.Replicas {
		if !containsReplica(metadata.ISR, replica) {
			return false, "replica_not_caught_up"
		}
	}
	localTopic := ch.TopicManager.GetTopic(topicName)
	if localTopic == nil || localTopic.LifecycleEpoch != metadata.LifecycleEpoch ||
		localTopic.LifecycleEpoch != definition.LifecycleEpoch {
		return false, "topic_lifecycle_mismatch"
	}
	if !config.HasCleanupPolicy(localTopic.PolicySnapshot().CleanupPolicy, config.CleanupPolicyCompact) {
		return false, "topic_policy_mismatch"
	}
	partition, err := localTopic.GetPartition(partitionID)
	if err != nil {
		return false, "partition_metadata_unavailable"
	}
	if partition.NextOffset() != metadata.CommittedHWM || partition.GetHWM() != metadata.CommittedHWM {
		return false, "replica_not_caught_up"
	}

	brokers := fsmRef.GetBrokers()
	byID := make(map[string]replicationFSM.BrokerInfo, len(brokers))
	for _, broker := range brokers {
		byID[broker.ID] = broker
	}
	for _, replica := range metadata.Replicas {
		broker, ok := byID[replica]
		if !ok || broker.Status != "active" {
			return false, "replica_not_active"
		}
		if broker.LifecycleProtocol < replicationFSM.DistributedCompactionProtocolVersion {
			return false, "mixed_broker_protocol"
		}
	}
	return true, ""
}
