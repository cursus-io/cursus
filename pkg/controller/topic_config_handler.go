package controller

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/topic"
)

func (ch *CommandHandler) topicMinISRMetadata(current *topic.Topic) string {
	configured := "default"
	var currentPolicy topic.Policy
	if current != nil {
		currentPolicy = current.PolicySnapshot()
	}
	if currentPolicy.MinInSyncReplicas != nil {
		configured = strconv.Itoa(*currentPolicy.MinInSyncReplicas)
	}
	brokerDefault := 1
	if ch != nil && ch.Config != nil {
		brokerDefault = ch.Config.MinInSyncReplicas
	}
	effective := topic.DefaultPolicy().EffectiveMinInSyncReplicas(brokerDefault)
	if current != nil {
		effective = currentPolicy.EffectiveMinInSyncReplicas(brokerDefault)
	}
	return fmt.Sprintf("min_in_sync_replicas=%s effective_min_in_sync_replicas=%d", configured, effective)
}

func (ch *CommandHandler) topicReplicationFactor(topicName string) int {
	if !ch.isDistributed() {
		return 1
	}
	if ch.Cluster == nil || ch.Cluster.RaftManager == nil {
		return 0
	}
	fsmRef := ch.Cluster.RaftManager.GetFSM()
	current := ch.TopicManager.GetTopic(topicName)
	if fsmRef == nil || current == nil {
		return 0
	}
	definition := current.Definition()
	replicationFactor := 0
	for partition := 0; partition < definition.Partitions; partition++ {
		metadata := fsmRef.GetPartitionMetadata(fmt.Sprintf("%s-%d", topicName, partition))
		if metadata == nil || len(metadata.Replicas) == 0 {
			return 0
		}
		if replicationFactor == 0 || len(metadata.Replicas) < replicationFactor {
			replicationFactor = len(metadata.Replicas)
		}
	}
	return replicationFactor
}

func (ch *CommandHandler) handleAlterTopicConfig(cmd string, ctx ...*ClientContext) string {
	requestCtx := firstClientContext(ctx).RequestContext()
	args := parseKeyValueArgs(cmd[len("ALTER_TOPIC_CONFIG "):])
	topicName := strings.TrimSpace(args["topic"])
	if topicName == "" {
		return "ERROR: missing_topic command=ALTER_TOPIC_CONFIG"
	}
	if err := topic.ValidateName(topicName); err != nil {
		return fmt.Sprintf("ERROR: invalid_topic_name topic=%q reason=%q", topicName, err.Error())
	}
	rawValue, supplied := args["min_in_sync_replicas"]
	if !supplied || strings.TrimSpace(rawValue) == "" {
		return "ERROR: missing_min_in_sync_replicas command=ALTER_TOPIC_CONFIG"
	}

	reset := strings.EqualFold(strings.TrimSpace(rawValue), "default")
	var value *int
	if !reset {
		parsed, err := strconv.Atoi(rawValue)
		if err != nil || parsed < 1 {
			return fmt.Sprintf("ERROR: invalid_min_in_sync_replicas value=%s", rawValue)
		}
		value = &parsed
	}

	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isLeaderAndForwardContext(requestCtx, cmd); forwarded {
			return resp
		}
	}
	current := ch.TopicManager.GetTopic(topicName)
	if current == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if value != nil {
		replicationFactor := ch.topicReplicationFactor(topicName)
		if replicationFactor < 1 || *value > replicationFactor {
			return fmt.Sprintf("ERROR: invalid_min_in_sync_replicas value=%d replication_factor=%d", *value, replicationFactor)
		}
	}

	if ch.isDistributed() {
		payload := map[string]interface{}{"name": topicName}
		if reset {
			payload["reset_min_in_sync_replicas"] = true
		} else {
			payload["min_in_sync_replicas"] = *value
		}
		if _, err := ch.applyAndWaitContext(requestCtx, "TOPIC_CONFIG", payload); err != nil {
			return fmt.Sprintf("ERROR: alter_topic_config_failed reason=%q", err.Error())
		}
	} else if err := ch.TopicManager.AlterTopicMinInSyncReplicas(topicName, value, 1); err != nil {
		return fmt.Sprintf("ERROR: alter_topic_config_failed reason=%q", err.Error())
	}

	current = ch.TopicManager.GetTopic(topicName)
	return fmt.Sprintf("OK topic=%s %s", topicName, ch.topicMinISRMetadata(current))
}
