package controller

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

// writeConsumerOffsetRecord routes an acknowledged append through the normal
// partition leader path. It is deliberately separate from metadata Raft: the
// replicated __consumer_offsets partition log is the durability boundary.
func (ch *CommandHandler) writeConsumerOffsetRecord(record coordinator.ConsumerMetadataRecord) error {
	if ch.TopicManager == nil {
		return fmt.Errorf("consumer offset topic manager is unavailable")
	}
	payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
	if err != nil {
		return err
	}
	topic := ch.TopicManager.GetTopic(config.ConsumerOffsetsTopicName)
	if topic == nil {
		return fmt.Errorf("consumer offset topic is unavailable")
	}
	// Partition by group identity so lifecycle and offset records share one
	// durable coordinator partition, while retaining their independent keys.
	partitionText, route, err := ch.consumerOffsetRoute(topic, record.Group)
	if err != nil {
		return err
	}
	// Use the resolved route, never recompute the fallback hash after routing.
	physicalPartition, err := strconv.Atoi(partitionText)
	if err != nil || physicalPartition < 0 {
		return fmt.Errorf("invalid consumer metadata route %q", partitionText)
	}
	if ch.Coordinator != nil {
		ch.Coordinator.RecordConsumerMetadataSource(record.Group, physicalPartition)
	}
	cmd := fmt.Sprintf("PUBLISH topic=%s partition=%s acks=all producerId=consumer-offset-coordinator key=%s", config.ConsumerOffsetsTopicName, partitionText, key)
	if route != nil {
		encoded, err := json.Marshal(route)
		if err != nil {
			return err
		}
		cmd += " consumer_route=" + base64.RawURLEncoding.EncodeToString(encoded)
	}
	cmd += " message=" + string(payload)
	resp := ch.handlePublish(cmd, NewInternalClientContext("default-group", 0))
	if strings.HasPrefix(resp, "OK") {
		return nil
	}
	var ack types.AckResponse
	if err := json.Unmarshal([]byte(resp), &ack); err != nil || ack.Status != "OK" {
		return fmt.Errorf("consumer offset append failed: %s", resp)
	}
	return nil
}

func (ch *CommandHandler) consumerOffsetPartition(internalTopic *topic.Topic, groupName string) (string, error) {
	partition, _, err := ch.consumerOffsetRoute(internalTopic, groupName)
	return partition, err
}

func (ch *CommandHandler) consumerOffsetRoute(internalTopic *topic.Topic, groupName string) (string, *types.ConsumerMetadataRoute, error) {
	if ch.hasRouter() {
		routes, _, err := ch.Cluster.RaftManager.GetFSM().ConsumerMetadataRoutes([]string{groupName})
		if err != nil {
			return "", nil, fmt.Errorf("resolve durable consumer offset partition: %w", err)
		}
		route := routes[groupName]
		if route.Owner != ch.Cluster.Router.BrokerID() {
			return "", nil, fmt.Errorf("consumer metadata origin is not the current coordinator")
		}
		return strconv.Itoa(route.Partition), &route, nil
	}
	partition := internalTopic.GetPartitionForMessage(types.Message{Key: coordinator.ConsumerMetadataGroupPartitionKey(groupName)})
	return strconv.Itoa(partition), nil, nil
}
