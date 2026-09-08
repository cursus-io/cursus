package controller

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
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
	msg := types.Message{Payload: string(payload), Key: key}
	partition := topic.GetPartitionForMessage(msg)
	cmd := fmt.Sprintf("PUBLISH topic=%s partition=%d acks=all producerId=consumer-offset-coordinator key=%s message=%s", config.ConsumerOffsetsTopicName, partition, key, payload)
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
