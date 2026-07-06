package controller

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"github.com/cursus-io/cursus/pkg/eventsource"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func (ch *CommandHandler) handleAppendStream(cmd string) string {
	partition, errResp := ch.eventStreamPartition(cmd, "APPEND_STREAM ")
	if errResp != "" {
		return errResp
	}
	if ch.Config != nil && ch.Config.EnabledDistribution && ch.Cluster != nil {
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(eventStreamTopic(cmd, "APPEND_STREAM "), partition, cmd); forwarded {
			return resp
		}
		result, errResp := ch.ESHandler.AppendStream(cmd, eventsource.AppendOptions{
			LeaderAppend: true,
			AfterAppend: func(topic string, partition int, msg types.Message) error {
				msgCmd := types.MessageCommand{
					Topic:         topic,
					Partition:     partition,
					Messages:      []types.Message{msg},
					Acks:          "all",
					SequenceScope: "partition",
				}
				return ch.Cluster.ReplicateToFollowers(topic, partition, msgCmd, ch.Config.MinInSyncReplicas)
			},
		})
		if errResp != "" {
			return errResp
		}
		return result.Response()
	}
	return ch.ESHandler.HandleAppendStream(cmd)
}

func (ch *CommandHandler) handleEventSourceRoutedCommand(cmd, prefix string, local func(string) string) string {
	partition, errResp := ch.eventStreamPartition(cmd, prefix)
	if errResp != "" {
		return errResp
	}
	if ch.Config != nil && ch.Config.EnabledDistribution && ch.Cluster != nil {
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(eventStreamTopic(cmd, prefix), partition, cmd); forwarded {
			return resp
		}
	}
	return local(cmd)
}

func (ch *CommandHandler) HandleReadStreamCommand(conn net.Conn, cmd string) {
	partition, errResp := ch.eventStreamPartition(cmd, "READ_STREAM ")
	if errResp != "" {
		writeReadStreamError(conn, errResp)
		return
	}
	if ch.Config != nil && ch.Config.EnabledDistribution && ch.Cluster != nil && !ch.Cluster.IsAuthorized(eventStreamTopic(cmd, "READ_STREAM "), partition) {
		leaderAddr := ch.resolvePartitionLeaderAddr(eventStreamTopic(cmd, "READ_STREAM "), partition)
		writeReadStreamError(conn, fmt.Sprintf("ERROR: NOT_LEADER LEADER_IS %s", leaderAddr))
		return
	}
	ch.ESHandler.HandleReadStream(cmd, conn)
}

func (ch *CommandHandler) indexReplicatedEventSourceMessages(topic string, partition int, messages []types.Message) error {
	t := ch.TopicManager.GetTopic(topic)
	if t == nil || !t.IsEventSourcing {
		return nil
	}
	return ch.ESHandler.IndexReplicatedMessages(topic, partition, messages)
}

func (ch *CommandHandler) eventStreamPartition(cmd, prefix string) (int, string) {
	topicName := eventStreamTopic(cmd, prefix)
	if topicName == "" {
		return 0, "ERROR: missing_topic"
	}
	key := eventStreamKey(cmd, prefix)
	if key == "" {
		return 0, "ERROR: missing_key"
	}
	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return 0, fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.IsEventSourcing {
		return 0, fmt.Sprintf("ERROR: event_sourcing_not_enabled topic=%s", topicName)
	}
	partition := t.GetPartitionForMessage(types.Message{Key: key})
	if partition < 0 {
		return 0, "ERROR: no_partitions_available"
	}
	return partition, ""
}

func eventStreamTopic(cmd, prefix string) string {
	return parseKeyValueArgs(strings.TrimPrefix(cmd, prefix))["topic"]
}

func eventStreamKey(cmd, prefix string) string {
	return parseKeyValueArgs(strings.TrimPrefix(cmd, prefix))["key"]
}

func writeReadStreamError(conn net.Conn, msg string) {
	code := strings.TrimPrefix(msg, "ERROR: ")
	payload, _ := json.Marshal(map[string]string{"status": "ERROR", "error": code})
	_ = util.WriteWithLength(conn, payload)
}
