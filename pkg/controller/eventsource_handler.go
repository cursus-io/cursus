package controller

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
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

func (ch *CommandHandler) handleSaveSnapshot(cmd string) string {
	partition, errResp := ch.eventStreamPartition(cmd, "SAVE_SNAPSHOT ")
	if errResp != "" {
		return errResp
	}
	if ch.Config != nil && ch.Config.EnabledDistribution && ch.Cluster != nil {
		topicName := eventStreamTopic(cmd, "SAVE_SNAPSHOT ")
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(topicName, partition, cmd); forwarded {
			return resp
		}
		result, errResp := ch.ESHandler.SaveSnapshot(cmd, func(result eventsource.SnapshotResult) error {
			payload, err := json.Marshal(result)
			if err != nil {
				return err
			}
			replicateCmd := fmt.Sprintf("REPLICATE_SNAPSHOT %spayload=%s", ch.internalAuthPrefix(), string(payload))
			return ch.Cluster.ReplicateCommandToFollowers(result.Topic, result.Partition, replicateCmd, ch.Config.MinInSyncReplicas)
		})
		if errResp != "" {
			return errResp
		}
		return result.Response()
	}
	return ch.ESHandler.HandleSaveSnapshot(cmd)
}

func (ch *CommandHandler) handleListSnapshots(cmd string) string {
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, "LIST_SNAPSHOTS "))
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=LIST_SNAPSHOTS"
	}
	partition, err := strconv.Atoi(args["partition"])
	if err != nil {
		return "ERROR: invalid_partition"
	}
	snaps, errResp := ch.ESHandler.ListSnapshots(topicName, partition)
	if errResp != "" {
		return errResp
	}
	payload, err := json.Marshal(snaps)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_snapshots_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK snapshots=%s", string(payload))
}

func (ch *CommandHandler) handleFetchSnapshot(cmd string) string {
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, "FETCH_SNAPSHOT "))
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=FETCH_SNAPSHOT"
	}
	key := args["key"]
	if key == "" {
		return "ERROR: missing_key command=FETCH_SNAPSHOT"
	}
	partition, err := strconv.Atoi(args["partition"])
	if err != nil {
		return "ERROR: invalid_partition"
	}
	snap, errResp := ch.ESHandler.FetchSnapshot(topicName, partition, key)
	if errResp != "" {
		return errResp
	}
	if snap == nil {
		return "OK snapshot=null"
	}
	payload, err := json.Marshal(snap)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_snapshot_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK snapshot=%s", string(payload))
}

func (ch *CommandHandler) handleCatchupSnapshots(cmd string) string {
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, "CATCHUP_SNAPSHOTS "))
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=CATCHUP_SNAPSHOTS"
	}
	partition, err := strconv.Atoi(args["partition"])
	if err != nil {
		return "ERROR: invalid_partition"
	}
	leaderAddr := args["leader"]
	if leaderAddr == "" {
		leaderAddr = ch.resolvePartitionLeaderAddr(topicName, partition)
	}
	if leaderAddr == "" {
		return "ERROR: leader_not_found"
	}
	if ch.Cluster == nil {
		return "ERROR: cluster_not_available"
	}
	resp, err := ch.Cluster.ForwardCommandToBroker(leaderAddr, fmt.Sprintf("LIST_SNAPSHOTS %stopic=%s partition=%d", ch.internalAuthPrefix(), topicName, partition))
	if err != nil {
		return fmt.Sprintf("ERROR: snapshot_catchup_failed reason=%q", err.Error())
	}
	if strings.HasPrefix(resp, "ERROR:") {
		return resp
	}
	payload := strings.TrimPrefix(resp, "OK snapshots=")
	if payload == resp {
		return fmt.Sprintf("ERROR: invalid_snapshot_catchup_response response=%q", resp)
	}
	var snaps []eventsource.SnapshotResult
	if err := json.Unmarshal([]byte(payload), &snaps); err != nil {
		return fmt.Sprintf("ERROR: unmarshal_failed reason=%q", err.Error())
	}
	applied := 0
	for _, snap := range snaps {
		if snap.Topic == "" {
			snap.Topic = topicName
		}
		if snap.Partition == 0 {
			snap.Partition = partition
		}
		if errResp := ch.ESHandler.SaveSnapshotReplica(snap); errResp != "" {
			return errResp
		}
		applied++
	}
	return fmt.Sprintf("OK snapshots=%d", applied)
}

func (ch *CommandHandler) handleReplicateSnapshot(cmd string) string {
	idx := strings.Index(cmd, "payload=")
	if idx == -1 {
		return "ERROR: missing_payload command=REPLICATE_SNAPSHOT"
	}
	payload := cmd[idx+8:]
	var snap eventsource.SnapshotResult
	if err := json.Unmarshal([]byte(payload), &snap); err != nil {
		return fmt.Sprintf("ERROR: unmarshal_failed reason=%q", err.Error())
	}
	if snap.Topic == "" || snap.Key == "" {
		return "ERROR: invalid_snapshot_payload"
	}
	if errResp := ch.ESHandler.SaveSnapshotReplica(snap); errResp != "" {
		return errResp
	}
	return "OK"
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
