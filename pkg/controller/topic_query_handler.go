package controller

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/topic"
)

// handleList processes LIST command
func (ch *CommandHandler) handleList() string {
	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isLeaderAndForward("LIST"); forwarded {
			return resp
		}
	}

	tm := ch.TopicManager
	names := tm.ListTopics()
	return fmt.Sprintf("OK count=%d topics=%s", len(names), strings.Join(names, ","))
}

// handleDescribeTopic processes DESCRIBE topic=<name> command
func (ch *CommandHandler) handleDescribeTopic(cmd string) string {
	args := decodeCommandInput(cmd).Args
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic expected=\"DESCRIBE topic=<name>\""
	}

	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}

	type PartitionMetadata struct {
		ID       int      `json:"id"`
		Leader   string   `json:"leader"`
		Replicas []string `json:"replicas"`
		ISR      []string `json:"isr"`
		LEO      uint64   `json:"leo"`
		HWM      uint64   `json:"hwm"`
	}

	type TopicMetadata struct {
		Status     string              `json:"status"`
		Topic      string              `json:"topic"`
		Partitions []PartitionMetadata `json:"partitions"`
		Policy     topic.Policy        `json:"policy"`
	}

	meta := TopicMetadata{
		Status: "OK",
		Topic:  topicName,
		Policy: t.Policy,
	}

	var raftLeader string
	if ch.isDistributed() {
		raftLeader = ch.Cluster.RaftManager.GetLeaderAddress()
	}

	for _, p := range t.Partitions {
		pm := PartitionMetadata{
			ID:  p.ID(),
			LEO: p.NextOffset(),
			HWM: p.GetHWM(),
		}

		if ch.isDistributed() {
			if fsmObj := ch.Cluster.RaftManager.GetFSM(); fsmObj != nil {
				partitionKey := fmt.Sprintf("%s-%d", topicName, p.ID())
				if partMeta := fsmObj.GetPartitionMetadata(partitionKey); partMeta != nil {
					pm.ISR = partMeta.ISR
					pm.Replicas = partMeta.Replicas

					if leaderBroker := fsmObj.GetBroker(partMeta.Leader); leaderBroker != nil {
						pm.Leader = leaderBroker.Addr
					} else {
						// Partition metadata exists but leader broker is unresolvable (failover in progress).
						// Report the raw leader ID rather than silently falling back to the Raft leader.
						pm.Leader = partMeta.Leader
					}
				} else {
					// No partition metadata yet; fall back to Raft leader address.
					pm.Leader = raftLeader
				}
			}
		}

		meta.Partitions = append(meta.Partitions, pm)
	}

	respJSON, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_metadata_failed reason=%q", err.Error())
	}
	return string(respJSON)
}

func (ch *CommandHandler) handleMetadata(cmd string) string {
	args := decodeCommandInput(cmd).Args
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=METADATA"
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}

	partitionCount := len(t.Partitions)
	leaders := make([]string, partitionCount)
	epochs := make([]string, partitionCount)

	for i := 0; i < partitionCount; i++ {
		leaders[i] = ch.resolvePartitionLeaderAddr(topicName, i)
		epochs[i] = "0"
		if ch.isDistributed() {
			if fsmRef := ch.Cluster.RaftManager.GetFSM(); fsmRef != nil {
				key := fmt.Sprintf("%s-%d", topicName, i)
				if meta := fsmRef.GetPartitionMetadata(key); meta != nil {
					epochs[i] = strconv.Itoa(meta.LeaderEpoch)
				}
			}
		}
	}

	return fmt.Sprintf("OK topic=%s partitions=%d leaders=%s epochs=%s cleanup_policy=%s partitioner=%s auth_policy=%s read_acl=%s write_acl=%s retention_hours=%d retention_bytes=%d",
		topicName, partitionCount, strings.Join(leaders, ","), strings.Join(epochs, ","), t.Policy.CleanupPolicy, t.Policy.Partitioner, t.Policy.AuthPolicy, strings.Join(t.Policy.ReadACL, ","), strings.Join(t.Policy.WriteACL, ","), t.Policy.RetentionHours, t.Policy.RetentionBytes)
}
