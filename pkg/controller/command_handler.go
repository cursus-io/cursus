package controller

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

// todo. consider an LRU cache(match) to prevent potential memory bloat.
var regexCache sync.Map

// handleHelp processes HELP command
func (ch *CommandHandler) handleHelp() string {
	return `Available commands:
CREATE topic=<name> [partitions=<N>] [idempotent=<true|false>] - create topic (default partitions=4, idempotent=false)
DELETE topic=<name> - delete topic
LIST - list all topics
PUBLISH topic=<name> acks=<0|1> message=<text> producerId=<id> [seqNum=<N> epoch=<N>] [isIdempotent=<true|false>] - publish message
CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>] - consume messages
JOIN_GROUP topic=<name> group=<name> member=<id> - join consumer group
SYNC_GROUP topic=<name> group=<name> member=<id> generation=<N> - sync group assignments
LEAVE_GROUP group=<name> member=<id> - leave consumer group
HEARTBEAT topic=<name> group=<name> member=<id> - send heartbeat
COMMIT_OFFSET topic=<name> partition=<N> group=<name> offset=<N> - commit offset
FETCH_OFFSET topic=<name> partition=<N> group=<name> - fetch committed offset
REGISTER_GROUP topic=<name> group=<name> - register consumer group
GROUP_STATUS group=<name> - get group status
DESCRIBE topic=<name> - get topic/partition metadata (leader, ISR, offsets)
HELP - show this help
EXIT - exit`
}

// handleCreate processes CREATE command
func (ch *CommandHandler) handleCreate(cmd string) string {
	args := parseKeyValueArgs(cmd[7:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "missing topic parameter. Expected: CREATE topic=<name> [partitions=<N>]"
	}

	partitions := 4 // default
	if partStr, ok := args["partitions"]; ok {
		n, err := strconv.Atoi(partStr)
		if err != nil || n <= 0 {
			return "partitions must be a positive integer"
		}
		partitions = n
	}

	idempotent := false
	if idempStr, ok := args["idempotent"]; ok {
		idempotent = strings.ToLower(idempStr) == "true"
	}

	tm := ch.TopicManager
	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		payload := map[string]interface{}{
			"name":       topicName,
			"partitions": partitions,
			"idempotent": idempotent,
		}
		_, err := ch.applyAndWait("TOPIC", payload)
		if err != nil {
			return fmt.Sprintf("❌ Failed to create topic: %v", err)
		}
	} else {
		if err := tm.CreateTopic(topicName, partitions, idempotent); err != nil {
			return fmt.Sprintf("ERROR: failed to create topic '%s': %v", topicName, err)
		}
	}

	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic '%s' was not created", topicName)
	}

	if ch.Coordinator != nil {
		err := ch.Coordinator.RegisterGroup(topicName, "default-group", partitions)
		if err != nil {
			util.Warn("Failed to register default group with coordinator: %v", err)
		}
	}
	return fmt.Sprintf("Topic '%s' now has %d partitions", topicName, len(t.Partitions))
}

// handleDelete processes DELETE command
func (ch *CommandHandler) handleDelete(cmd string) string {
	args := parseKeyValueArgs(cmd[7:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "missing topic parameter. Expected: DELETE topic=<name>"
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		payload := map[string]interface{}{
			"topic": topicName,
		}

		_, err := ch.applyAndWait("TOPIC_DELETE", payload)
		if err != nil {
			return fmt.Sprintf("❌ ERROR: %v", err)
		}
		return fmt.Sprintf("🗑️ Topic '%s' deleted across cluster", topicName)
	}

	if ch.TopicManager.DeleteTopic(topicName) {
		return fmt.Sprintf("🗑️ Topic '%s' deleted", topicName)
	}
	return fmt.Sprintf("topic '%s' not found", topicName)
}

// handleList processes LIST command
func (ch *CommandHandler) handleList() string {
	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward("LIST"); forwarded {
			return resp
		}
	}

	tm := ch.TopicManager
	names := tm.ListTopics()
	if len(names) == 0 {
		return "(no topics)"
	}
	return strings.Join(names, ", ")
}

// handleListCluster processes LIST_CLUSTER command
func (ch *CommandHandler) handleListCluster() string {
	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		fsm := ch.Cluster.RaftManager.GetFSM()
		if fsm == nil {
			return "ERROR: FSM not available"
		}

		brokers := fsm.GetBrokers()
		data, err := json.Marshal(brokers)
		if err != nil {
			return fmt.Sprintf("ERROR: failed to marshal brokers: %v", err)
		}
		return string(data)
	}
	return "ERROR: distribution not enabled"
}

// handleRegisterGroup processes REGISTER_GROUP command
func (ch *CommandHandler) handleRegisterGroup(cmd string) string {
	args := parseKeyValueArgs(cmd[15:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "REGISTER_GROUP requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "REGISTER_GROUP requires group parameter"
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		util.Warn("ch registerGroup: topic '%s' does not exist", topicName)
		return fmt.Sprintf("topic '%s' does not exist", topicName)
	}

	if ch.Coordinator != nil {
		if err := ch.Coordinator.RegisterGroup(topicName, groupName, len(t.Partitions)); err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		}
		return fmt.Sprintf("✅ Group '%s' registered for topic '%s'", groupName, topicName)
	}
	return "coordinator not available"
}

// handleJoinGroup processes JOIN_GROUP command
func (ch *CommandHandler) handleJoinGroup(cmd string, ctx *ClientContext) string {
	args := parseKeyValueArgs(cmd[11:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "JOIN_GROUP requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "JOIN_GROUP requires group parameter"
	}
	consumerID, ok := args["member"]
	if !ok || consumerID == "" {
		return "JOIN_GROUP requires member parameter"
	}

	n, err := rand.Int(rand.Reader, big.NewInt(10000))
	var randSuffix string
	if err != nil {
		util.Warn("Failed to generate random consumer suffix, falling back to time-based value: %v", err)
		randSuffix = fmt.Sprintf("%04d", time.Now().UnixNano()%10000)
	} else {
		randSuffix = fmt.Sprintf("%04d", n.Int64())
	}
	consumerID = fmt.Sprintf("%s-%s", consumerID, randSuffix)

	var assignments []int
	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		joinPayload := map[string]interface{}{
			"type":   "JOIN",
			"group":  groupName,
			"member": consumerID,
			"topic":  topicName,
		}

		_, err := ch.applyAndWait("GROUP_SYNC", joinPayload)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		}
		assignments = ch.Coordinator.GetMemberAssignments(groupName, consumerID)
	} else {
		if ch.Coordinator != nil {
			if ch.Coordinator.GetGroup(groupName) == nil {
				topic := ch.TopicManager.GetTopic(topicName)
				if topic == nil {
					return fmt.Sprintf("topic '%s' not found", topicName)
				}

				err := ch.Coordinator.RegisterGroup(topicName, groupName, len(topic.Partitions))
				if err != nil {
					return fmt.Sprintf("failed to register group: %v", err)
				}
			}

			assignments, err = ch.Coordinator.AddConsumer(groupName, consumerID)
			if err != nil {
				util.Error("failed to join %s: %v", groupName, err)
			}
		} else {
			return "coordinator not available"
		}
	}

	ctx.MemberID = consumerID
	ctx.Generation = ch.Coordinator.GetGeneration(groupName)
	util.Info("✅ Joined group '%s' member '%s' generation '%d' with partitions: %v", groupName, ctx.MemberID, ctx.Generation, assignments)
	return fmt.Sprintf("OK generation=%d member=%s assignments=%v", ctx.Generation, ctx.MemberID, assignments)
}

// handleSyncGroup processes SYNC_GROUP command
func (ch *CommandHandler) handleSyncGroup(cmd string) string {
	args := parseKeyValueArgs(cmd[11:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "SYNC_GROUP requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "SYNC_GROUP requires group parameter"
	}
	memberID, ok := args["member"]
	if !ok || memberID == "" {
		return "SYNC_GROUP requires member parameter"
	}

	if ch.Coordinator == nil {
		return "coordinator not available"
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

	assignments := ch.Coordinator.GetMemberAssignments(groupName, memberID)
	if assignments == nil {
		return fmt.Sprintf("member %s not found in group or no assignments", memberID)
	}
	return fmt.Sprintf("OK assignments=%v", assignments)
}

// handleLeaveGroup processes LEAVE_GROUP command
func (ch *CommandHandler) handleLeaveGroup(cmd string) string {
	args := parseKeyValueArgs(cmd[12:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "LEAVE_GROUP requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "LEAVE_GROUP requires group parameter"
	}
	consumerID, ok := args["member"]
	if !ok || consumerID == "" {
		return "LEAVE_GROUP requires member parameter"
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		payload := map[string]interface{}{
			"type":   "LEAVE",
			"group":  groupName,
			"member": consumerID,
		}

		_, err := ch.applyAndWait("GROUP_SYNC", payload)
		if err != nil {
			return fmt.Sprintf("❌ ERROR: %v", err)
		}
	} else {
		if ch.Coordinator != nil {
			if err := ch.Coordinator.RemoveConsumer(groupName, consumerID); err != nil {
				return fmt.Sprintf("ERROR: %v", err)
			}
		}
	}
	return fmt.Sprintf("✅ Left group '%s'", groupName)
}

// handleFetchOffset processes FETCH_OFFSET command
func (ch *CommandHandler) handleFetchOffset(cmd string) string {
	args := parseKeyValueArgs(cmd[13:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "FETCH_OFFSET requires topic parameter"
	}
	partitionStr, ok := args["partition"]
	if !ok || partitionStr == "" {
		return "FETCH_OFFSET requires partition parameter"
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		return "invalid partition"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "FETCH_OFFSET requires group parameter"
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
		if !ch.isAuthorizedForPartition(topicName, partition) {
			return fmt.Sprintf("NOT_AUTHORIZED_FOR_PARTITION %s:%d", topicName, partition)
		}
	}

	if ch.Coordinator == nil {
		return "offset manager not available"
	}

	group := ch.Coordinator.GetGroup(groupName)
	if group == nil {
		return fmt.Sprintf("ERROR group_not_found: %s", groupName)
	}

	if !isTopicMatched(topicName, group.TopicName) {
		return fmt.Sprintf("ERROR topic_not_assigned_to_group: %s %s", group.TopicName, topicName)
	}

	offset, isFind := ch.Coordinator.GetOffset(groupName, topicName, partition)
	if !isFind {
		return "0"
	}

	return fmt.Sprintf("%d", offset)
}

// handleGroupStatus processes GROUP_STATUS command
func (ch *CommandHandler) handleGroupStatus(cmd string) string {
	args := parseKeyValueArgs(cmd[13:])
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "GROUP_STATUS requires group parameter"
	}

	if ch.Coordinator == nil {
		return "coordinator not available"
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

	status, err := ch.Coordinator.GetGroupStatus(groupName)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	statusJSON, err := json.Marshal(status)
	if err != nil {
		return fmt.Sprintf("failed to marshal status: %v", err)
	}
	return string(statusJSON)
}

// handleHeartbeat processes HEARTBEAT command
func (ch *CommandHandler) handleHeartbeat(cmd string) string {
	args := parseKeyValueArgs(cmd[10:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "HEARTBEAT requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "HEARTBEAT requires group parameter"
	}
	consumerID, ok := args["member"]
	if !ok || consumerID == "" {
		return "HEARTBEAT requires member parameter"
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

	if ch.Coordinator != nil {
		err := ch.Coordinator.RecordHeartbeat(groupName, consumerID)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		} else {
			return "OK"
		}
	} else {
		return "coordinator not available"
	}
}

// handleCommitOffset processes COMMIT_OFFSET command
func (ch *CommandHandler) handleCommitOffset(cmd string) string {
	args := parseKeyValueArgs(cmd[14:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "COMMIT_OFFSET requires topic parameter"
	}
	partitionStr, ok := args["partition"]
	if !ok || partitionStr == "" {
		return "COMMIT_OFFSET requires partition parameter"
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		return "invalid partition"
	}
	groupID, ok := args["group"]
	if !ok || groupID == "" {
		return "COMMIT_OFFSET requires groupID parameter"
	}
	offsetStr, ok := args["offset"]
	if !ok || offsetStr == "" {
		return "COMMIT_OFFSET requires offset parameter"
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		return "invalid offset"
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		if !ch.isAuthorizedForPartition(topicName, partition) {
			return fmt.Sprintf("NOT_AUTHORIZED_FOR_PARTITION %s:%d", topicName, partition)
		}

		payload := map[string]interface{}{
			"type":      "COMMIT",
			"group":     groupID,
			"topic":     topicName,
			"partition": partition,
			"offset":    offset,
		}
		_, err := ch.applyAndWait("OFFSET_SYNC", payload)
		if err != nil {
			return fmt.Sprintf("❌ Offset sync failed: %v", err)
		}
		return "OK"
	}

	if ch.Coordinator != nil {
		group := ch.Coordinator.GetGroup(groupID)
		if group != nil && !isTopicMatched(group.TopicName, topicName) {
			return fmt.Sprintf("ERROR topic_not_assigned_to_group: %s %s", group.TopicName, topicName)
		}

		err := ch.Coordinator.CommitOffset(groupID, topicName, partition, offset)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		} else {
			return "OK"
		}
	}
	return "offset manager not available"
}

// handleBatchCommit processes BATCH_COMMIT topic=T1 group=G1 generation=1 member=M1 P0:10,P1:20...
func (ch *CommandHandler) handleBatchCommit(cmd string) string {
	args := parseKeyValueArgs(cmd[13:])

	topicName := args["topic"]
	groupID := args["group"]
	memberID := args["member"]
	generation, _ := strconv.Atoi(args["generation"])

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

	// P0:10,P1:20...
	partsIdx := strings.LastIndex(cmd, " ")
	if partsIdx == -1 {
		return "invalid batch commit format"
	}

	partitionData := cmd[partsIdx+1:]
	partitionPairs := strings.Split(partitionData, ",")

	var offsetList []coordinator.OffsetItem
	for _, pair := range partitionPairs {
		kv := strings.Split(pair, ":")

		if len(kv) != 2 {
			continue
		}

		p, err := strconv.Atoi(kv[0])
		if err != nil {
			util.Warn("Invalid partition in batch commit: %s", kv[0])
			continue
		}
		o, err := strconv.ParseUint(kv[1], 10, 64)
		if err != nil {
			util.Warn("Invalid offset in batch commit: %s", kv[1])
			continue
		}

		if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
			if !ch.isAuthorizedForPartition(topicName, p) {
				util.Warn("Unauthorized batch commit attempt for %s:%d", topicName, p)
				continue
			}
		}

		if !ch.ValidateOwnership(groupID, memberID, generation, p) {
			util.Warn("STALE_METADATA_OR_NOT_OWNER for partition %d in batch", p)
			continue
		}

		offsetList = append(offsetList, coordinator.OffsetItem{Partition: p, Offset: o})
	}

	if len(offsetList) == 0 {
		util.Warn("Batch commit received but no valid offsets parsed from: %s", partitionData)
		return "ERROR: no_valid_offsets"
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		batchCommitData := map[string]interface{}{
			"type":    "BATCH_COMMIT",
			"group":   groupID,
			"topic":   topicName,
			"offsets": offsetList,
		}
		_, err := ch.applyAndWait("BATCH_OFFSET", batchCommitData)
		if err != nil {
			util.Error("❌ Raft batch apply failed: %v", err)
			return fmt.Sprintf("❌ Raft batch apply failed: %v", err)
		}
	} else if ch.Coordinator != nil {
		err := ch.Coordinator.CommitOffsetsBulk(groupID, topicName, offsetList)
		if err != nil {
			return fmt.Sprintf("bulk commit failed: %v", err)
		}
	}

	return fmt.Sprintf("OK batched=%d", len(offsetList))
}

// resolveOffset determines the starting offset for a consumer
func (ch *CommandHandler) resolveOffset(p *topic.Partition, topicName string, cArgs CommonArgs) (uint64, error) {
	if ch.Coordinator != nil {
		savedOffset, isFind := ch.Coordinator.GetOffset(cArgs.GroupName, topicName, cArgs.PartitionID)
		if isFind {
			return savedOffset, nil
		}
	}

	if cArgs.HasOffset {
		util.Debug("Using explicitly requested offset %d", cArgs.Offset)
		return cArgs.Offset, nil
	}

	if cArgs.AutoOffsetReset == "latest" {
		latest := p.GetLatestOffset()
		util.Debug("Reset policy 'latest': starting at %d", latest)
		return latest, nil
	}

	util.Debug("Reset policy 'earliest': starting at 0")
	return 0, nil
}

func (ch *CommandHandler) ValidateOwnership(groupName, memberID string, generation int, partition int) bool {
	if ch.Coordinator == nil {
		util.Debug("failed to validate ownership: Coordinator is nil.")
		return false
	}

	return ch.Coordinator.ValidateOwnershipAtomic(groupName, memberID, generation, partition)
}

func isTopicMatched(pattern, topicName string) bool {
	if pattern == topicName {
		return true
	}
	if strings.Contains(pattern, "*") || strings.Contains(pattern, "?") {
		return match(pattern, topicName)
	}
	return false
}

func match(p, t string) bool {
	if val, ok := regexCache.Load(p); ok {
		if cachedRe, ok := val.(*regexp.Regexp); ok {
			return cachedRe.MatchString(t)
		}
	}

	escaped := regexp.QuoteMeta(p)
	regexPattern := strings.ReplaceAll(escaped, `\*`, ".*")
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, ".")

	newRe, err := regexp.Compile("^" + regexPattern + "$")
	if err != nil {
		util.Error("Regex compile error for pattern %s: %v", p, err)
		return false
	}

	regexCache.Store(p, newRe)
	return newRe.MatchString(t)
}

// handleDescribeTopic processes DESCRIBE topic=<name> command
func (ch *CommandHandler) handleDescribeTopic(cmd string) string {
	args := parseKeyValueArgs(cmd[9:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "missing topic parameter. Expected: DESCRIBE topic=<name>"
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("topic '%s' not found", topicName)
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
		Topic      string              `json:"topic"`
		Partitions []PartitionMetadata `json:"partitions"`
	}

	meta := TopicMetadata{
		Topic: topicName,
	}

	var raftLeader string
	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		raftLeader = ch.Cluster.RaftManager.GetLeaderAddress()
	}

	for _, p := range t.Partitions {
		pm := PartitionMetadata{
			ID:  p.ID(),
			LEO: p.NextOffset(),
			HWM: p.HWM,
		}

		if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
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
		return fmt.Sprintf("failed to marshal metadata: %v", err)
	}
	return string(respJSON)
}
