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
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

var (
	regexMu      sync.RWMutex
	regexCache   = make(map[string]*regexp.Regexp)
	maxCacheSize = 1024
)

// handleHelp processes HELP command
func (ch *CommandHandler) handleHelp() string {
	commands := []string{
		"CREATE", "DELETE", "LIST", "PUBLISH", "CONSUME", "STREAM", "JOIN_GROUP", "SYNC_GROUP",
		"LEAVE_GROUP", "HEARTBEAT", "COMMIT_OFFSET", "BATCH_COMMIT", "FETCH_OFFSET", "LIST_OFFSETS", "INIT_PRODUCER_ID", "BEGIN_TXN", "TXN_PUBLISH", "SEND_OFFSETS_TO_TXN", "END_TXN", "TXN_STATUS", "REGISTER_GROUP",
		"GROUP_STATUS", "DESCRIBE", "APPEND_STREAM", "READ_STREAM", "SAVE_SNAPSHOT",
		"READ_SNAPSHOT", "STREAM_VERSION", "METADATA", "FIND_COORDINATOR", "HELP", "EXIT",
	}
	return fmt.Sprintf("OK commands=%s", strings.Join(commands, ","))
}

// handleCreate processes CREATE command
func (ch *CommandHandler) handleCreate(cmd string) string {
	args := parseKeyValueArgs(cmd[7:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic expected=\"CREATE topic=<name> [partitions=<N>]\""
	}

	partitions := 4 // default
	if partStr, ok := args["partitions"]; ok {
		n, err := strconv.Atoi(partStr)
		if err != nil || n <= 0 {
			return "ERROR: invalid_partitions reason=\"must be a positive integer\""
		}
		partitions = n
	}

	idempotent := false
	if idempStr, ok := args["idempotent"]; ok {
		idempotent = strings.ToLower(idempStr) == "true"
	}

	eventSourcing := false
	if esStr, ok := args["event_sourcing"]; ok {
		eventSourcing = strings.ToLower(esStr) == "true"
	}

	policy, policyErr := parseTopicPolicy(args)
	if policyErr != "" {
		return policyErr
	}

	replicationFactor := ch.Config.DefaultReplicationFactor
	if rfStr, ok := args["replication_factor"]; ok {
		n, err := strconv.Atoi(rfStr)
		if err != nil || n <= 0 {
			return "ERROR: invalid_replication_factor reason=\"must be a positive integer\""
		}
		replicationFactor = n
	}

	tm := ch.TopicManager
	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		payload := map[string]interface{}{
			"name":               topicName,
			"partitions":         partitions,
			"idempotent":         idempotent,
			"event_sourcing":     eventSourcing,
			"replication_factor": replicationFactor,
			"policy":             policy,
		}
		_, err := ch.applyAndWait("TOPIC", payload)
		if err != nil {
			return fmt.Sprintf("ERROR: create_topic_failed reason=%q", err.Error())
		}
	} else {
		if err := tm.CreateTopicWithPolicy(topicName, partitions, idempotent, eventSourcing, policy); err != nil {
			return fmt.Sprintf("ERROR: create_topic_failed topic=%s reason=%q", topicName, err.Error())
		}
	}

	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_create_missing topic=%s", topicName)
	}

	if ch.Coordinator != nil {
		err := ch.Coordinator.RegisterGroup(topicName, "default-group", partitions)
		if err != nil {
			util.Warn("Failed to register default group with coordinator: %v", err)
		}
	}
	return fmt.Sprintf("OK topic=%s partitions=%d partitioner=%s auth_policy=%s read_acl=%s write_acl=%s retention_hours=%d retention_bytes=%d", topicName, len(t.Partitions), t.Policy.Partitioner, t.Policy.AuthPolicy, strings.Join(t.Policy.ReadACL, ","), strings.Join(t.Policy.WriteACL, ","), t.Policy.RetentionHours, t.Policy.RetentionBytes)
}

func parseTopicPolicy(args map[string]string) (topic.Policy, string) {
	policy := topic.DefaultPolicy()
	if v := args["retention_hours"]; v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return policy, fmt.Sprintf("ERROR: invalid_retention_hours value=%s", v)
		}
		policy.RetentionHours = parsed
	}
	if v := args["retention_bytes"]; v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return policy, fmt.Sprintf("ERROR: invalid_retention_bytes value=%s", v)
		}
		policy.RetentionBytes = parsed
	}
	if v := args["partitioner"]; v != "" {
		policy.Partitioner = v
	}
	if v := args["auth_policy"]; v != "" {
		policy.AuthPolicy = v
	}
	policy.ReadACL = parseACLArg(args["read_acl"])
	policy.WriteACL = parseACLArg(args["write_acl"])
	policy, err := policy.Normalize()
	if err != nil {
		return policy, fmt.Sprintf("ERROR: invalid_topic_policy reason=%q", err.Error())
	}
	return policy, ""
}

func parseACLArg(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	acl := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			acl = append(acl, part)
		}
	}
	return acl
}

// handleDelete processes DELETE command
func (ch *CommandHandler) handleDelete(cmd string) string {
	args := parseKeyValueArgs(cmd[7:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic expected=\"DELETE topic=<name>\""
	}

	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		if ch.ESHandler != nil {
			if err := ch.ESHandler.DeleteTopic(topicName); err != nil {
				util.Warn("Failed to close event sourcing metadata for deleted topic %s: %v", topicName, err)
			}
		}

		payload := map[string]interface{}{
			"topic": topicName,
		}

		_, err := ch.applyAndWait("TOPIC_DELETE", payload)
		if err != nil {
			return fmt.Sprintf("ERROR: delete_topic_failed reason=%q", err.Error())
		}
		return fmt.Sprintf("OK topic=%s deleted=true", topicName)
	}

	if ch.ESHandler != nil {
		if err := ch.ESHandler.DeleteTopic(topicName); err != nil {
			util.Warn("Failed to close event sourcing metadata for deleted topic %s: %v", topicName, err)
		}
	}
	if ch.TopicManager.DeleteTopic(topicName) {
		return fmt.Sprintf("OK topic=%s deleted=true", topicName)
	}
	return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
}

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

// handleListCluster processes LIST_CLUSTER command
func (ch *CommandHandler) handleListCluster() string {
	if ch.isDistributed() {
		fsm := ch.Cluster.RaftManager.GetFSM()
		if fsm == nil {
			return "ERROR: fsm_not_available"
		}

		brokers := fsm.GetBrokers()
		data, err := json.Marshal(brokers)
		if err != nil {
			return fmt.Sprintf("ERROR: marshal_brokers_failed reason=%q", err.Error())
		}
		return fmt.Sprintf("OK brokers=%s", string(data))
	}
	return "ERROR: distribution_not_enabled"
}

// handleRegisterGroup processes REGISTER_GROUP command
func (ch *CommandHandler) handleRegisterGroup(cmd string) string {
	args := parseKeyValueArgs(cmd[15:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=REGISTER_GROUP"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: missing_group command=REGISTER_GROUP"
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		util.Warn("ch registerGroup: topic '%s' does not exist", topicName)
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}

	if ch.Coordinator != nil {
		if err := ch.Coordinator.RegisterGroup(topicName, groupName, len(t.Partitions)); err != nil {
			return fmt.Sprintf("ERROR: register_group_failed reason=%q", err.Error())
		}
		return fmt.Sprintf("OK group=%s topic=%s registered=true", groupName, topicName)
	}
	return "ERROR: coordinator_not_available"
}

// handleJoinGroup processes JOIN_GROUP command
func (ch *CommandHandler) handleJoinGroup(cmd string, ctx *ClientContext) string {
	args := parseKeyValueArgs(cmd[11:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=JOIN_GROUP"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: missing_group command=JOIN_GROUP"
	}
	consumerID, ok := args["member"]
	if !ok || consumerID == "" {
		return "ERROR: missing_member command=JOIN_GROUP"
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
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}

		joinPayload := map[string]interface{}{
			"type":   "JOIN",
			"group":  groupName,
			"member": consumerID,
			"topic":  topicName,
		}

		_, err := ch.applyViaLeader("GROUP_SYNC", joinPayload)
		if err != nil {
			return fmt.Sprintf("ERROR: register_group_failed reason=%q", err.Error())
		}

		// Wait briefly for Raft to propagate to local FSM
		for i := 0; i < 10; i++ {
			assignments = ch.Coordinator.GetMemberAssignments(groupName, consumerID)
			if len(assignments) > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	} else {
		if ch.Coordinator != nil {
			if ch.Coordinator.GetGroup(groupName) == nil {
				topic := ch.TopicManager.GetTopic(topicName)
				if topic == nil {
					return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
				}

				err := ch.Coordinator.RegisterGroup(topicName, groupName, len(topic.Partitions))
				if err != nil {
					return fmt.Sprintf("ERROR: register_group_failed reason=%q", err.Error())
				}
			}

			assignments, err = ch.Coordinator.AddConsumer(groupName, consumerID)
			if err != nil {
				util.Error("failed to join %s: %v", groupName, err)
			}
		} else {
			return "ERROR: coordinator_not_available"
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
		return "ERROR: missing_topic command=SYNC_GROUP"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: missing_group command=SYNC_GROUP"
	}
	memberID, ok := args["member"]
	if !ok || memberID == "" {
		return "ERROR: missing_member command=SYNC_GROUP"
	}

	if ch.Coordinator == nil {
		return "ERROR: coordinator_not_available"
	}

	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}

	assignments := ch.Coordinator.GetMemberAssignments(groupName, memberID)
	if assignments == nil {
		return fmt.Sprintf("ERROR: member_not_found member=%s group=%s", memberID, groupName)
	}
	return fmt.Sprintf("OK assignments=%v", assignments)
}

// handleLeaveGroup processes LEAVE_GROUP command
func (ch *CommandHandler) handleLeaveGroup(cmd string) string {
	args := parseKeyValueArgs(cmd[12:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=LEAVE_GROUP"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: missing_group command=LEAVE_GROUP"
	}
	consumerID, ok := args["member"]
	if !ok || consumerID == "" {
		return "ERROR: missing_member command=LEAVE_GROUP"
	}

	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}

		payload := map[string]interface{}{
			"type":   "LEAVE",
			"group":  groupName,
			"member": consumerID,
		}

		_, err := ch.applyViaLeader("GROUP_SYNC", payload)
		if err != nil {
			return fmt.Sprintf("ERROR: register_group_failed reason=%q", err.Error())
		}
	} else {
		if ch.Coordinator != nil {
			if err := ch.Coordinator.RemoveConsumer(groupName, consumerID); err != nil {
				return fmt.Sprintf("ERROR: register_group_failed reason=%q", err.Error())
			}
		}
	}
	return fmt.Sprintf("OK group=%s member=%s left=true", groupName, consumerID)
}

// handleListOffsets processes LIST_OFFSETS topic=<name> [partition=<N>].
func (ch *CommandHandler) handleListOffsets(cmd string) string {
	argsText := ""
	if len(cmd) > len("LIST_OFFSETS") {
		argsText = strings.TrimSpace(cmd[len("LIST_OFFSETS"):])
	}
	args := parseKeyValueArgs(argsText)
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=LIST_OFFSETS"
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}

	format := func(p *topic.Partition) string {
		r := p.OffsetRange()
		return fmt.Sprintf("P%d:earliest=%d:latest=%d:leo=%d:hwm=%d", p.ID(), r.Earliest, r.Latest, r.LEO, r.HWM)
	}

	entries := make([]string, 0, len(t.Partitions))
	if partitionStr := args["partition"]; partitionStr != "" {
		partition, err := strconv.Atoi(partitionStr)
		if err != nil {
			return "ERROR: invalid_partition command=LIST_OFFSETS"
		}
		p, err := t.GetPartition(partition)
		if err != nil {
			return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
		}
		entries = append(entries, format(p))
	} else {
		for _, p := range t.Partitions {
			entries = append(entries, format(p))
		}
	}

	return fmt.Sprintf("OK topic=%s partitions=%d offsets=%s", topicName, len(entries), strings.Join(entries, ","))
}

// handleFetchOffset processes FETCH_OFFSET command
func (ch *CommandHandler) handleFetchOffset(cmd string) string {
	args := parseKeyValueArgs(cmd[13:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=FETCH_OFFSET"
	}
	partitionStr, ok := args["partition"]
	if !ok || partitionStr == "" {
		return "ERROR: missing_partition command=FETCH_OFFSET"
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		return "ERROR: invalid_partition"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: missing_group command=FETCH_OFFSET"
	}

	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}

	if ch.Coordinator == nil {
		return "ERROR: offset_manager_not_available"
	}

	group := ch.Coordinator.GetGroup(groupName)
	if group == nil {
		return fmt.Sprintf("ERROR: group_not_found group=%s", groupName)
	}

	offsetTopic, ok := resolveOffsetTopic(group.TopicName, topicName)
	if !ok {
		return fmt.Sprintf("ERROR: topic_not_assigned_to_group expected=%s actual=%s", group.TopicName, topicName)
	}

	offset, isFind := ch.Coordinator.GetOffset(groupName, offsetTopic, partition)
	if !isFind {
		return "OK offset=0"
	}

	return fmt.Sprintf("OK offset=%d", offset)
}

// handleGroupStatus processes GROUP_STATUS command
func (ch *CommandHandler) handleGroupStatus(cmd string) string {
	args := parseKeyValueArgs(cmd[13:])
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: missing_group command=GROUP_STATUS"
	}

	if ch.Coordinator == nil {
		return "ERROR: coordinator_not_available"
	}

	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}

	status, err := ch.Coordinator.GetGroupStatus(groupName)
	if err != nil {
		return fmt.Sprintf("ERROR: group_status_failed reason=%q", err.Error())
	}

	status.Status = "OK"

	statusJSON, err := json.Marshal(status)
	if err != nil {
		return fmt.Sprintf("ERROR: marshal_status_failed reason=%q", err.Error())
	}
	return string(statusJSON)
}

// handleHeartbeat processes HEARTBEAT command
func (ch *CommandHandler) handleHeartbeat(cmd string) string {
	args := parseKeyValueArgs(cmd[10:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=HEARTBEAT"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: missing_group command=HEARTBEAT"
	}
	consumerID, ok := args["member"]
	if !ok || consumerID == "" {
		return "ERROR: missing_member command=HEARTBEAT"
	}

	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupName)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}

	if ch.Coordinator == nil {
		return "ERROR: coordinator_not_available"
	}

	generation := -1
	if genStr := args["generation"]; genStr != "" {
		parsed, parseErr := strconv.Atoi(genStr)
		if parseErr != nil {
			return "ERROR: invalid_generation command=HEARTBEAT"
		}
		generation = parsed
	}
	if errResp := ch.Coordinator.ValidateMemberGeneration(groupName, consumerID, generation); errResp != "" {
		return errResp
	}
	if err := ch.Coordinator.RecordHeartbeat(groupName, consumerID); err != nil {
		return formatCoordinatorError(err)
	}
	return fmt.Sprintf("OK member=%s generation=%d", consumerID, ch.Coordinator.GetGeneration(groupName))
}

// handleCommitOffset processes COMMIT_OFFSET command
func (ch *CommandHandler) handleCommitOffset(cmd string) string {
	args := parseKeyValueArgs(cmd[14:])
	validateOnly := strings.EqualFold(args["validate_only"], "true")
	ownershipOnly := strings.EqualFold(args["ownership_only"], "true")

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=COMMIT_OFFSET"
	}
	partitionStr, ok := args["partition"]
	if !ok || partitionStr == "" {
		return "ERROR: missing_partition command=COMMIT_OFFSET"
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		return "ERROR: invalid_partition"
	}
	groupID, ok := args["group"]
	if !ok || groupID == "" {
		return "ERROR: missing_group command=COMMIT_OFFSET"
	}
	offsetStr, ok := args["offset"]
	if !ok || offsetStr == "" {
		return "ERROR: missing_offset command=COMMIT_OFFSET"
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		return "ERROR: invalid_offset"
	}
	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupID)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}
	offsetTopic, offsetTopicErr := ch.resolveGroupOffsetTopic(groupID, topicName)
	if offsetTopicErr != "" {
		return offsetTopicErr
	}
	if ch.Coordinator == nil {
		return "ERROR: offset_manager_not_available"
	}
	if memberID := args["member"]; memberID != "" || args["generation"] != "" {
		generation, genErr := strconv.Atoi(args["generation"])
		if genErr != nil {
			return "ERROR: invalid_generation command=COMMIT_OFFSET"
		}
		if errResp := ch.Coordinator.ValidateOwnershipFailure(groupID, memberID, generation, partition); errResp != "" {
			return errResp
		}
	}
	if validateOnly && ownershipOnly {
		return "OK validated=true"
	}
	if current, ok := ch.Coordinator.GetOffset(groupID, offsetTopic, partition); ok && offset < current {
		return formatCoordinatorError(fmt.Errorf("offset regression group=%s topic=%s partition=%d current=%d got=%d", groupID, offsetTopic, partition, current, offset))
	}
	if validateOnly {
		return "OK validated=true"
	}

	if ch.isDistributed() {
		payload := map[string]interface{}{
			"type":      "COMMIT",
			"group":     groupID,
			"topic":     offsetTopic,
			"partition": partition,
			"offset":    offset,
		}
		_, err := ch.applyViaLeader("OFFSET_SYNC", payload)
		if err != nil {
			return fmt.Sprintf("ERROR: offset_sync_failed reason=%q", err.Error())
		}
		return "OK"
	}

	err = ch.Coordinator.CommitOffset(groupID, offsetTopic, partition, offset)
	if err != nil {
		return formatCoordinatorError(err)
	}
	ch.recordConsumerLag(offsetTopic, partition, offset, groupID)
	return "OK"
}

// handleBatchCommit processes BATCH_COMMIT topic=T1 group=G1 generation=1 member=M1 P0:10,P1:20...
func (ch *CommandHandler) handleBatchCommit(cmd string) string {
	args := parseKeyValueArgs(cmd[13:])

	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=BATCH_COMMIT"
	}
	groupID := args["group"]
	if groupID == "" {
		return "ERROR: missing_group command=BATCH_COMMIT"
	}
	memberID := args["member"]
	if memberID == "" {
		return "ERROR: missing_member command=BATCH_COMMIT"
	}
	generation, genErr := strconv.Atoi(args["generation"])
	if genErr != nil {
		return "ERROR: invalid_generation command=BATCH_COMMIT"
	}
	offsetTopic, offsetTopicErr := ch.resolveGroupOffsetTopic(groupID, topicName)
	if offsetTopicErr != "" {
		return offsetTopicErr
	}
	if ch.Coordinator == nil {
		return "ERROR: offset_manager_not_available"
	}

	if ch.isDistributed() {
		coordAddr, isCoord := ch.checkCoordinator(groupID)
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}

	partsIdx := strings.LastIndex(cmd, " ")
	if partsIdx == -1 {
		return "ERROR: invalid_batch_commit_format"
	}

	partitionData := cmd[partsIdx+1:]
	partitionPairs := strings.Split(partitionData, ",")

	var offsetList []coordinator.OffsetItem
	for _, pair := range partitionPairs {
		kv := strings.Split(pair, ":")
		if len(kv) != 2 {
			continue
		}
		if !strings.HasPrefix(kv[0], "P") {
			util.Warn("Invalid partition in batch commit: missing P prefix: %s", kv[0])
			continue
		}
		partStr := strings.TrimPrefix(kv[0], "P")
		p, err := strconv.Atoi(partStr)
		if err != nil {
			util.Warn("Invalid partition in batch commit: %s", kv[0])
			continue
		}
		o, err := strconv.ParseUint(kv[1], 10, 64)
		if err != nil {
			util.Warn("Invalid offset in batch commit: %s", kv[1])
			continue
		}
		if errResp := ch.ValidateOwnershipFailure(groupID, memberID, generation, p); errResp != "" {
			util.Warn("Batch commit ownership rejected for partition %d: %s", p, errResp)
			return errResp
		}
		offsetList = append(offsetList, coordinator.OffsetItem{Partition: p, Offset: o})
	}

	if len(offsetList) == 0 {
		util.Warn("Batch commit received but no valid offsets parsed from: %s", partitionData)
		return "ERROR: no_valid_offsets"
	}

	if ch.isDistributed() {
		batchCommitData := map[string]interface{}{
			"type":    "BATCH_COMMIT",
			"group":   groupID,
			"topic":   offsetTopic,
			"offsets": offsetList,
		}
		_, err := ch.applyViaLeader("BATCH_OFFSET", batchCommitData)
		if err != nil {
			util.Error("Raft batch apply failed: %v", err)
			return fmt.Sprintf("ERROR: raft_batch_apply_failed reason=%q", err.Error())
		}
	} else if ch.Coordinator != nil {
		err := ch.Coordinator.CommitOffsetsBulk(groupID, offsetTopic, offsetList)
		if err != nil {
			return formatCoordinatorError(err)
		}
	} else {
		return "ERROR: offset_manager_not_available"
	}

	for _, item := range offsetList {
		ch.recordConsumerLag(offsetTopic, item.Partition, item.Offset, groupID)
	}

	return fmt.Sprintf("OK batched=%d", len(offsetList))
}
func (ch *CommandHandler) resolveGroupOffsetTopic(groupName, topicName string) (string, string) {
	if ch.Coordinator == nil {
		return topicName, ""
	}
	group := ch.Coordinator.GetGroup(groupName)
	if group == nil {
		return topicName, ""
	}
	offsetTopic, ok := resolveOffsetTopic(group.TopicName, topicName)
	if !ok {
		return "", fmt.Sprintf("ERROR: topic_not_assigned_to_group expected=%s actual=%s", group.TopicName, topicName)
	}
	return offsetTopic, ""
}

func resolveOffsetTopic(groupTopic, requestedTopic string) (string, bool) {
	if groupTopic == requestedTopic {
		return requestedTopic, true
	}
	if isTopicMatched(groupTopic, requestedTopic) {
		return requestedTopic, true
	}
	if isTopicMatched(requestedTopic, groupTopic) {
		return groupTopic, true
	}
	return "", false
}

func formatCoordinatorError(err error) string {
	if err == nil {
		return "OK"
	}
	msg := err.Error()
	if strings.HasPrefix(msg, "ERROR:") {
		return msg
	}
	if strings.Contains(msg, "offset regression") {
		return fmt.Sprintf("ERROR: offset_regression reason=%q", msg)
	}
	if strings.Contains(msg, "not found") {
		return fmt.Sprintf("ERROR: group_not_found reason=%q", msg)
	}
	return fmt.Sprintf("ERROR: coordinator_error reason=%q", msg)
}
func (ch *CommandHandler) recordConsumerLag(topicName string, partition int, committedOffset uint64, groupID string) {
	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return
	}
	p, err := t.GetPartition(partition)
	if err != nil {
		return
	}
	leo := p.NextOffset()
	lag := float64(0)
	if leo > committedOffset {
		lag = float64(leo - committedOffset)
	}
	metrics.ConsumerLag.WithLabelValues(topicName, fmt.Sprintf("%d", partition), groupID).Set(lag)
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
		latest := p.OffsetRange().Latest
		util.Debug("Reset policy 'latest': starting at %d", latest)
		return latest, nil
	}

	util.Debug("Reset policy 'earliest': starting at 0")
	return 0, nil
}

func (ch *CommandHandler) ValidateOwnership(groupName, memberID string, generation int, partition int) bool {
	return ch.ValidateOwnershipFailure(groupName, memberID, generation, partition) == ""
}

func (ch *CommandHandler) ValidateOwnershipFailure(groupName, memberID string, generation int, partition int) string {
	if ch.Coordinator == nil {
		util.Debug("failed to validate ownership: Coordinator is nil.")
		return "ERROR: coordinator_not_available"
	}
	return ch.Coordinator.ValidateOwnershipFailure(groupName, memberID, generation, partition)
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
	regexMu.RLock()
	cachedRe, ok := regexCache[p]
	regexMu.RUnlock()

	if ok {
		return cachedRe.MatchString(t)
	}

	escaped := regexp.QuoteMeta(p)
	regexPattern := strings.ReplaceAll(escaped, `\*`, ".*")
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, ".")

	newRe, err := regexp.Compile("^" + regexPattern + "$")
	if err != nil {
		util.Error("Regex compile error for pattern %s: %v", p, err)
		return false
	}

	regexMu.Lock()
	if len(regexCache) >= maxCacheSize {
		regexCache = make(map[string]*regexp.Regexp)
	}
	regexCache[p] = newRe
	regexMu.Unlock()

	return newRe.MatchString(t)
}

// handleDescribeTopic processes DESCRIBE topic=<name> command
func (ch *CommandHandler) handleDescribeTopic(cmd string) string {
	args := parseKeyValueArgs(cmd[9:])
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
	args := parseKeyValueArgs(cmd[9:]) // len("METADATA ") = 9
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

	return fmt.Sprintf("OK topic=%s partitions=%d leaders=%s epochs=%s partitioner=%s auth_policy=%s read_acl=%s write_acl=%s retention_hours=%d retention_bytes=%d",
		topicName, partitionCount, strings.Join(leaders, ","), strings.Join(epochs, ","), t.Policy.Partitioner, t.Policy.AuthPolicy, strings.Join(t.Policy.ReadACL, ","), strings.Join(t.Policy.WriteACL, ","), t.Policy.RetentionHours, t.Policy.RetentionBytes)
}

func (ch *CommandHandler) handleFindCoordinator(cmd string) string {
	args := parseKeyValueArgs(cmd[17:]) // len("FIND_COORDINATOR ") = 17
	coordKey := args["group"]
	coordType := "group"
	if coordKey == "" {
		txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
		if txnID == "" {
			return "ERROR: missing_coordinator_key command=FIND_COORDINATOR"
		}
		coordKey = transactionCoordinatorKey(txnID)
		coordType = "transaction"
	}

	host := "localhost"
	port := ch.Config.BrokerPort

	if ch.isDistributed() {
		coordID, _, err := ch.Cluster.Router.FindCoordinator(coordKey)
		if err != nil {
			return fmt.Sprintf("ERROR: find_coordinator_failed reason=%q", err.Error())
		}

		if coordID == ch.Cluster.Router.BrokerID() {
			if ch.Config.AdvertisedClientHost != "" {
				host = ch.Config.AdvertisedClientHost
			}
			if ch.Config.AdvertisedBrokerPort > 0 {
				port = ch.Config.AdvertisedBrokerPort
			}
			return fmt.Sprintf("OK coordinator_id=%s coordinator_type=%s host=%s port=%d", coordID, coordType, host, port)
		}

		encodedCmd := util.EncodeMessage("", cmd)
		resp, fwdErr := ch.Cluster.Router.ForwardToCoordinator(coordKey, string(encodedCmd))
		if fwdErr != nil {
			return fmt.Sprintf("ERROR: forward_to_coordinator_failed reason=%q", fwdErr.Error())
		}
		return resp
	}

	if ch.Config.AdvertisedClientHost != "" {
		host = ch.Config.AdvertisedClientHost
	}
	if ch.Config.AdvertisedBrokerPort > 0 {
		port = ch.Config.AdvertisedBrokerPort
	}
	return fmt.Sprintf("OK coordinator_id=standalone coordinator_type=%s host=%s port=%d", coordType, host, port)
}
