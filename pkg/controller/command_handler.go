package controller

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
)

// handleCreate processes CREATE command
func (ch *CommandHandler) handleCreate(cmd string, ctx ...*ClientContext) string {
	requestCtx := firstClientContext(ctx).RequestContext()
	args := parseKeyValueArgs(cmd[7:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic expected=\"CREATE topic=<name> [partitions=<N>]\""
	}

	if err := topic.ValidateName(topicName); err != nil {
		return fmt.Sprintf("ERROR: invalid_topic_name topic=%q reason=%q", topicName, err.Error())
	}

	patch, patchErr := parseTopicDefinitionPatch(args)
	if patchErr != "" {
		return patchErr
	}
	defaults := topic.DefaultDefinition(topicName, ch.Config)
	validationReplicationFactor := defaults.ReplicationFactor
	if patch.ReplicationFactor != nil {
		validationReplicationFactor = *patch.ReplicationFactor
	}
	if existing := ch.TopicManager.GetTopic(topicName); existing != nil {
		validationReplicationFactor = ch.topicReplicationFactor(topicName)
	} else if !ch.isDistributed() {
		validationReplicationFactor = 1
	}
	if patch.MinInSyncReplicas != nil && *patch.MinInSyncReplicas > validationReplicationFactor {
		return fmt.Sprintf("ERROR: invalid_min_in_sync_replicas value=%d replication_factor=%d", *patch.MinInSyncReplicas, validationReplicationFactor)
	}

	tm := ch.TopicManager
	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isLeaderAndForwardContext(requestCtx, cmd); forwarded {
			return resp
		}

		var current *topic.Definition
		if fsmRef := ch.Cluster.RaftManager.GetFSM(); fsmRef != nil {
			if definition, found := fsmRef.GetTopicDefinition(topicName); found {
				current = &definition
			}
		}
		payload, payloadErr := distributedTopicCommandPayload(defaults, patch, current)
		if payloadErr != nil {
			return formatCreateTopicError(topicName, payloadErr)
		}
		_, err := ch.applyAndWaitContext(requestCtx, "TOPIC", payload)
		if err != nil {
			return formatCreateTopicError(topicName, err)
		}
	} else {
		if tm.GetTopic(topicName) == nil {
			if err := ch.ensureTopicRecreationIsClean(topicName); err != nil {
				return formatCreateTopicError(topicName, err)
			}
		}
		if _, err := tm.CreateTopicWithPatch(defaults, patch); err != nil {
			return formatCreateTopicError(topicName, err)
		}
	}

	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_create_missing topic=%s", topicName)
	}

	if ch.Coordinator != nil {
		err := ch.Coordinator.RegisterGroup(topicName, "default-group", len(t.Partitions))
		if err != nil {
			util.Warn("Failed to register default group with coordinator: %v", err)
		}
	}
	definition := t.Definition()
	return formatTopicDefinitionResponse(definition) + " " + ch.topicMinISRMetadata(definition.Policy)
}

// handleDelete processes DELETE command
func (ch *CommandHandler) handleDelete(cmd string, ctx ...*ClientContext) string {
	requestCtx := firstClientContext(ctx).RequestContext()
	args := parseKeyValueArgs(cmd[7:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic expected=\"DELETE topic=<name>\""
	}

	if err := topic.ValidateName(topicName); err != nil {
		return fmt.Sprintf("ERROR: invalid_topic_name topic=%q reason=%q", topicName, err.Error())
	}
	if topicName == config.ConsumerOffsetsTopicName {
		return fmt.Sprintf("ERROR: internal_topic_delete_forbidden topic=%s", topicName)
	}
	ifExists := false
	if value, present := args["if_exists"]; present {
		parsed, valid := parseCreateBool(value)
		if !valid {
			return fmt.Sprintf("ERROR: invalid_if_exists value=%q", value)
		}
		ifExists = parsed
	}

	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isLeaderAndForwardContext(requestCtx, cmd); forwarded {
			return resp
		}

		payload := map[string]interface{}{
			"topic":     topicName,
			"if_exists": ifExists,
		}
		result, err := ch.applyAndWaitContext(requestCtx, "TOPIC_DELETE", payload)
		if err != nil {
			if errors.Is(err, topic.ErrTopicNotFound) {
				return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
			}
			if errors.Is(err, topic.ErrTopicDeleteBlocked) {
				return fmt.Sprintf("ERROR: topic_delete_blocked topic=%s reason=%q", topicName, err.Error())
			}
			return fmt.Sprintf("ERROR: delete_topic_failed reason=%q", err.Error())
		}
		ch.closeEventSourcingTopic(topicName)
		deleted := true
		cleanupPending := false
		if deleteResult, ok := result.(topic.DeleteResult); ok {
			deleted = deleteResult.Deleted
			cleanupPending = deleteResult.CleanupPending
		}
		if cleanupPending {
			return fmt.Sprintf("OK topic=%s deleted=%t cleanup_pending=true", topicName, deleted)
		}
		return fmt.Sprintf("OK topic=%s deleted=%t", topicName, deleted)
	}

	exists := ch.TopicManager.GetTopic(topicName) != nil
	if !exists && !ifExists {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	transactionState, err := ch.prepareStandaloneTopicDependencies(topicName)
	if err != nil {
		if errors.Is(err, topic.ErrTopicDeleteBlocked) {
			return fmt.Sprintf("ERROR: topic_delete_blocked topic=%s reason=%q", topicName, err.Error())
		}
		return fmt.Sprintf("ERROR: delete_topic_failed topic=%s reason=%q", topicName, err.Error())
	}
	if !exists {
		if err := ch.applyStandaloneTopicDependencyCleanup(topicName, transactionState); err != nil {
			util.Warn("Topic %s is absent with pending dependency cleanup: %v", topicName, err)
			return fmt.Sprintf("OK topic=%s deleted=false cleanup_pending=true", topicName)
		}
		return fmt.Sprintf("OK topic=%s deleted=false", topicName)
	}
	deleted, deleteErr := ch.TopicManager.DeleteTopicDurable(topicName)
	if deleteErr != nil && !deleted {
		return fmt.Sprintf("ERROR: delete_topic_failed topic=%s reason=%q", topicName, deleteErr.Error())
	}
	if !deleted {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	cleanupErr := ch.applyStandaloneTopicDependencyCleanup(topicName, transactionState)
	if deleteErr != nil || cleanupErr != nil {
		util.Warn("Topic %s was logically deleted with pending cleanup: storage=%v dependencies=%v", topicName, deleteErr, cleanupErr)
		return fmt.Sprintf("OK topic=%s deleted=true cleanup_pending=true", topicName)
	}
	return fmt.Sprintf("OK topic=%s deleted=true", topicName)
}

// handleTruncate resets topic data while retaining its definition. The
// required expected_revision is an optimistic guard and is consumed exactly
// once by the lifecycle epoch transition.
func (ch *CommandHandler) handleTruncate(cmd string, ctx ...*ClientContext) string {
	requestCtx := firstClientContext(ctx).RequestContext()
	args := parseKeyValueArgs(strings.TrimPrefix(cmd, "TRUNCATE "))
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic expected=\"TRUNCATE topic=<name> expected_revision=<N>\""
	}
	if err := topic.ValidateName(topicName); err != nil {
		return fmt.Sprintf("ERROR: invalid_topic_name topic=%q reason=%q", topicName, err.Error())
	}
	if topicName == config.ConsumerOffsetsTopicName {
		return fmt.Sprintf("ERROR: internal_topic_truncate_forbidden topic=%s", topicName)
	}
	expectedValue := args["expected_revision"]
	if expectedValue == "" {
		return "ERROR: missing_expected_revision command=TRUNCATE"
	}
	expectedRevision, err := strconv.ParseUint(expectedValue, 10, 64)
	if err != nil || expectedRevision == 0 {
		return fmt.Sprintf("ERROR: invalid_expected_revision value=%q", expectedValue)
	}

	if ch.isDistributed() {
		if resp, forwarded, _ := ch.isLeaderAndForwardContext(requestCtx, cmd); forwarded {
			return resp
		}
		result, applyErr := ch.applyAndWaitContext(requestCtx, "TOPIC_TRUNCATE", map[string]interface{}{
			"topic":             topicName,
			"expected_revision": expectedRevision,
		})
		if applyErr != nil {
			return formatTruncateError(topicName, expectedRevision, applyErr)
		}
		truncateResult, ok := result.(topic.TruncateResult)
		if !ok {
			return fmt.Sprintf("ERROR: truncate_topic_failed topic=%s reason=%q", topicName, "invalid replicated truncate result")
		}
		return formatTruncateResult(truncateResult)
	}

	transactionState, err := ch.prepareStandaloneTopicDependencies(topicName)
	if err != nil {
		if errors.Is(err, topic.ErrTopicDeleteBlocked) {
			return fmt.Sprintf("ERROR: topic_truncate_blocked topic=%s reason=%q", topicName, err.Error())
		}
		return fmt.Sprintf("ERROR: truncate_topic_failed topic=%s reason=%q", topicName, err.Error())
	}
	result, truncateErr := ch.TopicManager.TruncateTopicDurable(topicName, expectedRevision)
	if truncateErr != nil && !result.Truncated {
		return formatTruncateError(topicName, expectedRevision, truncateErr)
	}
	dependencyErr := ch.applyStandaloneTopicDependencyCleanup(topicName, transactionState)
	completeErr := ch.TopicManager.CompleteTruncation(topicName)
	if truncateErr != nil || dependencyErr != nil || completeErr != nil {
		util.Warn("Topic %s truncate committed with pending cleanup: storage=%v dependencies=%v completion=%v", topicName, truncateErr, dependencyErr, completeErr)
		result.CleanupPending = true
		return formatTruncateResult(result)
	}
	result.CleanupPending = false
	return formatTruncateResult(result)
}

func formatTruncateError(topicName string, expectedRevision uint64, err error) string {
	switch {
	case errors.Is(err, topic.ErrTopicNotFound):
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	case errors.Is(err, topic.ErrTopicRevisionConflict):
		return fmt.Sprintf("ERROR: topic_revision_conflict topic=%s expected=%d reason=%q", topicName, expectedRevision, err.Error())
	case errors.Is(err, topic.ErrTopicDeleteBlocked):
		return fmt.Sprintf("ERROR: topic_truncate_blocked topic=%s reason=%q", topicName, err.Error())
	default:
		return fmt.Sprintf("ERROR: truncate_topic_failed topic=%s reason=%q", topicName, err.Error())
	}
}

func formatTruncateResult(result topic.TruncateResult) string {
	response := fmt.Sprintf(
		"OK topic=%s truncated=%t revision=%d lifecycle_epoch=%d leo=0 hwm=0",
		result.Topic, result.Truncated, result.Definition.Revision, result.Definition.LifecycleEpoch,
	)
	if result.CleanupPending {
		response += " cleanup_pending=true"
	}
	return response
}

// RecoverPendingTruncations resumes a standalone lifecycle transition found
// in the manifest before the broker begins serving requests.
func (ch *CommandHandler) RecoverPendingTruncations() error {
	if ch == nil || ch.TopicManager == nil || ch.isDistributed() {
		return nil
	}
	for _, definition := range ch.TopicManager.PendingTruncations() {
		if definition.Revision <= topic.InitialDefinitionRevision {
			return fmt.Errorf("invalid pending truncate revision for topic %q", definition.Name)
		}
		transactionState, err := ch.prepareStandaloneTopicDependencies(definition.Name)
		if err != nil {
			return err
		}
		result, truncateErr := ch.TopicManager.TruncateTopicDurable(definition.Name, definition.Revision-1)
		if truncateErr != nil && !result.Truncated {
			return truncateErr
		}
		if err := ch.applyStandaloneTopicDependencyCleanup(definition.Name, transactionState); err != nil {
			return err
		}
		if err := ch.TopicManager.CompleteTruncation(definition.Name); err != nil {
			return err
		}
	}
	return nil
}

func (ch *CommandHandler) ensureTopicRecreationIsClean(topicName string) error {
	if ch.Coordinator != nil {
		if references := ch.Coordinator.TopicGroupReferences(topicName); len(references) != 0 {
			return fmt.Errorf("topic %q lifecycle cleanup is pending for consumer group %q", topicName, references[0].Name)
		}
	}
	if ch.TxnManager != nil {
		_, affected, err := ch.TxnManager.StateWithoutTopicReferences(topicName)
		if err != nil {
			return fmt.Errorf("topic %q lifecycle cleanup is pending: %w", topicName, err)
		}
		if len(affected) != 0 {
			return fmt.Errorf("topic %q lifecycle cleanup is pending for transaction %q", topicName, affected[0])
		}
	}
	return nil
}

func (ch *CommandHandler) prepareStandaloneTopicDependencies(topicName string) (map[string]*transaction.Snapshot, error) {
	if ch.Coordinator != nil {
		for _, reference := range ch.Coordinator.TopicGroupReferences(topicName) {
			if reference.MemberCount != 0 {
				return nil, fmt.Errorf("%w: consumer group %q has %d active member(s)", topic.ErrTopicDeleteBlocked, reference.Name, reference.MemberCount)
			}
		}
	}

	if ch.TxnManager == nil {
		return nil, nil
	}
	state, _, err := ch.TxnManager.StateWithoutTopicReferences(topicName)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", topic.ErrTopicDeleteBlocked, err)
	}
	return state, nil
}

func (ch *CommandHandler) applyStandaloneTopicDependencyCleanup(topicName string, transactionState map[string]*transaction.Snapshot) error {
	if ch.Coordinator != nil {
		if _, err := ch.Coordinator.DeleteInactiveGroupsForTopic(topicName); err != nil {
			return fmt.Errorf("delete inactive consumer groups for topic %q: %w", topicName, err)
		}
	}
	if transactionState != nil {
		if ch.txnJournal != nil {
			if err := ch.txnJournal.Rewrite(transactionState); err != nil {
				return fmt.Errorf("rewrite transaction journal for topic deletion: %w", err)
			}
		}
		if err := ch.TxnManager.ImportState(transactionState); err != nil {
			return fmt.Errorf("install transaction state for topic deletion: %w", err)
		}
	}
	return nil
}

func (ch *CommandHandler) closeEventSourcingTopic(topicName string) {
	if ch.ESHandler == nil {
		return
	}
	if err := ch.ESHandler.DeleteTopic(topicName); err != nil {
		util.Warn("Failed to close event sourcing metadata for deleted topic %s: %v", topicName, err)
	}
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

	if ch.isDistributed() {
		_, err := ch.applyViaLeader("GROUP_SYNC", map[string]interface{}{
			"type":            "REGISTER",
			"group":           groupName,
			"topic":           topicName,
			"partition_count": len(t.Partitions),
		})
		if err != nil {
			return fmt.Sprintf("ERROR: register_group_failed reason=%q", err.Error())
		}
		return fmt.Sprintf("OK group=%s topic=%s registered=true", groupName, topicName)
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

	if ch.isDistributed() {
		coordAddr, isCoord, coordErr := ch.checkCoordinator(groupName)
		if coordErr != nil {
			return coordinatorUnavailableResponse
		}
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}
	if _, topicErr := ch.resolveGroupOffsetTopic(groupName, topicName); topicErr != "" {
		return topicErr
	}

	if generationText := args["generation"]; generationText != "" {
		generation, parseErr := strconv.Atoi(generationText)
		if parseErr != nil {
			return "ERROR: invalid_generation command=JOIN_GROUP"
		}
		if ch.Coordinator == nil {
			return "ERROR: coordinator_not_available"
		}
		assignments, resumeErr := ch.Coordinator.ResumeConsumer(groupName, consumerID, generation)
		if resumeErr != nil {
			return formatCoordinatorError(resumeErr)
		}
		ctx.MemberID = consumerID
		ctx.Generation = generation
		return fmt.Sprintf(
			"OK generation=%d member=%s assignments=%v resumed=true",
			generation,
			consumerID,
			assignments,
		)
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
		topicRef := ch.TopicManager.GetTopic(topicName)
		if topicRef == nil {
			return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
		}
		joinPayload := map[string]interface{}{
			"type":            "JOIN",
			"group":           groupName,
			"member":          consumerID,
			"topic":           topicName,
			"partition_count": len(topicRef.Partitions),
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
		coordAddr, isCoord, coordErr := ch.checkCoordinator(groupName)
		if coordErr != nil {
			return coordinatorUnavailableResponse
		}
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}
	if _, topicErr := ch.resolveGroupOffsetTopic(groupName, topicName); topicErr != "" {
		return topicErr
	}

	generationText := args["generation"]
	if generationText == "" {
		return "ERROR: missing_generation command=SYNC_GROUP"
	}
	generation, parseErr := strconv.Atoi(generationText)
	if parseErr != nil {
		return "ERROR: invalid_generation command=SYNC_GROUP"
	}
	assignments, syncErr := ch.Coordinator.ResumeConsumer(groupName, memberID, generation)
	if syncErr != nil {
		return formatCoordinatorError(syncErr)
	}
	return fmt.Sprintf("OK generation=%d member=%s assignments=%v", generation, memberID, assignments)
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

	generationText := args["generation"]
	if generationText == "" {
		return "ERROR: missing_generation command=LEAVE_GROUP"
	}
	generation, parseErr := strconv.Atoi(generationText)
	if parseErr != nil {
		return "ERROR: invalid_generation command=LEAVE_GROUP"
	}
	if ch.Coordinator == nil {
		return "ERROR: coordinator_not_available"
	}

	if ch.isDistributed() {
		coordAddr, isCoord, coordErr := ch.checkCoordinator(groupName)
		if coordErr != nil {
			return coordinatorUnavailableResponse
		}
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}

	}
	if _, topicErr := ch.resolveGroupOffsetTopic(groupName, topicName); topicErr != "" {
		return topicErr
	}
	if errResp := ch.Coordinator.ValidateMemberGeneration(groupName, consumerID, generation); errResp != "" {
		return errResp
	}
	if ch.isDistributed() {
		payload := map[string]interface{}{
			"type":       "LEAVE",
			"group":      groupName,
			"member":     consumerID,
			"generation": generation,
		}

		_, err := ch.applyViaLeader("GROUP_SYNC", payload)
		if err != nil {
			return formatReplicatedGroupError(err, "register_group_failed")
		}
	} else {
		if ch.Coordinator != nil {
			if err := ch.Coordinator.RemoveConsumerForGeneration(groupName, consumerID, generation); err != nil {
				return formatCoordinatorError(err)
			}
		}
	}
	return fmt.Sprintf("OK group=%s member=%s left=true", groupName, consumerID)
}

// handleListOffsets processes LIST_OFFSETS topic=<name> [partition=<N>].
func (ch *CommandHandler) handleListOffsets(cmd string, ctx *ClientContext) string {
	argsText := ""
	if len(cmd) > len("LIST_OFFSETS") {
		argsText = strings.TrimSpace(cmd[len("LIST_OFFSETS"):])
	}
	args := parseKeyValueArgs(argsText)
	if authResp := ch.authenticateInline(args, ctx); authResp != "" {
		return authResp
	}
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=LIST_OFFSETS"
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if authResp := ch.authorizeTopicRead(t.PolicySnapshot(), ctx); authResp != "" {
		return fmt.Sprintf("%s topic=%s", authResp, topicName)
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
		coordAddr, isCoord, coordErr := ch.checkCoordinator(groupName)
		if coordErr != nil {
			return coordinatorUnavailableResponse
		}
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
		coordAddr, isCoord, coordErr := ch.checkCoordinator(groupName)
		if coordErr != nil {
			return coordinatorUnavailableResponse
		}
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
		coordAddr, isCoord, coordErr := ch.checkCoordinator(groupName)
		if coordErr != nil {
			return coordinatorUnavailableResponse
		}
		if !isCoord {
			return notCoordinatorResponse(coordAddr)
		}
	}

	if ch.Coordinator == nil {
		return "ERROR: coordinator_not_available"
	}
	if _, topicErr := ch.resolveGroupOffsetTopic(groupName, topicName); topicErr != "" {
		return topicErr
	}

	generationText := args["generation"]
	if generationText == "" {
		return "ERROR: missing_generation command=HEARTBEAT"
	}
	generation, parseErr := strconv.Atoi(generationText)
	if parseErr != nil {
		return "ERROR: invalid_generation command=HEARTBEAT"
	}
	if err := ch.Coordinator.RecordHeartbeatForGeneration(groupName, consumerID, generation); err != nil {
		return formatCoordinatorError(err)
	}
	return fmt.Sprintf("OK member=%s generation=%d", consumerID, generation)
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
	if err != nil || partition < 0 {
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
		coordAddr, isCoord, coordErr := ch.checkCoordinator(groupID)
		if coordErr != nil {
			return coordinatorUnavailableResponse
		}
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
	memberID := args["member"]
	if memberID == "" {
		return "ERROR: missing_member command=COMMIT_OFFSET"
	}
	generationText := args["generation"]
	if generationText == "" {
		return "ERROR: missing_generation command=COMMIT_OFFSET"
	}
	generation, genErr := strconv.Atoi(generationText)
	if genErr != nil {
		return "ERROR: invalid_generation command=COMMIT_OFFSET"
	}
	if errResp := ch.Coordinator.ValidateOwnershipFailure(groupID, memberID, generation, partition); errResp != "" {
		return errResp
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
			"type":       "COMMIT",
			"group":      groupID,
			"topic":      offsetTopic,
			"member":     memberID,
			"generation": generation,
			"partition":  partition,
			"offset":     offset,
		}
		_, err := ch.applyViaLeader("OFFSET_SYNC", payload)
		if err != nil {
			return formatReplicatedGroupError(err, "offset_sync_failed")
		}
		return "OK"
	}

	err = ch.Coordinator.ValidateAndCommit(groupID, offsetTopic, partition, offset, generation, memberID)
	if err != nil {
		return formatCoordinatorError(err)
	}
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
	generationText := args["generation"]
	if generationText == "" {
		return "ERROR: missing_generation command=BATCH_COMMIT"
	}
	generation, genErr := strconv.Atoi(generationText)
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
		coordAddr, isCoord, coordErr := ch.checkCoordinator(groupID)
		if coordErr != nil {
			return coordinatorUnavailableResponse
		}
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
		pair = strings.TrimSpace(pair)
		kv := strings.Split(pair, ":")
		if len(kv) != 2 {
			return fmt.Sprintf("ERROR: invalid_batch_commit_entry entry=%q", pair)
		}
		if !strings.HasPrefix(kv[0], "P") {
			return fmt.Sprintf("ERROR: invalid_partition entry=%q", pair)
		}
		partStr := strings.TrimPrefix(kv[0], "P")
		p, err := strconv.Atoi(partStr)
		if err != nil || p < 0 {
			return fmt.Sprintf("ERROR: invalid_partition entry=%q", pair)
		}
		o, err := strconv.ParseUint(kv[1], 10, 64)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid_offset entry=%q", pair)
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
			"type":       "BATCH_COMMIT",
			"group":      groupID,
			"topic":      offsetTopic,
			"member":     memberID,
			"generation": generation,
			"offsets":    offsetList,
		}
		_, err := ch.applyViaLeader("BATCH_OFFSET", batchCommitData)
		if err != nil {
			util.Error("Raft batch apply failed: %v", err)
			return formatReplicatedGroupError(err, "raft_batch_apply_failed")
		}
	} else if ch.Coordinator != nil {
		err := ch.Coordinator.ValidateAndCommitOffsetsBulk(groupID, offsetTopic, memberID, generation, offsetList)
		if err != nil {
			return formatCoordinatorError(err)
		}
	} else {
		return "ERROR: offset_manager_not_available"
	}

	return fmt.Sprintf("OK batched=%d", len(offsetList))
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

		if fsm := ch.Cluster.RaftManager.GetFSM(); fsm != nil {
			if broker := fsm.GetBroker(coordID); broker != nil && broker.ClientAddr != "" {
				if brokerHost, brokerPort, err := net.SplitHostPort(broker.ClientAddr); err == nil {
					parsedPort, _ := strconv.Atoi(brokerPort)
					if brokerHost != "" && parsedPort > 0 {
						return fmt.Sprintf("OK coordinator_id=%s coordinator_type=%s host=%s port=%d", coordID, coordType, brokerHost, parsedPort)
					}
				}
			}
		}

		resp, fwdErr := ch.Cluster.Router.ForwardToCoordinator(coordKey, cmd)
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
