package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/ackpolicy"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const unassignedLeaderOffset = uint64(math.MaxUint64)

func markLeaderOffsetsUnassigned(messages []types.Message) {
	for i := range messages {
		messages[i].Offset = unassignedLeaderOffset
	}
}

func appendedLeaderMessages(messages []types.Message) []types.Message {
	appended := make([]types.Message, 0, len(messages))
	for i := range messages {
		if messages[i].Offset != unassignedLeaderOffset {
			appended = append(appended, messages[i])
		}
	}
	return appended
}

// handlePublish processes PUBLISH command
func (ch *CommandHandler) handlePublish(cmd string, ctx ...*ClientContext) (response string) {
	var clientCtx *ClientContext
	if len(ctx) > 0 {
		clientCtx = ctx[0]
	}
	requestCtx := clientCtx.RequestContext()
	args := parseKeyValueArgs(cmd[8:])
	if authResp := ch.authenticateInline(args, clientCtx); authResp != "" {
		return authResp
	}
	var err error

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing_topic command=PUBLISH"
	}

	message, ok := args["message"]
	if !ok || message == "" {
		return "ERROR: missing_message command=PUBLISH"
	}

	producerID, ok := args["producerId"]
	if !ok || producerID == "" {
		return "ERROR: missing_producer_id command=PUBLISH"
	}
	if topicName == config.ConsumerOffsetsTopicName {
		return fmt.Sprintf("ERROR: internal_topic_write_forbidden topic=%s", topicName)
	}

	acks, ok := args["acks"]
	if !ok || acks == "" {
		acks = "1"
	}

	isIdempotent := false
	if val, ok := args["isIdempotent"]; ok {
		valLower := strings.ToLower(val)
		switch valLower {
		case "true":
			isIdempotent = true
		case "false":
			isIdempotent = false
		default:
			return fmt.Sprintf("ERROR: invalid_is_idempotent value=%s", val)
		}
	}

	ackSelection, ackErr := ackpolicy.Parse(acks)
	if ackErr != nil {
		return fmt.Sprintf("ERROR: invalid_acks value=%s", acks)
	}
	acks = ackSelection.Requested
	defer func() {
		if clientCtx != nil && clientCtx.Internal {
			return
		}
		result := "success"
		if strings.HasPrefix(response, "ERROR:") {
			result = "failure"
		}
		metrics.PublishAcknowledgements.WithLabelValues(string(ackSelection.Mode), result).Inc()
	}()

	var seqNum uint64
	if seqNumStr, ok := args["seqNum"]; ok {
		seqNum, err = strconv.ParseUint(seqNumStr, 10, 64)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid_seq_num reason=%q", err.Error())
		}
	}

	var epoch int64
	if epochStr, ok := args["epoch"]; ok {
		epoch, err = strconv.ParseInt(epochStr, 10, 64)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid_epoch reason=%q", err.Error())
		}
	}

	partition := -1
	if partitionStr, ok := args["partition"]; ok {
		parsedPartition, parseErr := strconv.Atoi(partitionStr)
		if parseErr != nil {
			return fmt.Sprintf("ERROR: invalid_partition reason=%q", parseErr.Error())
		}
		partition = parsedPartition
	}

	var ackResp types.AckResponse
	t, waitErr := ch.waitForTopicContext(requestCtx, topicName)
	if waitErr != nil {
		return "ERROR: request_cancelled"
	}
	if t == nil {
		util.Warn("ch publish: topic '%s' does not exist after retries", topicName)
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if strings.EqualFold(args["internal_txn_publish"], "true") {
		if clientCtx == nil || !clientCtx.Internal {
			return "ERROR: internal_txn_publish_forbidden command=PUBLISH"
		}
		if !t.PolicySnapshot().CanWrite() {
			return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", topicName)
		}
	} else if authResp := ch.authorizeTopicWrite(t.PolicySnapshot(), clientCtx); authResp != "" {
		return fmt.Sprintf("%s topic=%s", authResp, topicName)
	}
	effectiveIdempotent := isIdempotent || ch.Config.EnableIdempotence || t.IsIdempotent
	if effectiveIdempotent && !ackSelection.SupportsIdempotence() {
		return "ERROR: invalid_acks reason=\"idempotent publish requires acks=all or acks=-1\""
	}

	controlBatchVersion, errResp := parseControlBatchVersion(args["control_batch_version"])
	if errResp != "" {
		return errResp
	}
	controlBatchCoordinatorEpoch, errResp := parseControlBatchCoordinatorEpoch(args["control_batch_coordinator_epoch"])
	if errResp != "" {
		return errResp
	}
	controlBatchKey, errResp := parseControlBatchBytes(args["control_batch_key"], "key")
	if errResp != "" {
		return errResp
	}
	controlBatchValue, errResp := parseControlBatchBytes(args["control_batch_value"], "value")
	if errResp != "" {
		return errResp
	}

	msg := &types.Message{
		Payload:                      message,
		Key:                          args["key"],
		ProducerID:                   producerID,
		SeqNum:                       seqNum,
		Epoch:                        epoch,
		TransactionalID:              args["transactional_id"],
		TransactionState:             args["transaction_state"],
		TransactionMarker:            args["transaction_marker"],
		ControlBatchType:             args["control_batch_type"],
		ControlBatchVersion:          controlBatchVersion,
		ControlBatchCoordinatorEpoch: controlBatchCoordinatorEpoch,
		ControlBatchKey:              controlBatchKey,
		ControlBatchValue:            controlBatchValue,
	}

	if partition < 0 {
		partition = t.GetPartitionForMessage(*msg)
	}
	if _, err := t.GetPartition(partition); err != nil {
		return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
	}
	if errResp := ch.validateTransactionPublishMetadata(args, topicName, partition, msg); errResp != "" {
		return errResp
	}

	if ch.Config.EnabledDistribution && ch.Cluster != nil {
		forwardCmd := cmd
		if _, explicitPartition := args["partition"]; !explicitPartition {
			forwardCmd = fmt.Sprintf("PUBLISH topic=%s acks=%s producerId=%s partition=%d seqNum=%d epoch=%d", topicName, acks, producerID, partition, seqNum, epoch)
			if _, explicitIdempotent := args["isIdempotent"]; explicitIdempotent {
				forwardCmd += fmt.Sprintf(" isIdempotent=%t", isIdempotent)
			}
			if msg.TransactionalID != "" {
				forwardCmd += fmt.Sprintf(" transactional_id=%s", msg.TransactionalID)
			}
			if msg.TransactionState != "" {
				forwardCmd += fmt.Sprintf(" transaction_state=%s", msg.TransactionState)
			}
			if msg.TransactionMarker != "" {
				forwardCmd += fmt.Sprintf(" transaction_marker=%s", msg.TransactionMarker)
			}
			if msg.ControlBatchType != "" {
				forwardCmd += fmt.Sprintf(" control_batch_type=%s control_batch_version=%d control_batch_coordinator_epoch=%d", msg.ControlBatchType, msg.ControlBatchVersion, msg.ControlBatchCoordinatorEpoch)
				if len(msg.ControlBatchKey) > 0 {
					forwardCmd += fmt.Sprintf(" control_batch_key=%s", base64.StdEncoding.EncodeToString(msg.ControlBatchKey))
				}
				if len(msg.ControlBatchValue) > 0 {
					forwardCmd += fmt.Sprintf(" control_batch_value=%s", base64.StdEncoding.EncodeToString(msg.ControlBatchValue))
				}
			}
			if strings.EqualFold(args["internal_txn_publish"], "true") {
				forwardCmd += " internal_txn_publish=true"
			}
			forwardCmd += " message=" + message
		}
		if resp, forwarded, _ := ch.isPartitionLeaderAndForwardContext(requestCtx, topicName, partition, forwardCmd); forwarded {
			return resp
		}

		p, err := t.GetPartition(partition)
		if err != nil {
			util.Error("Publish failed: partition %d not found in topic %s: %v", partition, topicName, err)
			return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
		}

		effectiveMinISR := t.PolicySnapshot().EffectiveMinInSyncReplicas(ch.Config.MinInSyncReplicas)
		requiredISR := 0
		if ackSelection.Mode == ackpolicy.All {
			requiredISR = effectiveMinISR
		}
		releaseWrite, replicationSnapshot, err := ch.preparePartitionLeaderSnapshot(topicName, partition, p, requiredISR)
		if err != nil {
			return ch.partitionPreparationErrorResponse(err)
		}
		defer releaseWrite()
		if ch.replication == nil {
			return "ERROR: cluster_metadata_unavailable command=PUBLISH"
		}
		reservation, reserveErr := ch.replication.reserve(requestCtx, topicName, partition)
		if reserveErr != nil {
			return ch.errorResponse(fmt.Sprintf("replication backpressure: %v", reserveErr))
		}
		submitted := false
		defer func() {
			if !submitted {
				reservation.release()
			}
		}()

		scope := "partition"
		messageData := types.MessageCommand{
			Topic:         topicName,
			Partition:     partition,
			IsIdempotent:  effectiveIdempotent,
			SequenceScope: scope,
			Messages:      []types.Message{*msg},
			Acks:          acks,
		}

		markLeaderOffsetsUnassigned(messageData.Messages)
		// Duplicate producer sequences remain unassigned and must not be
		// replicated or committed again.
		if err := p.EnqueueBatchLeaderWithMode(messageData.Messages, effectiveIdempotent); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append locally: %v", err))
		}
		appended := appendedLeaderMessages(messageData.Messages)
		if len(appended) == 0 {
			lastOffset := uint64(0)
			nextOffset := p.NextOffset()
			if nextOffset > 0 {
				lastOffset = nextOffset - 1
			}
			if ackSelection.Mode == ackpolicy.All {
				result := make(chan error, 1)
				reservation.submit(partitionReplicationTask{
					topic:       topicName,
					partition:   partition,
					ackMode:     ackSelection.Mode,
					barrierOnly: true,
					snapshot:    replicationSnapshot,
					result:      result,
				})
				submitted = true
				select {
				case replicationErr := <-result:
					if replicationErr != nil {
						return ch.errorResponse(fmt.Sprintf("replication failed (offset=%d): %v", lastOffset, replicationErr))
					}
				case <-requestCtx.Done():
					return "ERROR: request_cancelled"
				}
			} else {
				reservation.release()
				submitted = true
			}
			ackResp = types.AckResponse{
				Status:        "OK",
				LastOffset:    lastOffset,
				ProducerEpoch: epoch,
				ProducerID:    producerID,
				SeqStart:      seqNum,
				SeqEnd:        seqNum,
			}
			goto Respond
		}

		messageData.Messages = appended
		assignedOffset := appended[len(appended)-1].Offset

		commitHWM := assignedOffset + 1
		var replicationResult chan error
		if ackSelection.Mode == ackpolicy.All {
			replicationResult = make(chan error, 1)
		}
		reservation.submit(partitionReplicationTask{
			topic:        topicName,
			partition:    partition,
			command:      messageData,
			commitHWM:    commitHWM,
			ackMode:      ackSelection.Mode,
			snapshot:     replicationSnapshot,
			partitionRef: p,
			result:       replicationResult,
		})
		submitted = true
		if replicationResult != nil {
			select {
			case replicationErr := <-replicationResult:
				if replicationErr != nil {
					return ch.errorResponse(fmt.Sprintf("replication failed (offset=%d): %v", assignedOffset, replicationErr))
				}
			case <-requestCtx.Done():
				return "ERROR: request_cancelled"
			}
		}

		ackResp = types.AckResponse{
			Status:        "OK",
			LastOffset:    assignedOffset,
			ProducerEpoch: epoch,
			ProducerID:    producerID,
			SeqStart:      seqNum,
			SeqEnd:        seqNum,
		}
		goto Respond
	} else { // stand-alone
		if ackSelection.Mode == ackpolicy.None {
			err = ch.TopicManager.PublishToPartition(topicName, partition, msg)
			if err != nil {
				util.Error("acks=0 publish failed (stand-alone): %v", err)
				return ch.errorResponse(fmt.Sprintf("acks=0 publish failed: %v", err))
			}
			return "OK"
		}
		if ackSelection.Mode == ackpolicy.All && t.PolicySnapshot().EffectiveMinInSyncReplicas(ch.Config.MinInSyncReplicas) > 1 {
			return "ERROR: insufficient_in_sync_replicas current=1"
		}
		if effectiveIdempotent {
			err = ch.TopicManager.PublishToPartitionWithAckIdempotent(topicName, partition, msg)
		} else {
			err = ch.TopicManager.PublishToPartitionWithAck(topicName, partition, msg)
		}
		if err != nil {
			return ch.errorResponse(fmt.Sprintf("acks=1 publish failed: %v", err))
		}
	}

	ackResp = types.AckResponse{
		Status:        "OK",
		LastOffset:    ch.TopicManager.GetLastOffset(topicName, partition),
		ProducerEpoch: epoch,
		ProducerID:    producerID,
		SeqStart:      seqNum,
		SeqEnd:        seqNum,
	}

Respond:
	if ch.isDistributed() {
		if leader := ch.Cluster.RaftManager.GetLeaderAddress(); leader != "" {
			ackResp.Leader = leader
		}
	}

	respBytes, err := json.Marshal(ackResp)
	if err != nil {
		util.Error("Failed to marshal response: %v", err)
		return "ERROR: marshal_ack_failed"
	}
	return string(respBytes)
}

func (ch *CommandHandler) waitForTopic(topicName string) *topic.Topic {
	t, _ := ch.waitForTopicContext(context.Background(), topicName)
	return t
}

func (ch *CommandHandler) waitForTopicContext(ctx context.Context, topicName string) (*topic.Topic, error) {
	t := ch.TopicManager.GetTopic(topicName)
	if t != nil {
		return t, nil
	}

	const maxRetries = 5
	const retryDelay = 100 * time.Millisecond
	util.Warn("Topic '%s' not found. Checking if creation is pending...", topicName)
	for i := 0; i < maxRetries; i++ {
		t = ch.TopicManager.GetTopic(topicName)
		if t != nil {
			return t, nil
		}
		if err := waitForContext(ctx, retryDelay); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (ch *CommandHandler) handleReplicateMessage(cmd string) string {
	// Format: REPLICATE_MESSAGE [internal_token=<token>] payload=<json_MessageCommand>
	idx := strings.Index(cmd, "payload=")
	if idx == -1 {
		return "ERROR: missing_payload command=REPLICATE_MESSAGE"
	}

	payload := cmd[idx+8:]
	var msgCmd types.MessageCommand
	if err := json.Unmarshal([]byte(payload), &msgCmd); err != nil {
		return fmt.Sprintf("ERROR: unmarshal_failed reason=%q", err.Error())
	}

	if errResp := ch.validateReplicaLeader(msgCmd); errResp != "" {
		return errResp
	}
	t := ch.TopicManager.GetTopic(msgCmd.Topic)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", msgCmd.Topic)
	}

	p, err := t.GetPartition(msgCmd.Partition)
	if err != nil {
		return fmt.Sprintf("ERROR: partition_not_found partition=%d", msgCmd.Partition)
	}
	if ch.isDistributed() {
		releaseWrite, prepareErr := ch.preparePartitionReplica(msgCmd.Topic, msgCmd.Partition, p, msgCmd.LeaderID, msgCmd.LeaderEpoch)
		if prepareErr != nil {
			return ch.errorResponse(prepareErr.Error())
		}
		defer releaseWrite()
	}

	if len(msgCmd.Messages) == 0 && msgCmd.CommitHWM == nil {
		return "ERROR: empty_messages command=REPLICATE_MESSAGE"
	}
	for i := range msgCmd.Messages {
		if errResp := ch.validateReplicatedTransactionMessage(msgCmd.Topic, msgCmd.Partition, &msgCmd.Messages[i]); errResp != "" {
			return errResp
		}
	}

	if len(msgCmd.Messages) > 0 {
		if err := p.ReplicaAppendWithMode(msgCmd.Messages, msgCmd.IsIdempotent); err != nil {
			return fmt.Sprintf("ERROR: replica_append_failed reason=%q", err.Error())
		}
	}
	if msgCmd.CommitHWM != nil {
		if t.IsEventSourcing && ch.ESHandler != nil {
			if err := ch.ESHandler.PrepareCommittedIndex(msgCmd.Topic, msgCmd.Partition); err != nil {
				return fmt.Sprintf("ERROR: replica_index_prepare_failed reason=%q", err.Error())
			}
		}
		if err := p.ApplyReplicaHWM(*msgCmd.CommitHWM); err != nil {
			return fmt.Sprintf("ERROR: invalid_commit_watermark reason=%q", err.Error())
		}
		p.FlushDisk()
		if t.IsEventSourcing && ch.ESHandler != nil {
			if err := ch.ESHandler.IndexCommittedToHWM(msgCmd.Topic, msgCmd.Partition, *msgCmd.CommitHWM); err != nil {
				return fmt.Sprintf("ERROR: replica_index_failed reason=%q", err.Error())
			}
		}
	}
	return fmt.Sprintf("OK leo=%d hwm=%d", p.NextOffset(), p.GetHWM())
}

// HandleBatchMessage processes PUBLISH of multiple messages.
func (ch *CommandHandler) HandleBatchMessage(data []byte, conn net.Conn, ctx ...*ClientContext) (response string, returnErr error) {
	var clientCtx *ClientContext
	if len(ctx) > 0 {
		clientCtx = ctx[0]
	}
	requestCtx := clientCtx.RequestContext()
	batch, err := util.DecodeBatchMessages(data)
	if err != nil {
		util.Error("Batch message decoding failed: %v", err)
		return fmt.Sprintf("ERROR: batch_decode_failed reason=%q", err.Error()), nil
	}
	if batch.Topic == config.ConsumerOffsetsTopicName {
		return fmt.Sprintf("ERROR: internal_topic_write_forbidden topic=%s", batch.Topic), nil
	}

	acks := batch.Acks
	if acks == "" {
		acks = "1"
	}

	ackSelection, ackErr := ackpolicy.Parse(acks)
	if ackErr != nil {
		return fmt.Sprintf("ERROR: invalid_acks value=%s", acks), nil
	}
	acks = ackSelection.Requested
	defer func() {
		if clientCtx != nil && clientCtx.Internal {
			return
		}
		result := "success"
		if strings.HasPrefix(response, "ERROR:") || returnErr != nil {
			result = "failure"
		}
		metrics.PublishAcknowledgements.WithLabelValues(string(ackSelection.Mode), result).Inc()
	}()

	var respAck types.AckResponse
	var lastMsg *types.Message
	var lastOffset uint64
	if ch.Config.EnabledDistribution && ch.Cluster != nil {
		if !ch.Cluster.IsAuthorized(batch.Topic, batch.Partition) {
			const maxRetries = 3
			const retryDelay = 200 * time.Millisecond
			var lastErr error

			for i := 0; i < maxRetries; i++ {
				util.Debug("Not Partition leader, forwarding BATCH (Attempt %d/%d)", i+1, maxRetries)
				resp, forwardErr := ch.Cluster.Router.ForwardDataToPartitionLeader(batch.Topic, batch.Partition, data)
				if forwardErr == nil {
					return resp, nil
				}

				util.Debug("Failed to forward batch to Partition leader: %v", forwardErr)

				if i < maxRetries-1 {
					if err := waitForContext(requestCtx, retryDelay); err != nil {
						return "ERROR: request_cancelled", nil
					}
				}
				lastErr = forwardErr
			}

			return ch.errorResponse(fmt.Sprintf("failed to forward BATCH to partition leader after %d attempts: %v", maxRetries, lastErr)), nil
		}

		util.Debug("Processing BATCH locally as Partition leader for %s:%d", batch.Topic, batch.Partition)

		t, waitErr := ch.waitForTopicContext(requestCtx, batch.Topic)
		if waitErr != nil {
			return "ERROR: request_cancelled", nil
		}
		if t == nil {
			util.Error("Batch process failed: topic '%s' not found", batch.Topic)
			return fmt.Sprintf("ERROR: topic_not_found topic=%s", batch.Topic), nil
		}
		if authResp := ch.authorizeTopicWrite(t.PolicySnapshot(), clientCtx); authResp != "" {
			return fmt.Sprintf("%s topic=%s", authResp, batch.Topic), nil
		}

		p, err := t.GetPartition(batch.Partition)
		if err != nil {
			util.Error("Batch process failed: partition %d not found in topic %s", batch.Partition, batch.Topic)
			return fmt.Sprintf("ERROR: partition_not_found partition=%d", batch.Partition), nil
		}

		if len(batch.Messages) == 0 {
			return ch.errorResponse("empty batch messages"), nil
		}

		effectiveIdempotent := batch.IsIdempotent || ch.Config.EnableIdempotence || t.IsIdempotent
		if effectiveIdempotent && !ackSelection.SupportsIdempotence() {
			return "ERROR: invalid_acks reason=\"idempotent publish requires acks=all or acks=-1\"", nil
		}
		scope := "partition"

		effectiveMinISR := t.PolicySnapshot().EffectiveMinInSyncReplicas(ch.Config.MinInSyncReplicas)
		requiredISR := 0
		if ackSelection.Mode == ackpolicy.All {
			requiredISR = effectiveMinISR
		}
		releaseWrite, replicationSnapshot, err := ch.preparePartitionLeaderSnapshot(batch.Topic, batch.Partition, p, requiredISR)
		if err != nil {
			return ch.partitionPreparationErrorResponse(err), nil
		}
		defer releaseWrite()
		if ch.replication == nil {
			return "ERROR: cluster_metadata_unavailable command=BATCH", nil
		}
		reservation, reserveErr := ch.replication.reserve(requestCtx, batch.Topic, batch.Partition)
		if reserveErr != nil {
			return ch.errorResponse(fmt.Sprintf("replication backpressure: %v", reserveErr)), nil
		}
		submitted := false
		defer func() {
			if !submitted {
				reservation.release()
			}
		}()

		markLeaderOffsetsUnassigned(batch.Messages)
		// Duplicate producer sequences remain unassigned and are acknowledged
		// without another replication round.
		if err := p.EnqueueBatchLeaderWithMode(batch.Messages, effectiveIdempotent); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append batch locally: %v", err)), nil
		}
		appended := appendedLeaderMessages(batch.Messages)
		if len(appended) == 0 {
			nextOffset := p.NextOffset()
			if nextOffset > 0 {
				lastOffset = nextOffset - 1
			}
			if ackSelection.Mode == ackpolicy.All {
				result := make(chan error, 1)
				reservation.submit(partitionReplicationTask{
					topic:       batch.Topic,
					partition:   batch.Partition,
					ackMode:     ackSelection.Mode,
					barrierOnly: true,
					snapshot:    replicationSnapshot,
					result:      result,
				})
				submitted = true
				select {
				case replicationErr := <-result:
					if replicationErr != nil {
						return ch.errorResponse(fmt.Sprintf("batch replication failed (offset=%d): %v", lastOffset, replicationErr)), nil
					}
				case <-requestCtx.Done():
					return "ERROR: request_cancelled", nil
				}
			} else {
				reservation.release()
				submitted = true
			}
			respAck = types.AckResponse{
				Status:        "OK",
				LastOffset:    lastOffset,
				SeqStart:      batch.Messages[0].SeqNum,
				SeqEnd:        batch.Messages[len(batch.Messages)-1].SeqNum,
				ProducerID:    batch.Messages[len(batch.Messages)-1].ProducerID,
				ProducerEpoch: batch.Messages[len(batch.Messages)-1].Epoch,
			}
			goto Respond
		}

		lastOffset = appended[len(appended)-1].Offset

		msgCmd := types.MessageCommand{
			Topic:         batch.Topic,
			Partition:     batch.Partition,
			IsIdempotent:  effectiveIdempotent,
			SequenceScope: scope,
			Messages:      appended,
			Acks:          acks,
		}

		commitHWM := lastOffset + 1
		var replicationResult chan error
		if ackSelection.Mode == ackpolicy.All {
			replicationResult = make(chan error, 1)
		}
		reservation.submit(partitionReplicationTask{
			topic:        batch.Topic,
			partition:    batch.Partition,
			command:      msgCmd,
			commitHWM:    commitHWM,
			ackMode:      ackSelection.Mode,
			snapshot:     replicationSnapshot,
			partitionRef: p,
			result:       replicationResult,
		})
		submitted = true
		if replicationResult != nil {
			select {
			case replicationErr := <-replicationResult:
				if replicationErr != nil {
					return ch.errorResponse(fmt.Sprintf("batch replication failed (offset=%d): %v", lastOffset, replicationErr)), nil
				}
			case <-requestCtx.Done():
				return "ERROR: request_cancelled", nil
			}
		}

		respAck = types.AckResponse{
			Status:        "OK",
			LastOffset:    lastOffset,
			SeqStart:      batch.Messages[0].SeqNum,
			SeqEnd:        batch.Messages[len(batch.Messages)-1].SeqNum,
			ProducerID:    batch.Messages[len(batch.Messages)-1].ProducerID,
			ProducerEpoch: batch.Messages[len(batch.Messages)-1].Epoch,
		}
		goto Respond
	}

	// stand-alone
	{
		t, waitErr := ch.waitForTopicContext(requestCtx, batch.Topic)
		if waitErr != nil {
			return "ERROR: request_cancelled", nil
		}
		if t == nil {
			return fmt.Sprintf("ERROR: topic_not_found topic=%s", batch.Topic), nil
		}
		if authResp := ch.authorizeTopicWrite(t.PolicySnapshot(), clientCtx); authResp != "" {
			return fmt.Sprintf("%s topic=%s", authResp, batch.Topic), nil
		}
		p, err := t.GetPartition(batch.Partition)
		if err != nil {
			return fmt.Sprintf("ERROR: partition_not_found partition=%d", batch.Partition), nil
		}

		effectiveIdempotent := batch.IsIdempotent || ch.Config.EnableIdempotence || t.IsIdempotent
		if effectiveIdempotent && !ackSelection.SupportsIdempotence() {
			return "ERROR: invalid_acks reason=\"idempotent publish requires acks=all or acks=-1\"", nil
		}
		if ackSelection.Mode == ackpolicy.None {
			err = p.EnqueueBatch(batch.Messages)
			if err != nil {
				util.Error("acks=0 batch publish failed (stand-alone): %v", err)
				return ch.errorResponse(fmt.Sprintf("acks=0 batch publish failed: %v", err)), nil
			}
			return "OK", nil
		}
		if ackSelection.Mode == ackpolicy.All && t.PolicySnapshot().EffectiveMinInSyncReplicas(ch.Config.MinInSyncReplicas) > 1 {
			return "ERROR: insufficient_in_sync_replicas current=1", nil
		}
		err = p.EnqueueBatchSyncWithMode(batch.Messages, effectiveIdempotent)
		if err != nil {
			return ch.errorResponse(fmt.Sprintf("acks=1 batch publish failed: %v", err)), err
		}

		if len(batch.Messages) > 0 {
			lastMsg = &batch.Messages[len(batch.Messages)-1]
		} else {
			return ch.errorResponse("empty batch messages"), nil
		}

		lastOffset = 0
		if nextOffset := p.NextOffset(); nextOffset > 0 {
			lastOffset = nextOffset - 1
		}
		respAck = types.AckResponse{
			Status:        "OK",
			LastOffset:    lastOffset,
			SeqStart:      batch.Messages[0].SeqNum,
			SeqEnd:        lastMsg.SeqNum,
			ProducerID:    lastMsg.ProducerID,
			ProducerEpoch: lastMsg.Epoch,
		}
	}

Respond:
	if ch.isDistributed() {
		if leader := ch.Cluster.RaftManager.GetLeaderAddress(); leader != "" {
			respAck.Leader = leader
		}
	}

	ackBytes, err := json.Marshal(respAck)
	if err != nil {
		util.Error("Failed to marshal AckResponse: %v", err)
		return "ERROR: marshal_ack_failed", nil
	}

	responseStr := string(ackBytes)
	util.Debug("Broker Sending Batch Ack (Topic: %s): %s", batch.Topic, responseStr)
	return responseStr, nil
}

func parseControlBatchVersion(value string) (int16, string) {
	if value == "" {
		return 0, ""
	}
	parsed, err := strconv.ParseInt(value, 10, 16)
	if err != nil {
		return 0, fmt.Sprintf("ERROR: invalid_control_batch_version reason=%q", err.Error())
	}
	return int16(parsed), ""
}

func parseControlBatchCoordinatorEpoch(value string) (int64, string) {
	if value == "" {
		return 0, ""
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Sprintf("ERROR: invalid_control_batch_coordinator_epoch reason=%q", err.Error())
	}
	return parsed, ""
}

func parseControlBatchBytes(value, field string) ([]byte, string) {
	if value == "" {
		return nil, ""
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Sprintf("ERROR: invalid_control_batch_bytes field=%s reason=%q", field, err.Error())
	}
	return decoded, ""
}
