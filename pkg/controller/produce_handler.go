package controller

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

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
func (ch *CommandHandler) handlePublish(cmd string, ctx ...*ClientContext) string {
	var clientCtx *ClientContext
	if len(ctx) > 0 {
		clientCtx = ctx[0]
	}
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

	acksLower := strings.ToLower(acks)
	if acksLower != "0" && acksLower != "1" && acksLower != "-1" && acksLower != "all" {
		return fmt.Sprintf("ERROR: invalid_acks value=%s", acks)
	}

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
	t := ch.waitForTopic(topicName)
	if t == nil {
		util.Warn("ch publish: topic '%s' does not exist after retries", topicName)
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if strings.EqualFold(args["internal_txn_publish"], "true") {
		if clientCtx == nil || !clientCtx.Internal {
			return "ERROR: internal_txn_publish_forbidden command=PUBLISH"
		}
		if !t.Policy.CanWrite() {
			return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", topicName)
		}
	} else if authResp := ch.authorizeTopicWrite(t.Policy, clientCtx); authResp != "" {
		return fmt.Sprintf("%s topic=%s", authResp, topicName)
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
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(topicName, partition, forwardCmd); forwarded {
			return resp
		}

		p, err := t.GetPartition(partition)
		if err != nil {
			util.Error("Publish failed: partition %d not found in topic %s: %v", partition, topicName, err)
			return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
		}

		effectiveIdempotent := isIdempotent || ch.Config.EnableIdempotence

		releaseWrite, err := ch.preparePartitionLeader(topicName, partition, p)
		if err != nil {
			return ch.errorResponse(err.Error())
		}
		defer releaseWrite()

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
		if err := p.EnqueueBatchLeader(messageData.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append locally: %v", err))
		}
		appended := appendedLeaderMessages(messageData.Messages)
		if len(appended) == 0 {
			lastOffset := uint64(0)
			if nextOffset := p.NextOffset(); nextOffset > 0 {
				lastOffset = nextOffset - 1
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
		p.FlushDisk()

		minISR := ch.Config.MinInSyncReplicas
		if minISR < 1 {
			minISR = 1
		}
		err = ch.Cluster.ReplicateToFollowers(topicName, partition, messageData, minISR)
		if err != nil {
			return ch.errorResponse(fmt.Sprintf("replication failed (offset=%d): %v", assignedOffset, err))
		}

		commitHWM := p.NextOffset()
		if err := ch.commitPartitionHWM(topicName, partition, commitHWM); err != nil {
			return ch.errorResponse(fmt.Sprintf("commit watermark failed (offset=%d): %v", assignedOffset, err))
		}
		if err := p.ApplyReplicaHWM(commitHWM); err != nil {
			return ch.errorResponse(fmt.Sprintf("local commit watermark failed: %v", err))
		}
		p.FlushDisk()

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
		if acks == "0" {
			err = ch.TopicManager.PublishToPartition(topicName, partition, msg)
			if err != nil {
				util.Error("acks=0 publish failed (stand-alone): %v", err)
			}
			return "OK"
		}
		err = ch.TopicManager.PublishToPartitionWithAck(topicName, partition, msg)
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
	t := ch.TopicManager.GetTopic(topicName)
	if t != nil {
		return t
	}

	const maxRetries = 5
	const retryDelay = 100 * time.Millisecond
	util.Warn("Topic '%s' not found. Checking if creation is pending...", topicName)
	for i := 0; i < maxRetries; i++ {
		t = ch.TopicManager.GetTopic(topicName)
		if t != nil {
			return t
		}
		time.Sleep(retryDelay)
	}
	return nil
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

	if len(msgCmd.Messages) == 0 && msgCmd.CommitHWM == nil {
		return "ERROR: empty_messages command=REPLICATE_MESSAGE"
	}
	for i := range msgCmd.Messages {
		if errResp := ch.validateReplicatedTransactionMessage(msgCmd.Topic, msgCmd.Partition, &msgCmd.Messages[i]); errResp != "" {
			return errResp
		}
	}

	if len(msgCmd.Messages) > 0 {
		if err := p.ReplicaAppend(msgCmd.Messages); err != nil {
			return fmt.Sprintf("ERROR: replica_append_failed reason=%q", err.Error())
		}
		p.FlushDisk()
		if err := ch.indexReplicatedEventSourceMessages(msgCmd.Topic, msgCmd.Partition, msgCmd.Messages); err != nil {
			return fmt.Sprintf("ERROR: replica_index_failed reason=%q", err.Error())
		}
	}
	if msgCmd.CommitHWM != nil {
		if err := p.ApplyReplicaHWM(*msgCmd.CommitHWM); err != nil {
			return fmt.Sprintf("ERROR: invalid_commit_watermark reason=%q", err.Error())
		}
		p.FlushDisk()
	}
	return fmt.Sprintf("OK leo=%d hwm=%d", p.NextOffset(), p.GetHWM())
}

// HandleBatchMessage processes PUBLISH of multiple messages.
func (ch *CommandHandler) HandleBatchMessage(data []byte, conn net.Conn, ctx ...*ClientContext) (string, error) {
	var clientCtx *ClientContext
	if len(ctx) > 0 {
		clientCtx = ctx[0]
	}
	batch, err := util.DecodeBatchMessages(data)
	if err != nil {
		util.Error("Batch message decoding failed: %v", err)
		return fmt.Sprintf("ERROR: batch_decode_failed reason=%q", err.Error()), nil
	}

	acks := batch.Acks
	if acks == "" {
		acks = "1"
	}

	acksLower := strings.ToLower(acks)
	if acksLower != "0" && acksLower != "1" && acksLower != "-1" && acksLower != "all" {
		return fmt.Sprintf("ERROR: invalid_acks value=%s", acks), nil
	}

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
					time.Sleep(retryDelay)
				}
				lastErr = forwardErr
			}

			return ch.errorResponse(fmt.Sprintf("failed to forward BATCH to partition leader after %d attempts: %v", maxRetries, lastErr)), nil
		}

		util.Debug("Processing BATCH locally as Partition leader for %s:%d", batch.Topic, batch.Partition)

		t := ch.waitForTopic(batch.Topic)
		if t == nil {
			util.Error("Batch process failed: topic '%s' not found", batch.Topic)
			return fmt.Sprintf("ERROR: topic_not_found topic=%s", batch.Topic), nil
		}
		if authResp := ch.authorizeTopicWrite(t.Policy, clientCtx); authResp != "" {
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

		effectiveIdempotent := batch.IsIdempotent || ch.Config.EnableIdempotence
		scope := "partition"

		releaseWrite, err := ch.preparePartitionLeader(batch.Topic, batch.Partition, p)
		if err != nil {
			return ch.errorResponse(err.Error()), nil
		}
		defer releaseWrite()

		markLeaderOffsetsUnassigned(batch.Messages)
		// Duplicate producer sequences remain unassigned and are acknowledged
		// without another replication round.
		if err := p.EnqueueBatchLeader(batch.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append batch locally: %v", err)), nil
		}
		appended := appendedLeaderMessages(batch.Messages)
		if len(appended) == 0 {
			if nextOffset := p.NextOffset(); nextOffset > 0 {
				lastOffset = nextOffset - 1
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

		p.FlushDisk()
		minISR := ch.Config.MinInSyncReplicas
		if minISR < 1 {
			minISR = 1
		}

		msgCmd := types.MessageCommand{
			Topic:         batch.Topic,
			Partition:     batch.Partition,
			IsIdempotent:  effectiveIdempotent,
			SequenceScope: scope,
			Messages:      appended,
			Acks:          acks,
		}

		err = ch.Cluster.ReplicateToFollowers(batch.Topic, batch.Partition, msgCmd, minISR)
		if err != nil {
			return ch.errorResponse(fmt.Sprintf("batch replication failed (offset=%d): %v", lastOffset, err)), nil
		}
		commitHWM := p.NextOffset()
		if err := ch.commitPartitionHWM(batch.Topic, batch.Partition, commitHWM); err != nil {
			return ch.errorResponse(fmt.Sprintf("batch commit watermark failed (offset=%d): %v", lastOffset, err)), nil
		}
		if err := p.ApplyReplicaHWM(commitHWM); err != nil {
			return ch.errorResponse(fmt.Sprintf("local commit watermark failed: %v", err)), nil
		}
		p.FlushDisk()

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
		t := ch.waitForTopic(batch.Topic)
		if t == nil {
			return fmt.Sprintf("ERROR: topic_not_found topic=%s", batch.Topic), nil
		}
		if authResp := ch.authorizeTopicWrite(t.Policy, clientCtx); authResp != "" {
			return fmt.Sprintf("%s topic=%s", authResp, batch.Topic), nil
		}
		p, err := t.GetPartition(batch.Partition)
		if err != nil {
			return fmt.Sprintf("ERROR: partition_not_found partition=%d", batch.Partition), nil
		}

		if acks == "0" {
			err = p.EnqueueBatch(batch.Messages)
			if err != nil {
				return ch.errorResponse(fmt.Sprintf("acks=0 batch publish failed: %v", err)), err
			}
			return "OK", nil
		}
		err = p.EnqueueBatchSync(batch.Messages)
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
		return nil, fmt.Sprintf("ERROR: invalid_control_batch_%s reason=%q", field, err.Error())
	}
	return decoded, ""
}
