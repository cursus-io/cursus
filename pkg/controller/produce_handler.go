package controller

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// handlePublish processes PUBLISH command
func (ch *CommandHandler) handlePublish(cmd string) string {
	args := parseKeyValueArgs(cmd[8:])
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

	var ackResp types.AckResponse
	t := ch.waitForTopic(topicName)
	if t == nil {
		util.Warn("ch publish: topic '%s' does not exist after retries", topicName)
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.Policy.CanWrite() {
		return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", topicName)
	}

	msg := &types.Message{
		Payload:    message,
		ProducerID: producerID,
		SeqNum:     seqNum,
		Epoch:      epoch,
	}

	partition := t.GetPartitionForMessage(*msg)

	if ch.Config.EnabledDistribution && ch.Cluster != nil {
		if resp, forwarded, _ := ch.isPartitionLeaderAndForward(topicName, partition, cmd); forwarded {
			return resp
		}

		p, err := t.GetPartition(partition)
		if err != nil {
			util.Error("Publish failed: partition %d not found in topic %s: %v", partition, topicName, err)
			return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
		}

		effectiveIdempotent := isIdempotent || ch.Config.EnableIdempotence

		scope := "partition"
		messageData := types.MessageCommand{
			Topic:         topicName,
			Partition:     partition,
			IsIdempotent:  effectiveIdempotent,
			SequenceScope: scope,
			Messages: []types.Message{
				{
					Payload:    message,
					ProducerID: producerID,
					SeqNum:     seqNum,
					Epoch:      epoch,
				},
			},
			Acks: acks,
		}

		// 1. Append locally (LEO advances, HWM stays until replication succeeds)
		if err := p.EnqueueBatchLeader(messageData.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append locally: %v", err))
		}

		// Read the offset assigned by EnqueueBatch
		assignedOffset := messageData.Messages[0].Offset

		// 2. Replicate to followers if acks != 0
		if acksLower != "0" {
			minISR := 1
			if acksLower == "-1" || acksLower == "all" {
				minISR = ch.Config.MinInSyncReplicas
			}

			err = ch.Cluster.ReplicateToFollowers(topicName, partition, messageData, minISR)
			if err != nil {
				return ch.errorResponse(fmt.Sprintf("replication failed (offset=%d): %v", assignedOffset, err))
			}
		}

		// Ensure data is on disk before ACK
		p.FlushDisk()

		// Advance HWM now that replication and flush are complete
		p.AdvanceHWM()

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
			err = ch.TopicManager.Publish(topicName, msg)
			if err != nil {
				util.Error("acks=0 publish failed (stand-alone): %v", err)
			}
			return "OK"
		}
		err = ch.TopicManager.PublishWithAck(topicName, msg)
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
	// Format: REPLICATE_MESSAGE payload=<json_MessageCommand>
	idx := strings.Index(cmd, "payload=")
	if idx == -1 {
		return "ERROR: missing_payload command=REPLICATE_MESSAGE"
	}

	payload := cmd[idx+8:]
	var msgCmd types.MessageCommand
	if err := json.Unmarshal([]byte(payload), &msgCmd); err != nil {
		return fmt.Sprintf("ERROR: unmarshal_failed reason=%q", err.Error())
	}

	t := ch.TopicManager.GetTopic(msgCmd.Topic)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", msgCmd.Topic)
	}

	p, err := t.GetPartition(msgCmd.Partition)
	if err != nil {
		return fmt.Sprintf("ERROR: partition_not_found partition=%d", msgCmd.Partition)
	}

	// Guard against empty messages to prevent index-out-of-range panic
	if len(msgCmd.Messages) == 0 {
		return "ERROR: empty_messages command=REPLICATE_MESSAGE"
	}

	if err := p.ReplicaAppend(msgCmd.Messages); err != nil {
		return fmt.Sprintf("ERROR: replica_append_failed reason=%q", err.Error())
	}
	if err := ch.indexReplicatedEventSourceMessages(msgCmd.Topic, msgCmd.Partition, msgCmd.Messages); err != nil {
		return fmt.Sprintf("ERROR: replica_index_failed reason=%q", err.Error())
	}

	return "OK"
}

// HandleBatchMessage processes PUBLISH of multiple messages.
func (ch *CommandHandler) HandleBatchMessage(data []byte, conn net.Conn) (string, error) {
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
		if !t.Policy.CanWrite() {
			return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", batch.Topic), nil
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

		// 1. Append locally (LEO advances, HWM stays until replication succeeds)
		if err := p.EnqueueBatchLeader(batch.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append batch locally: %v", err)), nil
		}

		// Read offsets assigned by EnqueueBatch
		lastOffset = batch.Messages[len(batch.Messages)-1].Offset

		// 2. Replicate to followers if acks != 0
		if acksLower != "0" {
			minISR := 1
			if acksLower == "-1" || acksLower == "all" {
				minISR = ch.Config.MinInSyncReplicas
			}

			msgCmd := types.MessageCommand{
				Topic:         batch.Topic,
				Partition:     batch.Partition,
				IsIdempotent:  effectiveIdempotent,
				SequenceScope: scope,
				Messages:      batch.Messages,
				Acks:          acks,
			}

			err = ch.Cluster.ReplicateToFollowers(batch.Topic, batch.Partition, msgCmd, minISR)
			if err != nil {
				return ch.errorResponse(fmt.Sprintf("batch replication failed (offset=%d): %v", lastOffset, err)), nil
			}
		}

		// Ensure data is on disk before ACK
		p.FlushDisk()

		// Advance HWM now that replication and flush are complete
		p.AdvanceHWM()

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
		if !t.Policy.CanWrite() {
			return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", batch.Topic), nil
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
