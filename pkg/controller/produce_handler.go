package controller

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

// handlePublish processes PUBLISH command
func (ch *CommandHandler) handlePublish(cmd string) string {
	args := parseKeyValueArgs(cmd[8:])
	var err error

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing topic parameter"
	}

	message, ok := args["message"]
	if !ok || message == "" {
		return "ERROR: missing message parameter"
	}

	producerID, ok := args["producerId"]
	if !ok || producerID == "" {
		return "ERROR: missing producerID parameter"
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
			return fmt.Sprintf("ERROR: invalid isIdempotent value: %s. Expected 'true' or 'false'", val)
		}
	}

	acksLower := strings.ToLower(acks)
	if acksLower != "0" && acksLower != "1" && acksLower != "-1" && acksLower != "all" {
		return fmt.Sprintf("ERROR: invalid acks value: %s. Expected -1 (all), 0, or 1", acks)
	}

	var seqNum uint64
	if seqNumStr, ok := args["seqNum"]; ok {
		seqNum, err = strconv.ParseUint(seqNumStr, 10, 64)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid seqNum: %v", err)
		}
	}

	var epoch int64
	if epochStr, ok := args["epoch"]; ok {
		epoch, err = strconv.ParseInt(epochStr, 10, 64)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid epoch: %v", err)
		}
	}

	var ackResp types.AckResponse
	tm := ch.TopicManager
	t := tm.GetTopic(topicName)
	if t == nil {
		const maxRetries = 5
		const retryDelay = 100 * time.Millisecond

		util.Warn("Topic '%s' not found. Checking if creation is pending...", topicName)

		found := false
		for i := 0; i < maxRetries; i++ {
			t = tm.GetTopic(topicName)
			if t != nil {
				found = true
				break
			}
			time.Sleep(retryDelay)
		}

		if !found {
			util.Warn("ch publish: topic '%s' does not exist after retries", topicName)
			return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
		}
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
			return fmt.Sprintf("ERROR: PARTITION_NOT_FOUND %d", partition)
		}

		assignedOffset := p.ReserveOffsets(1)
		msg.Offset = assignedOffset
		effectiveIdempotent := isIdempotent || ch.Config.EnableIdempotence

		scope := "global"
		messageData := types.MessageCommand{
			Topic:         topicName,
			Partition:     partition,
			IsIdempotent:  effectiveIdempotent,
			SequenceScope: scope,
			Messages: []types.Message{
				{
					Offset:     assignedOffset,
					Payload:    message,
					ProducerID: producerID,
					SeqNum:     seqNum,
					Epoch:      epoch,
				},
			},
			Acks: acks,
		}

		// 1. Append locally
		if err := p.EnqueueBatch(messageData.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append locally: %v", err))
		}

		// 2. Replicate to followers if acks != 0
		if acksLower != "0" {
			minISR := 1
			if acksLower == "-1" || acksLower == "all" {
				minISR = ch.Config.MinInSyncReplicas
			}

			err = ch.Cluster.ReplicateToFollowers(topicName, partition, messageData, minISR)
			if err != nil {
				return ch.errorResponse(fmt.Sprintf("replication failed: %v", err))
			}
		}

		// 3. Update HWM and LEO locally
		p.SetHWM(assignedOffset + 1)
		p.UpdateLEO(assignedOffset + 1)

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
	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if leader := ch.Cluster.RaftManager.GetLeaderAddress(); leader != "" {
			ackResp.Leader = leader
		}
	}

	respBytes, err := json.Marshal(ackResp)
	if err != nil {
		util.Error("Failed to marshal response: %v", err)
		return "ERROR: internal marshal error"
	}
	return string(respBytes)
}

func (ch *CommandHandler) handleReplicateMessage(cmd string) string {
	// Format: REPLICATE_MESSAGE payload=<json_MessageCommand>
	idx := strings.Index(cmd, "payload=")
	if idx == -1 {
		return "ERROR: missing payload"
	}

	payload := cmd[idx+8:]
	var msgCmd types.MessageCommand
	if err := json.Unmarshal([]byte(payload), &msgCmd); err != nil {
		return fmt.Sprintf("ERROR: unmarshal failed: %v", err)
	}

	t := ch.TopicManager.GetTopic(msgCmd.Topic)
	if t == nil {
		return fmt.Sprintf("ERROR: topic %s not found", msgCmd.Topic)
	}

	p, err := t.GetPartition(msgCmd.Partition)
	if err != nil {
		return fmt.Sprintf("ERROR: partition %d not found", msgCmd.Partition)
	}

	if err := p.EnqueueBatch(msgCmd.Messages); err != nil {
		return fmt.Sprintf("ERROR: enqueue failed: %v", err)
	}

	lastMsg := msgCmd.Messages[len(msgCmd.Messages)-1]
	p.SetHWM(lastMsg.Offset + 1)
	p.UpdateLEO(lastMsg.Offset + 1)

	return "OK"
}

// HandleBatchMessage processes PUBLISH of multiple messages.
func (ch *CommandHandler) HandleBatchMessage(data []byte, conn net.Conn) (string, error) {
	batch, err := util.DecodeBatchMessages(data)
	if err != nil {
		util.Error("Batch message decoding failed: %v", err)
		return fmt.Sprintf("ERROR: %v", err), nil
	}

	acks := batch.Acks
	if acks == "" {
		acks = "1"
	}

	acksLower := strings.ToLower(acks)
	if acksLower != "0" && acksLower != "1" && acksLower != "-1" && acksLower != "all" {
		return fmt.Sprintf("ERROR: invalid acks value in batch: %s. Expected -1 (all), 0, or 1", acks), nil
	}

	var respAck types.AckResponse
	var lastMsg *types.Message
	if ch.Config.EnabledDistribution && ch.Cluster != nil {
		if _, forwarded, _ := ch.isPartitionLeaderAndForward(batch.Topic, batch.Partition, "BATCH"); forwarded {
			// Note: isPartitionLeaderAndForward doesn't support binary BATCH data currently.
			// Actually HandleBatchMessage is called for binary BATCH data.
			// I need a way to forward binary data to Partition Leader.
			util.Warn("Binary batch forwarding to partition leader is not fully implemented for all cases")
			// For now, let's just forward to Raft leader if not the leader,
			// but better would be to use ForwardDataToPartitionLeader.
		}

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

		t := ch.TopicManager.GetTopic(batch.Topic)
		if t == nil {
			util.Error("Batch process failed: topic '%s' not found", batch.Topic)
			return fmt.Sprintf("ERROR: TOPIC_NOT_FOUND %s", batch.Topic), nil
		}

		p, err := t.GetPartition(batch.Partition)
		if err != nil {
			util.Error("Batch process failed: partition %d not found in topic %s", batch.Partition, batch.Topic)
			return fmt.Sprintf("ERROR: PARTITION_NOT_FOUND %d", batch.Partition), nil
		}

		batchSize := len(batch.Messages)
		startOffset := p.ReserveOffsets(batchSize)
		for i := range batch.Messages {
			batch.Messages[i].Offset = startOffset + uint64(i)
		}

		effectiveIdempotent := batch.IsIdempotent || ch.Config.EnableIdempotence
		scope := "global"

		// 1. Append locally
		if err := p.EnqueueBatch(batch.Messages); err != nil {
			return ch.errorResponse(fmt.Sprintf("failed to append batch locally: %v", err)), nil
		}

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
				return ch.errorResponse(fmt.Sprintf("batch replication failed: %v", err)), nil
			}
		}

		// 3. Update HWM and LEO locally
		lastOffset := batch.Messages[len(batch.Messages)-1].Offset
		p.SetHWM(lastOffset + 1)
		p.UpdateLEO(lastOffset + 1)

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
	if acks == "0" {
		err = ch.TopicManager.PublishBatchAsync(batch.Topic, batch.Messages)
		if err != nil {
			return ch.errorResponse(fmt.Sprintf("acks=0 batch publish failed: %v", err)), err
		}
		return "OK", nil
	}
	err = ch.TopicManager.PublishBatchSync(batch.Topic, batch.Messages)
	if err != nil {
		return ch.errorResponse(fmt.Sprintf("acks=1 batch publish failed: %v", err)), err
	}

	if len(batch.Messages) > 0 {
		lastMsg = &batch.Messages[len(batch.Messages)-1]
	} else {
		return ch.errorResponse("empty batch messages"), nil
	}

	respAck = types.AckResponse{
		Status:        "OK",
		LastOffset:    ch.TopicManager.GetLastOffset(batch.Topic, batch.Partition),
		SeqStart:      batch.Messages[0].SeqNum,
		SeqEnd:        lastMsg.SeqNum,
		ProducerID:    lastMsg.ProducerID,
		ProducerEpoch: lastMsg.Epoch,
	}

Respond:
	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if leader := ch.Cluster.RaftManager.GetLeaderAddress(); leader != "" {
			respAck.Leader = leader
		}
	}

	ackBytes, err := json.Marshal(respAck)
	if err != nil {
		util.Error("Failed to marshal AckResponse: %v", err)
		return "ERROR: internal marshal error", nil
	}

	responseStr := string(ackBytes)
	util.Debug("Broker Sending Batch Ack (Topic: %s): %s", batch.Topic, responseStr)
	return responseStr, nil
}
