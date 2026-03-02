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
	if ch.Config.EnabledDistribution && ch.Cluster != nil && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

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
		if !ch.isAuthorizedForPartition(topicName, partition) {
			util.Error("NOT_AUTHORIZED_FOR_PARTITION %s:%d", topicName, partition)
			return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_PARTITION %s:%d", topicName, partition)
		}

		p, err := t.GetPartition(partition)
		if err != nil {
			util.Error("Publish failed: partition %d not found in topic %s: %v", partition, topicName, err)
			return fmt.Sprintf("ERROR: PARTITION_NOT_FOUND %d", partition)
		}

		assignedOffset := p.ReserveOffsets(1)
		msg.Offset = assignedOffset
		if acks == "-1" || acksLower == "all" {
			ackResp, err = ch.Cluster.RaftManager.ReplicateWithQuorum(topicName, partition, *msg, ch.Config.MinInSyncReplicas)

			if err != nil {
				return ch.errorResponse(fmt.Sprintf("failed to replicate with quorum (acks=-1): %v", err))
			}
			goto Respond

		} else {
			messageData := types.MessageCommand{
				Topic:        topicName,
				Partition:    partition,
				IsIdempotent: false,
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

			jsonData, err := json.Marshal(messageData)
			if err != nil {
				util.Error("Failed to marshal: %v", err)
				return "ERROR: internal marshal error"
			}

			if acks == "0" {
				err = ch.Cluster.RaftManager.ApplyCommand("MESSAGE", jsonData)
				if err != nil {
					util.Error("raft message apply failed: %s", err)
				}
				return "OK"
			}

			ackResp, err = ch.Cluster.RaftManager.ApplyResponse("MESSAGE", jsonData, 5*time.Second)
			if err != nil {
				return ch.errorResponse(fmt.Sprintf("raft apply failed: %v", err))
			}
			goto Respond
		}
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
		leader := ch.Cluster.IsLeader()
		if !leader {
			const maxRetries = 3
			const retryDelay = 200 * time.Millisecond
			var lastErr error

			for i := 0; i < maxRetries; i++ {
				resp, forwardErr := ch.Cluster.Router.ForwardDataToLeader(data)
				if forwardErr == nil {
					return resp, nil
				}

				util.Debug("Failed to forward batch to leader. Retrying (Attempt %d/%d). Error: %v", i+1, maxRetries, forwardErr)

				if i < maxRetries-1 {
					time.Sleep(retryDelay)
				}
				lastErr = forwardErr
			}

			return ch.errorResponse(fmt.Sprintf("failed to forward BATCH to leader after %d attempts: %v", maxRetries, lastErr)), nil
		} else {
			leaderAddr := ch.Cluster.RaftManager.GetLeaderAddress()
			util.Debug("Processing BATCH locally as leader: %s", leaderAddr)
		}

		if !ch.isAuthorizedForPartition(batch.Topic, batch.Partition) {
			util.Error("NOT_AUTHORIZED_FOR_PARTITION %s:%d", batch.Topic, batch.Partition)
			return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_PARTITION %s:%d", batch.Topic, batch.Partition), nil
		}

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

		if acks == "-1" || acksLower == "all" {
			respAck, err = ch.Cluster.RaftManager.ReplicateBatchWithQuorum(batch.Topic, batch.Partition, batch.Messages, ch.Config.MinInSyncReplicas, acks)
			if err != nil {
				return ch.errorResponse(err.Error()), nil
			}
			goto Respond
		} else {
			batchData, err := json.Marshal(batch)
			if err != nil {
				util.Error("Failed to marshal: %v", err)
				return ch.errorResponse("batch marshal error"), nil
			}

			if acks == "0" {
				err = ch.Cluster.RaftManager.ApplyCommand("BATCH", batchData)
				if err != nil {
					util.Error("raft batch apply failed: %s", err)
				}
				return "OK", nil
			}

			respAck, err = ch.Cluster.RaftManager.ApplyResponse("BATCH", batchData, 5*time.Second)
			if err != nil {
				return ch.errorResponse(err.Error()), nil
			}
			goto Respond
		}
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
