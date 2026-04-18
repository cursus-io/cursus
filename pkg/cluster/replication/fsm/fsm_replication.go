package fsm

import (
	"encoding/json"
	"fmt"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func (f *BrokerFSM) applyMessageCommand(jsonData string) interface{} {
	var cmd types.MessageCommand
	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("FSM: Unmarshal failed: %v", err)
		return errorAckResponse("unmarshal failed", "", 0)
	}

	if err := f.validateMessageCommand(&cmd); err != nil {
		pID, epoch := "", int64(0)
		if len(cmd.Messages) > 0 {
			pID, epoch = cmd.Messages[0].ProducerID, cmd.Messages[0].Epoch
		}
		return errorAckResponse(err.Error(), pID, epoch)
	}

	return f.applyMessageBatch(&cmd)
}

func (f *BrokerFSM) applyMessageBatch(cmd *types.MessageCommand) interface{} {
	if len(cmd.Messages) == 0 {
		return errorAckResponse("cannot process empty message batch", "", 0)
	}

	first := cmd.Messages[0]
	last := cmd.Messages[len(cmd.Messages)-1]

	partitionKey := fmt.Sprintf("%s-%d", cmd.Topic, cmd.Partition)

	f.mu.Lock()
	meta, topicExists := f.partitionMetadata[partitionKey]

	effectiveIdempotent := cmd.IsIdempotent
	if topicExists && meta.Idempotent {
		effectiveIdempotent = true
	}

	if effectiveIdempotent {
		trackPartition := -1
		if cmd.SequenceScope == "partition" {
			trackPartition = cmd.Partition
		}

		exists, lastSeq := f.getProducerSequence(cmd.Topic, trackPartition, first.ProducerID)
		if exists && int64(last.SeqNum) <= lastSeq {
			f.mu.Unlock()
			util.Debug("FSM: Duplicate detected for idempotent producer %s (Topic: %s, Scope: %s, Partition: %d), skipping", first.ProducerID, cmd.Topic, cmd.SequenceScope, trackPartition)
			return f.makeSuccessAck(&last, first.SeqNum)
		}
	}

	topic := f.tm.GetTopic(cmd.Topic)
	if topic == nil {
		f.mu.Unlock()
		return errorAckResponse(fmt.Sprintf("topic %s not found", cmd.Topic), first.ProducerID, first.Epoch)
	}
	partition, err := topic.GetPartition(cmd.Partition)
	if err != nil {
		f.mu.Unlock()
		return errorAckResponse(err.Error(), first.ProducerID, first.Epoch)
	}
	f.mu.Unlock()

	if err := partition.EnqueueBatch(cmd.Messages); err != nil {
		return errorAckResponse(err.Error(), first.ProducerID, first.Epoch)
	}

	// Re-read after EnqueueBatch since it assigns actual disk offsets
	last = cmd.Messages[len(cmd.Messages)-1]

	f.mu.Lock()
	if effectiveIdempotent {
		trackPartition := -1
		if cmd.SequenceScope == "partition" {
			trackPartition = cmd.Partition
		}
		f.updateProducerState(cmd.Topic, trackPartition, first.ProducerID, int64(last.SeqNum))
	}
	f.mu.Unlock()

	return f.makeSuccessAck(&last, first.SeqNum)
}

func (f *BrokerFSM) getProducerSequence(topic string, partition int, pID string) (bool, int64) {
	if pMap, ok := f.producerState[topic]; ok {
		if sMap, ok := pMap[partition]; ok {
			if seq, ok := sMap[pID]; ok {
				return true, seq
			}
		}
		// If searching for specific partition, fallback to check global (-1)
		if partition != -1 {
			if gMap, ok := pMap[-1]; ok {
				if seq, ok := gMap[pID]; ok {
					return true, seq
				}
			}
		}
	}
	return false, -1
}

func (f *BrokerFSM) updateProducerState(topic string, partition int, pID string, seq int64) {
	if f.producerState[topic] == nil {
		f.producerState[topic] = make(map[int]map[string]int64)
	}
	if f.producerState[topic][partition] == nil {
		f.producerState[topic][partition] = make(map[string]int64)
	}
	f.producerState[topic][partition][pID] = seq
}

func (f *BrokerFSM) makeSuccessAck(msg *types.Message, seqStart uint64) types.AckResponse {
	return types.AckResponse{
		Status:        "OK",
		LastOffset:    msg.Offset,
		ProducerID:    msg.ProducerID,
		ProducerEpoch: msg.Epoch,
		SeqStart:      seqStart,
		SeqEnd:        msg.SeqNum,
	}
}

func (f *BrokerFSM) validateMessageCommand(cmd *types.MessageCommand) error {
	if cmd.Topic == "" || len(cmd.Messages) == 0 {
		return fmt.Errorf("invalid command: missing topic or messages")
	}

	firstMsg := cmd.Messages[0]
	lastMsg := cmd.Messages[len(cmd.Messages)-1]

	partitionKey := fmt.Sprintf("%s-%d", cmd.Topic, cmd.Partition)

	f.mu.Lock()
	meta, topicExists := f.partitionMetadata[partitionKey]

	effectiveIdempotent := cmd.IsIdempotent
	if topicExists && meta.Idempotent {
		effectiveIdempotent = true
	}

	trackPartition := -1
	if cmd.SequenceScope == "partition" {
		trackPartition = cmd.Partition
	}
	exists, lastSeq := f.getProducerSequence(cmd.Topic, trackPartition, firstMsg.ProducerID)
	f.mu.Unlock()

	if !topicExists {
		return fmt.Errorf("partition metadata '%s' not found (topic=%s, partition=%d)", partitionKey, cmd.Topic, cmd.Partition)
	}

	if effectiveIdempotent {
		if cmd.SequenceScope == "partition" {
			if exists {
				if int64(lastMsg.SeqNum) <= lastSeq {
					return nil
				}
				if int64(firstMsg.SeqNum) <= lastSeq {
					return fmt.Errorf("out-of-order sequence (overlap) in partition scope: lastSeq %d, firstMsg.SeqNum %d", lastSeq, firstMsg.SeqNum)
				}
				if firstMsg.SeqNum > uint64(lastSeq+1) {
					return fmt.Errorf("idempotency gap in partition scope for producer %s: expected %d, got %d", firstMsg.ProducerID, lastSeq+1, firstMsg.SeqNum)
				}
			} else if firstMsg.SeqNum > 1 {
				return fmt.Errorf("idempotency error: first message in partition scope for producer %s must have seqNum 1, got %d", firstMsg.ProducerID, firstMsg.SeqNum)
			}
		} else {
			if exists {
				if int64(firstMsg.SeqNum) <= lastSeq {
					if int64(lastMsg.SeqNum) <= lastSeq {
						return nil // entire batch is duplicate
					}
					return fmt.Errorf("out-of-order sequence (overlap) in global scope: lastSeq %d, firstMsg.SeqNum %d", lastSeq, firstMsg.SeqNum)
				}
				if firstMsg.SeqNum > uint64(lastSeq+1) {
					return fmt.Errorf("idempotency gap in global scope for producer %s: expected %d, got %d", firstMsg.ProducerID, lastSeq+1, firstMsg.SeqNum)
				}
			} else if firstMsg.SeqNum > 1 {
				return fmt.Errorf("idempotency error: first message in global scope for producer %s must have seqNum 1, got %d", firstMsg.ProducerID, firstMsg.SeqNum)
			}
		}
	}

	for i, curr := range cmd.Messages {
		if curr.ProducerID != firstMsg.ProducerID || curr.Epoch != firstMsg.Epoch {
			return fmt.Errorf("mixed producer info at index %d", i)
		}
		if len(curr.Payload) == 0 {
			return fmt.Errorf("empty payload at index %d", i)
		}
		if i > 0 {
			if err := f.validateSequence(effectiveIdempotent, cmd.Messages[i-1], curr, i); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *BrokerFSM) validateSequence(isIdempotent bool, prev, curr types.Message, idx int) error {
	if isIdempotent && curr.SeqNum != prev.SeqNum+1 {
		return fmt.Errorf("seq gap within batch at %d: %d->%d", idx, prev.SeqNum, curr.SeqNum)
	}
	if !isIdempotent && curr.SeqNum <= prev.SeqNum {
		return fmt.Errorf("seq not increasing at %d: %d->%d", idx, prev.SeqNum, curr.SeqNum)
	}
	return nil
}
