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
	first := cmd.Messages[0]
	last := cmd.Messages[len(cmd.Messages)-1]

	f.mu.Lock()
	if cmd.IsIdempotent {
		exists, lastSeq := f.getProducerSequence(cmd.Topic, cmd.Partition, first.ProducerID)
		if exists && int64(last.SeqNum) <= lastSeq {
			f.mu.Unlock()
			util.Debug("FSM: Duplicate detected for idempotent producer %s, skipping", first.ProducerID)
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

	f.mu.Lock()
	f.updateProducerState(cmd.Topic, cmd.Partition, first.ProducerID, int64(last.SeqNum))
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

	f.mu.Lock()
	_, topicExists := f.partitionMetadata[cmd.Topic]
	exists, lastSeq := f.getProducerSequence(cmd.Topic, cmd.Partition, firstMsg.ProducerID)
	f.mu.Unlock()

	if !topicExists {
		return fmt.Errorf("topic '%s' not found", cmd.Topic)
	}

	if cmd.IsIdempotent && exists && uint64(firstMsg.SeqNum) > uint64(lastSeq+1) {
		return fmt.Errorf("idempotency gap: expected %d, got %d", lastSeq+1, firstMsg.SeqNum)
	}

	for i, curr := range cmd.Messages {
		if curr.ProducerID != firstMsg.ProducerID || curr.Epoch != firstMsg.Epoch {
			return fmt.Errorf("mixed producer info at index %d", i)
		}
		if len(curr.Payload) == 0 {
			return fmt.Errorf("empty payload at index %d", i)
		}
		if i > 0 {
			if err := f.validateSequence(cmd.IsIdempotent, cmd.Messages[i-1], curr, i); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *BrokerFSM) validateSequence(isIdempotent bool, prev, curr types.Message, idx int) error {
	if curr.Offset != prev.Offset+1 {
		return fmt.Errorf("offset gap at %d: %d->%d", idx, prev.Offset, curr.Offset)
	}
	if isIdempotent && curr.SeqNum != prev.SeqNum+1 {
		return fmt.Errorf("seq gap at %d: %d->%d", idx, prev.SeqNum, curr.SeqNum)
	}
	if !isIdempotent && curr.SeqNum <= prev.SeqNum {
		return fmt.Errorf("seq not increasing at %d: %d->%d", idx, prev.SeqNum, curr.SeqNum)
	}
	return nil
}
