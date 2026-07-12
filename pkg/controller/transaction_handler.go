package controller

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
)

func (ch *CommandHandler) handleBeginTxn(cmd string) string {
	args := parseKeyValueArgs(cmd[len("BEGIN_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=BEGIN_TXN"
	}
	producerID := firstNonEmpty(args["producerId"], args["producer_id"])
	if producerID == "" {
		return "ERROR: missing_producer_id command=BEGIN_TXN"
	}
	epoch, err := parseOptionalInt64(args["epoch"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_epoch reason=%q", err.Error())
	}
	if err := ch.TxnManager.Begin(txnID, producerID, epoch); err != nil {
		return fmt.Sprintf("ERROR: transaction_begin_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=open producerId=%s epoch=%d", txnID, producerID, epoch)
}

func (ch *CommandHandler) handleTxnPublish(cmd string) string {
	args := parseKeyValueArgs(cmd[len("TXN_PUBLISH "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=TXN_PUBLISH"
	}
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=TXN_PUBLISH"
	}
	message := args["message"]
	if message == "" {
		return "ERROR: missing_message command=TXN_PUBLISH"
	}
	producerID := firstNonEmpty(args["producerId"], args["producer_id"])
	if producerID == "" {
		return "ERROR: missing_producer_id command=TXN_PUBLISH"
	}
	partition := -1
	if partitionStr := args["partition"]; partitionStr != "" {
		parsed, err := strconv.Atoi(partitionStr)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid_partition reason=%q", err.Error())
		}
		partition = parsed
	}
	seqNum, err := parseOptionalUint64(args["seqNum"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_seq_num reason=%q", err.Error())
	}
	epoch, err := parseOptionalInt64(args["epoch"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_epoch reason=%q", err.Error())
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.Policy.CanWrite() {
		return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", topicName)
	}
	msg := types.Message{Payload: message, ProducerID: producerID, SeqNum: seqNum, Epoch: epoch, Key: args["key"]}
	if partition < 0 {
		partition = t.GetPartitionForMessage(msg)
	}
	if _, err := t.GetPartition(partition); err != nil {
		return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
	}
	if err := ch.TxnManager.AddMessage(txnID, transaction.MessageOperation{Topic: topicName, Partition: partition, Message: msg}); err != nil {
		return fmt.Sprintf("ERROR: transaction_publish_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s staged_messages=1 topic=%s partition=%d", txnID, topicName, partition)
}

func (ch *CommandHandler) handleSendOffsetsToTxn(cmd string) string {
	args := parseKeyValueArgs(cmd[len("SEND_OFFSETS_TO_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=SEND_OFFSETS_TO_TXN"
	}
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=SEND_OFFSETS_TO_TXN"
	}
	groupID := args["group"]
	if groupID == "" {
		return "ERROR: missing_group command=SEND_OFFSETS_TO_TXN"
	}
	if ch.Coordinator == nil {
		return "ERROR: offset_manager_not_available"
	}
	offsetTopic, offsetTopicErr := ch.resolveGroupOffsetTopic(groupID, topicName)
	if offsetTopicErr != "" {
		return offsetTopicErr
	}
	offsets, err := parseTxnOffsetPairs(cmd)
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_txn_offsets reason=%q", err.Error())
	}
	ops := make([]transaction.OffsetOperation, 0, len(offsets))
	for partition, offset := range offsets {
		ops = append(ops, transaction.OffsetOperation{Topic: offsetTopic, Group: groupID, Partition: partition, Offset: offset})
	}
	if err := ch.TxnManager.AddOffsets(txnID, ops); err != nil {
		return fmt.Sprintf("ERROR: transaction_offsets_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s staged_offsets=%d", txnID, len(ops))
}

func (ch *CommandHandler) handleEndTxn(cmd string) string {
	args := parseKeyValueArgs(cmd[len("END_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=END_TXN"
	}
	result := strings.ToLower(firstNonEmpty(args["result"], args["action"], args["state"]))
	if result == "" {
		result = "commit"
	}
	if result == "abort" {
		if err := ch.TxnManager.Abort(txnID); err != nil {
			return fmt.Sprintf("ERROR: transaction_abort_failed reason=%q", err.Error())
		}
		return fmt.Sprintf("OK transactional_id=%s state=aborted", txnID)
	}
	if result != "commit" {
		return fmt.Sprintf("ERROR: invalid_transaction_result value=%s", result)
	}

	tx, err := ch.TxnManager.PrepareCommit(txnID)
	if err != nil {
		return fmt.Sprintf("ERROR: transaction_prepare_failed reason=%q", err.Error())
	}
	if err := ch.applyTransaction(tx); err != nil {
		_ = ch.TxnManager.Abort(txnID)
		return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q", err.Error())
	}
	if err := ch.TxnManager.Commit(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=committed messages=%d offsets=%d", txnID, len(tx.Messages), len(tx.Offsets))
}

func (ch *CommandHandler) handleTxnStatus(cmd string) string {
	args := parseKeyValueArgs(cmd[len("TXN_STATUS "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=TXN_STATUS"
	}
	tx, err := ch.TxnManager.Status(txnID)
	if err != nil {
		return fmt.Sprintf("ERROR: transaction_not_found reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=%s messages=%d offsets=%d", tx.ID, tx.State, len(tx.Messages), len(tx.Offsets))
}

func (ch *CommandHandler) applyTransaction(tx *transaction.Transaction) error {
	ch.txnApplyMu.Lock()
	defer ch.txnApplyMu.Unlock()

	for _, op := range tx.Messages {
		t := ch.TopicManager.GetTopic(op.Topic)
		if t == nil {
			return fmt.Errorf("topic %s not found", op.Topic)
		}
		if !t.Policy.CanWrite() {
			return fmt.Errorf("NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", op.Topic)
		}
		if _, err := t.GetPartition(op.Partition); err != nil {
			return err
		}
	}
	for _, op := range tx.Offsets {
		if ch.Coordinator == nil {
			return fmt.Errorf("offset manager not available")
		}
		if current, ok := ch.Coordinator.GetOffset(op.Group, op.Topic, op.Partition); ok && op.Offset < current {
			return fmt.Errorf("offset regression group=%s topic=%s partition=%d current=%d got=%d", op.Group, op.Topic, op.Partition, current, op.Offset)
		}
	}
	for _, op := range tx.Messages {
		msg := op.Message
		if err := ch.TopicManager.PublishToPartitionWithAck(op.Topic, op.Partition, &msg); err != nil {
			return err
		}
	}
	for _, op := range tx.Offsets {
		if err := ch.Coordinator.CommitOffset(op.Group, op.Topic, op.Partition, op.Offset); err != nil {
			return err
		}
		ch.recordConsumerLag(op.Topic, op.Partition, op.Offset, op.Group)
	}
	return nil
}

func parseTxnOffsetPairs(cmd string) (map[int]uint64, error) {
	partsIdx := strings.LastIndex(cmd, " ")
	if partsIdx == -1 {
		return nil, fmt.Errorf("missing offset pairs")
	}
	partitionData := cmd[partsIdx+1:]
	if !strings.Contains(partitionData, ":") {
		return nil, fmt.Errorf("missing offset pairs")
	}
	pairs := strings.Split(partitionData, ",")
	offsets := make(map[int]uint64, len(pairs))
	for _, pair := range pairs {
		kv := strings.Split(pair, ":")
		if len(kv) != 2 || !strings.HasPrefix(kv[0], "P") {
			return nil, fmt.Errorf("invalid pair %s", pair)
		}
		partition, err := strconv.Atoi(strings.TrimPrefix(kv[0], "P"))
		if err != nil {
			return nil, err
		}
		offset, err := strconv.ParseUint(kv[1], 10, 64)
		if err != nil {
			return nil, err
		}
		offsets[partition] = offset
	}
	if len(offsets) == 0 {
		return nil, fmt.Errorf("no offsets supplied")
	}
	return offsets, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func parseOptionalUint64(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseOptionalInt64(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}
