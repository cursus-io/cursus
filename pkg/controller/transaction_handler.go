package controller

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func (ch *CommandHandler) handleBeginTxn(cmd string) string {
	args := parseKeyValueArgs(cmd[len("BEGIN_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=BEGIN_TXN"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
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
	if err := ch.syncTransactionState(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=open producerId=%s epoch=%d", txnID, producerID, epoch)
}

func (ch *CommandHandler) handleTxnPublish(cmd string) string {
	args := parseKeyValueArgs(cmd[len("TXN_PUBLISH "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=TXN_PUBLISH"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=TXN_PUBLISH"
	}
	message := args["message"]
	if message == "" {
		return "ERROR: missing_message command=TXN_PUBLISH"
	}
	producerID, epoch, errResp := parseTxnProducerEpoch(args, "TXN_PUBLISH")
	if errResp != "" {
		return errResp
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

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic_not_found topic=%s", topicName)
	}
	if !t.Policy.CanWrite() {
		return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", topicName)
	}
	msg := types.Message{Payload: message, ProducerID: producerID, SeqNum: seqNum, Epoch: epoch, Key: args["key"], TransactionalID: txnID, TransactionState: types.TransactionStateOpen}
	if partition < 0 {
		partition = t.GetPartitionForMessage(msg)
	}
	if _, err := t.GetPartition(partition); err != nil {
		return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
	}
	if err := ch.TxnManager.AddMessage(txnID, producerID, epoch, transaction.MessageOperation{Topic: topicName, Partition: partition, Message: msg}); err != nil {
		return fmt.Sprintf("ERROR: transaction_publish_failed reason=%q", err.Error())
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s staged_messages=1 topic=%s partition=%d", txnID, topicName, partition)
}

func (ch *CommandHandler) handleSendOffsetsToTxn(cmd string) string {
	args := parseKeyValueArgs(cmd[len("SEND_OFFSETS_TO_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=SEND_OFFSETS_TO_TXN"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	topicName := args["topic"]
	if topicName == "" {
		return "ERROR: missing_topic command=SEND_OFFSETS_TO_TXN"
	}
	groupID := args["group"]
	if groupID == "" {
		return "ERROR: missing_group command=SEND_OFFSETS_TO_TXN"
	}
	producerID, epoch, errResp := parseTxnProducerEpoch(args, "SEND_OFFSETS_TO_TXN")
	if errResp != "" {
		return errResp
	}
	memberID := args["member"]
	if memberID == "" {
		return "ERROR: missing_member command=SEND_OFFSETS_TO_TXN"
	}
	generation, genErr := strconv.Atoi(args["generation"])
	if genErr != nil {
		return "ERROR: invalid_generation command=SEND_OFFSETS_TO_TXN"
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
		if errResp := ch.Coordinator.ValidateOwnershipFailure(groupID, memberID, generation, partition); errResp != "" {
			return errResp
		}
		ops = append(ops, transaction.OffsetOperation{Topic: offsetTopic, Group: groupID, Member: memberID, Generation: generation, Partition: partition, Offset: offset})
	}
	if err := ch.TxnManager.AddOffsets(txnID, producerID, epoch, ops); err != nil {
		return fmt.Sprintf("ERROR: transaction_offsets_failed reason=%q", err.Error())
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s staged_offsets=%d", txnID, len(ops))
}

func (ch *CommandHandler) handleEndTxn(cmd string) string {
	args := parseKeyValueArgs(cmd[len("END_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=END_TXN"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	producerID, epoch, errResp := parseTxnProducerEpoch(args, "END_TXN")
	if errResp != "" {
		return errResp
	}
	result := strings.ToLower(firstNonEmpty(args["result"], args["action"], args["state"]))
	if result == "" {
		result = "commit"
	}
	current, statusErr := ch.TxnManager.ValidateOwner(txnID, producerID, epoch)
	if statusErr != nil {
		return fmt.Sprintf("ERROR: transaction_not_found reason=%q", statusErr.Error())
	}
	if result == "abort" {
		if current.State == transaction.StateCommitted {
			return fmt.Sprintf("ERROR: transaction_already_committed transactional_id=%s", txnID)
		}
		if current.State == transaction.StateAborted {
			return fmt.Sprintf("OK transactional_id=%s state=aborted", txnID)
		}
		if err := ch.TxnManager.Abort(txnID, producerID, epoch); err != nil {
			return fmt.Sprintf("ERROR: transaction_abort_failed reason=%q", err.Error())
		}
		if err := ch.syncTransactionState(txnID); err != nil {
			return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
		}
		return fmt.Sprintf("OK transactional_id=%s state=aborted", txnID)
	}
	if result != "commit" {
		return fmt.Sprintf("ERROR: invalid_transaction_result value=%s", result)
	}

	if current.State == transaction.StateCommitted {
		return fmt.Sprintf("OK transactional_id=%s state=committed messages=%d offsets=%d", txnID, len(current.Messages), len(current.Offsets))
	}
	if current.State == transaction.StateAborted {
		return fmt.Sprintf("ERROR: transaction_aborted transactional_id=%s", txnID)
	}

	tx, err := ch.TxnManager.PrepareCommit(txnID, producerID, epoch)
	if err != nil {
		return fmt.Sprintf("ERROR: transaction_prepare_failed reason=%q", err.Error())
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	if err := ch.applyTransaction(tx); err != nil {
		_ = ch.TxnManager.Abort(txnID, producerID, epoch)
		_ = ch.syncTransactionState(txnID)
		return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q", err.Error())
	}
	if err := ch.TxnManager.Commit(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q", err.Error())
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=committed messages=%d offsets=%d", txnID, len(tx.Messages), len(tx.Offsets))
}

func (ch *CommandHandler) handleTxnStatus(cmd string) string {
	args := parseKeyValueArgs(cmd[len("TXN_STATUS "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=TXN_STATUS"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
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
		if errResp := ch.Coordinator.ValidateOwnershipFailure(op.Group, op.Member, op.Generation, op.Partition); errResp != "" {
			return fmt.Errorf("%s", errResp)
		}
		if current, ok := ch.Coordinator.GetOffset(op.Group, op.Topic, op.Partition); ok && op.Offset < current {
			return fmt.Errorf("offset regression group=%s topic=%s partition=%d current=%d got=%d", op.Group, op.Topic, op.Partition, current, op.Offset)
		}
	}
	for _, op := range tx.Messages {
		if err := ch.publishCommittedTransactionMessage(op); err != nil {
			return err
		}
	}
	for _, op := range tx.Offsets {
		if err := ch.commitTransactionOffset(op); err != nil {
			return err
		}
	}
	return nil
}

func (ch *CommandHandler) publishCommittedTransactionMessage(op transaction.MessageOperation) error {
	msg := op.Message
	msg.TransactionState = types.TransactionStateCommitted
	msg.TransactionMarker = types.TransactionMarkerNone
	if ch.isDistributed() {
		cmd := fmt.Sprintf("PUBLISH topic=%s acks=1 producerId=%s partition=%d seqNum=%d epoch=%d", op.Topic, msg.ProducerID, op.Partition, msg.SeqNum, msg.Epoch)
		if msg.Key != "" {
			cmd += fmt.Sprintf(" key=%s", msg.Key)
		}
		if msg.TransactionalID != "" {
			cmd += fmt.Sprintf(" transactional_id=%s transaction_state=%s", msg.TransactionalID, msg.TransactionState)
		}
		cmd += " message=" + msg.Payload
		resp := ch.handlePublish(cmd)
		if !strings.HasPrefix(resp, "OK") && !strings.HasPrefix(resp, "{") {
			return fmt.Errorf("%s", resp)
		}
		return nil
	}
	return ch.TopicManager.PublishToPartitionWithAck(op.Topic, op.Partition, &msg)
}

func (ch *CommandHandler) commitTransactionOffset(op transaction.OffsetOperation) error {
	if ch.isDistributed() && ch.Cluster != nil && ch.Cluster.Router != nil {
		cmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d member=%s generation=%d", op.Topic, op.Partition, op.Group, op.Offset, op.Member, op.Generation)
		encodedCmd := util.EncodeMessage("", cmd)
		resp, err := ch.Cluster.Router.ForwardToCoordinator(op.Group, string(encodedCmd))
		if err != nil {
			return err
		}
		if !strings.HasPrefix(resp, "OK") {
			return fmt.Errorf("%s", resp)
		}
		return nil
	}
	if err := ch.Coordinator.CommitOffset(op.Group, op.Topic, op.Partition, op.Offset); err != nil {
		return err
	}
	ch.recordConsumerLag(op.Topic, op.Partition, op.Offset, op.Group)
	return nil
}

func (ch *CommandHandler) ensureTransactionCoordinator(txnID string) string {
	if !ch.isDistributed() {
		return ""
	}
	coordAddr, isCoord := ch.checkCoordinator(transactionCoordinatorKey(txnID))
	if !isCoord {
		return notCoordinatorResponse(coordAddr)
	}
	return ""
}

func (ch *CommandHandler) syncTransactionState(txnID string) error {
	if !ch.isDistributed() {
		return nil
	}
	snapshots := ch.TxnManager.ExportState()
	snap := snapshots[txnID]
	if snap == nil {
		return fmt.Errorf("transaction %s not found", txnID)
	}
	_, err := ch.applyViaLeader("TXN_SYNC", map[string]interface{}{"transaction": snap})
	return err
}

func transactionCoordinatorKey(txnID string) string {
	return "txn:" + txnID
}

func parseTxnProducerEpoch(args map[string]string, command string) (string, int64, string) {
	producerID := firstNonEmpty(args["producerId"], args["producer_id"])
	if producerID == "" {
		return "", 0, fmt.Sprintf("ERROR: missing_producer_id command=%s", command)
	}
	epoch, err := parseOptionalInt64(args["epoch"])
	if err != nil {
		return "", 0, fmt.Sprintf("ERROR: invalid_epoch reason=%q", err.Error())
	}
	return producerID, epoch, ""
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
