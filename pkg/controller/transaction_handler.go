package controller

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

const transactionControlMarkerPayload = "__cursus_txn_control_marker__"

func (ch *CommandHandler) handleInitProducerID(cmd string) string {
	args := parseKeyValueArgs(cmd[len("INIT_PRODUCER_ID "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=INIT_PRODUCER_ID"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	previousState := ch.TxnManager.ExportState()
	previousSnap, hadPrevious := previousState[txnID]
	producerID, epoch, err := ch.TxnManager.InitProducer(txnID)
	if err != nil {
		return fmt.Sprintf("ERROR: init_producer_failed reason=%q", err.Error())
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		if hadPrevious {
			ch.TxnManager.ApplySnapshot(previousSnap)
		} else {
			ch.TxnManager.Delete(txnID)
		}
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s producerId=%s epoch=%d", txnID, producerID, epoch)
}

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
	seqNum, err := parseRequiredPositiveUint64(args["seqNum"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_seq_num command=TXN_PUBLISH reason=%q", err.Error())
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
		op := transaction.OffsetOperation{Topic: offsetTopic, Group: groupID, Member: memberID, Generation: generation, Partition: partition, Offset: offset}
		if err := ch.validateTransactionOffset(op, false); err != nil {
			return err.Error()
		}
		ops = append(ops, op)
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
		if err := ch.appendTransactionMarkers(current, types.TransactionMarkerAbort); err != nil {
			return fmt.Sprintf("ERROR: transaction_abort_marker_failed reason=%q", err.Error())
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
		if syncErr := ch.syncTransactionState(txnID); syncErr != nil {
			return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q sync_reason=%q", err.Error(), syncErr.Error())
		}
		return fmt.Sprintf("ERROR: transaction_commit_failed state=committing reason=%q", err.Error())
	}
	if err := ch.TxnManager.Commit(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q", err.Error())
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=committed messages=%d offsets=%d", txnID, len(tx.Messages), len(tx.Offsets))
}

func (ch *CommandHandler) RecoverPreparedTransactions() error {
	if ch.TxnManager == nil {
		return nil
	}
	pending := ch.TxnManager.TransactionsByState(transaction.StateCommitting)
	if len(pending) == 0 {
		return nil
	}
	for _, tx := range pending {
		if tx == nil {
			continue
		}
		if resp := ch.ensureTransactionCoordinator(tx.ID); resp != "" {
			util.Debug("Skipping transaction recovery for %s on non-coordinator: %s", tx.ID, resp)
			continue
		}
		if err := ch.applyTransaction(tx); err != nil {
			return fmt.Errorf("recover transaction %s: %w", tx.ID, err)
		}
		if err := ch.TxnManager.Commit(tx.ID); err != nil {
			return fmt.Errorf("mark recovered transaction %s committed: %w", tx.ID, err)
		}
		if err := ch.syncTransactionState(tx.ID); err != nil {
			return fmt.Errorf("sync recovered transaction %s: %w", tx.ID, err)
		}
		util.Info("Recovered prepared transaction %s", tx.ID)
	}
	return nil
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
		if err := ch.validateTransactionOffset(op, true); err != nil {
			return err
		}
	}
	apply := func() error {
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
		if err := ch.appendTransactionMarkers(tx, types.TransactionMarkerCommit); err != nil {
			return err
		}
		return nil
	}
	return ch.withTransactionOffsetFences(tx.Offsets, apply)
}

type transactionOffsetFence struct {
	Group      string
	Member     string
	Generation int
	Partitions []int
}

func (ch *CommandHandler) withTransactionOffsetFences(ops []transaction.OffsetOperation, apply func() error) error {
	if ch.Coordinator == nil || len(ops) == 0 || ch.isDistributed() {
		return apply()
	}
	fences := buildTransactionOffsetFences(ops)
	var run func(int) error
	run = func(idx int) error {
		if idx >= len(fences) {
			return apply()
		}
		fence := fences[idx]
		return ch.Coordinator.WithOwnershipFence(fence.Group, fence.Member, fence.Generation, fence.Partitions, func() error {
			return run(idx + 1)
		})
	}
	return run(0)
}

func (ch *CommandHandler) validateTransactionOffset(op transaction.OffsetOperation, checkRegression bool) error {
	if ch.Coordinator == nil {
		return fmt.Errorf("ERROR: offset_manager_not_available")
	}
	if ch.isDistributed() && ch.Cluster != nil && ch.Cluster.Router != nil {
		cmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d member=%s generation=%d validate_only=true", op.Topic, op.Partition, op.Group, op.Offset, op.Member, op.Generation)
		if !checkRegression {
			cmd += " ownership_only=true"
		}
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
	if errResp := ch.Coordinator.ValidateOwnershipFailure(op.Group, op.Member, op.Generation, op.Partition); errResp != "" {
		return fmt.Errorf("%s", errResp)
	}
	if checkRegression {
		if current, ok := ch.Coordinator.GetOffset(op.Group, op.Topic, op.Partition); ok && op.Offset < current {
			return fmt.Errorf("offset regression group=%s topic=%s partition=%d current=%d got=%d", op.Group, op.Topic, op.Partition, current, op.Offset)
		}
	}
	return nil
}
func buildTransactionOffsetFences(ops []transaction.OffsetOperation) []transactionOffsetFence {
	type key struct {
		group      string
		member     string
		generation int
	}
	index := make(map[key]int)
	partitionSeen := make(map[key]map[int]struct{})
	fences := make([]transactionOffsetFence, 0)
	for _, op := range ops {
		k := key{group: op.Group, member: op.Member, generation: op.Generation}
		idx, ok := index[k]
		if !ok {
			idx = len(fences)
			index[k] = idx
			partitionSeen[k] = make(map[int]struct{})
			fences = append(fences, transactionOffsetFence{Group: op.Group, Member: op.Member, Generation: op.Generation})
		}
		if _, ok := partitionSeen[k][op.Partition]; ok {
			continue
		}
		partitionSeen[k][op.Partition] = struct{}{}
		fences[idx].Partitions = append(fences[idx].Partitions, op.Partition)
	}
	return fences
}
func (ch *CommandHandler) appendTransactionMarkers(tx *transaction.Transaction, marker string) error {
	if tx == nil || len(tx.Messages) == 0 {
		return nil
	}
	state := types.TransactionStateCommitted
	if marker == types.TransactionMarkerAbort {
		state = types.TransactionStateAborted
	}

	partitions := touchedTransactionPartitions(tx)
	for _, partition := range partitions {
		msg := types.Message{
			Payload:           transactionControlMarkerPayload,
			ProducerID:        transactionMarkerProducerID(tx, marker),
			SeqNum:            1,
			Epoch:             tx.Epoch,
			TransactionalID:   tx.ID,
			TransactionState:  state,
			TransactionMarker: marker,
		}
		if err := ch.publishTransactionMarker(partition.Topic, partition.Partition, msg); err != nil {
			return err
		}
	}
	return nil
}

type transactionPartition struct {
	Topic     string
	Partition int
}

func transactionMarkerProducerID(tx *transaction.Transaction, marker string) string {
	if tx == nil {
		return "txn-marker:unknown"
	}
	return fmt.Sprintf("txn-marker:%s:%s", tx.ID, marker)
}
func touchedTransactionPartitions(tx *transaction.Transaction) []transactionPartition {
	seen := make(map[transactionPartition]struct{})
	for _, op := range tx.Messages {
		seen[transactionPartition{Topic: op.Topic, Partition: op.Partition}] = struct{}{}
	}
	out := make([]transactionPartition, 0, len(seen))
	for partition := range seen {
		out = append(out, partition)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic == out[j].Topic {
			return out[i].Partition < out[j].Partition
		}
		return out[i].Topic < out[j].Topic
	})
	return out
}

func (ch *CommandHandler) publishTransactionMarker(topicName string, partition int, msg types.Message) error {
	if ch.isDistributed() {
		cmd := fmt.Sprintf("PUBLISH topic=%s acks=1 producerId=%s partition=%d seqNum=%d epoch=%d isIdempotent=true transactional_id=%s transaction_state=%s transaction_marker=%s message=%s", topicName, msg.ProducerID, partition, msg.SeqNum, msg.Epoch, msg.TransactionalID, msg.TransactionState, msg.TransactionMarker, msg.Payload)
		resp := ch.handlePublish(cmd)
		if !strings.HasPrefix(resp, "OK") && !strings.HasPrefix(resp, "{") {
			return fmt.Errorf("%s", resp)
		}
		return nil
	}
	return ch.TopicManager.PublishToPartitionWithAckIdempotent(topicName, partition, &msg)
}
func (ch *CommandHandler) publishCommittedTransactionMessage(op transaction.MessageOperation) error {
	msg := op.Message
	msg.TransactionState = types.TransactionStateOpen
	msg.TransactionMarker = types.TransactionMarkerNone
	if ch.isDistributed() {
		cmd := fmt.Sprintf("PUBLISH topic=%s acks=1 producerId=%s partition=%d seqNum=%d epoch=%d isIdempotent=true", op.Topic, msg.ProducerID, op.Partition, msg.SeqNum, msg.Epoch)
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
	return ch.TopicManager.PublishToPartitionWithAckIdempotent(op.Topic, op.Partition, &msg)
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
	if err := ch.Coordinator.ValidateAndCommit(op.Group, op.Topic, op.Partition, op.Offset, op.Generation, op.Member); err != nil {
		return err
	}
	ch.recordConsumerLag(op.Topic, op.Partition, op.Offset, op.Group)
	return nil
}

func (ch *CommandHandler) ensureTransactionCoordinator(txnID string) string {
	if !ch.isDistributed() {
		return ""
	}
	coordAddr, isCoord := ch.checkTransactionCoordinator(txnID)
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

func parseRequiredPositiveUint64(value string) (uint64, error) {
	if value == "" {
		return 0, fmt.Errorf("missing seqNum")
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	if parsed == 0 {
		return 0, fmt.Errorf("seqNum must be greater than zero")
	}
	return parsed, nil
}

func parseOptionalInt64(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.ParseInt(value, 10, 64)
}
