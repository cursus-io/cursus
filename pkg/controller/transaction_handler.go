package controller

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/cursus-io/cursus/util"
)

const maxTransactionReconcilePagesPerPass = 4

const transactionControlMarkerPayload = "__cursus_txn_control_marker__"

func (ch *CommandHandler) handleInitProducerID(cmd string, contexts ...*ClientContext) string {
	args := parseKeyValueArgs(cmd[len("INIT_PRODUCER_ID "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=INIT_PRODUCER_ID"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

	previousSnap, hadPrevious := ch.TxnManager.Snapshot(txnID)
	mode := transaction.ModeProcessingV1
	producerID, epoch, err := ch.TxnManager.InitProducerWithMode(txnID, mode)
	if err != nil {
		return fmt.Sprintf("ERROR: init_producer_failed reason=%q", err.Error())
	}
	if mode == transaction.ModeProcessingV1 && ch.isDistributed() {
		owner, _, coordinatorEpoch, coordErr := ch.Cluster.Router.FindTransactionCoordinator(txnID)
		if coordErr != nil || owner != ch.Cluster.Router.BrokerID() {
			ch.restoreTransaction(txnID, previousSnap, hadPrevious)
			return coordinatorUnavailableResponse
		}
		if err := ch.TxnManager.SetCoordinatorEpoch(txnID, coordinatorEpoch); err != nil {
			ch.restoreTransaction(txnID, previousSnap, hadPrevious)
			return fmt.Sprintf("ERROR: init_producer_failed reason=%q", err.Error())
		}
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

func (ch *CommandHandler) handleBeginTxn(cmd string, contexts ...*ClientContext) string {
	ctx := firstClientContext(contexts)
	args := parseKeyValueArgs(cmd[len("BEGIN_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=BEGIN_TXN"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

	producerID := firstNonEmpty(args["producerId"], args["producer_id"])
	if producerID == "" {
		return "ERROR: missing_producer_id command=BEGIN_TXN"
	}
	epoch, err := parseOptionalInt64(args["epoch"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_epoch reason=%q", err.Error())
	}
	previousSnap, hadPrevious := ch.snapshotTransaction(txnID)
	if resp := ch.requireTransactionMode(txnID, ctx); resp != "" {
		return resp
	}
	deadline := time.Time{}
	if current, statusErr := ch.TxnManager.Status(txnID); statusErr == nil && current.Mode == transaction.ModeProcessingV1 {
		deadline = time.Now().Add(transactionTimeout(ch.Config))
	}
	if err := ch.TxnManager.BeginWithDeadline(txnID, producerID, epoch, deadline); err != nil {
		if errors.Is(err, transaction.ErrProducerReinitializationRequired) {
			return fmt.Sprintf("ERROR: producer_reinitialization_required transactional_id=%s epoch=%d", txnID, epoch)
		}
		return fmt.Sprintf("ERROR: transaction_begin_failed reason=%q", err.Error())
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		ch.restoreTransaction(txnID, previousSnap, hadPrevious)
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s state=open producerId=%s epoch=%d", txnID, producerID, epoch)
}

func (ch *CommandHandler) handleTxnPublish(cmd string, ctx ...*ClientContext) string {
	var clientCtx *ClientContext
	if len(ctx) > 0 {
		clientCtx = ctx[0]
	}
	args := parseKeyValueArgs(cmd[len("TXN_PUBLISH "):])
	if authResp := ch.authenticateInline(args, clientCtx); authResp != "" {
		return authResp
	}
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=TXN_PUBLISH"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

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
	if t.PolicySnapshot().AggregateReplay {
		return fmt.Sprintf("ERROR: aggregate_publish_requires_append_stream topic=%s", topicName)
	}
	if authResp := ch.authorizeTopicWrite(t.PolicySnapshot(), clientCtx); authResp != "" {
		return fmt.Sprintf("%s topic=%s", authResp, topicName)
	}
	msg := types.Message{Payload: message, ProducerID: producerID, SeqNum: seqNum, Epoch: epoch, Key: args["key"], TransactionalID: txnID, TransactionState: types.TransactionStateOpen}
	if partition < 0 {
		partition = t.GetPartitionForMessage(msg)
	}
	if _, err := t.GetPartition(partition); err != nil {
		return fmt.Sprintf("ERROR: partition_not_found partition=%d", partition)
	}
	if resp := ch.requireTransactionMode(txnID, clientCtx); resp != "" {
		return resp
	}
	previousSnap, hadPrevious := ch.snapshotTransaction(txnID)
	current, statusErr := ch.TxnManager.Status(txnID)
	if statusErr != nil {
		return fmt.Sprintf("ERROR: transaction_not_found reason=%q", statusErr.Error())
	}
	if current.Mode == transaction.ModeProcessingV1 {
		participant := transaction.Participant{Topic: topicName, Partition: partition}
		if err := ch.TxnManager.AddParticipant(txnID, producerID, epoch, participant, time.Time{}); err != nil {
			return fmt.Sprintf("ERROR: transaction_publish_failed reason=%q", err.Error())
		}
		if err := ch.syncTransactionState(txnID); err != nil {
			ch.restoreTransaction(txnID, previousSnap, hadPrevious)
			return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
		}
		if err := ch.publishCommittedTransactionMessage(transaction.MessageOperation{Topic: topicName, Partition: partition, Message: msg}); err != nil {
			return fmt.Sprintf("ERROR: transaction_publish_failed reason=%q", err.Error())
		}
		return fmt.Sprintf("OK transactional_id=%s appended=true topic=%s partition=%d state=open", txnID, topicName, partition)
	}
	if err := ch.TxnManager.AddMessage(txnID, producerID, epoch, transaction.MessageOperation{Topic: topicName, Partition: partition, Message: msg}); err != nil {
		return fmt.Sprintf("ERROR: transaction_publish_failed reason=%q", err.Error())
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		ch.restoreTransaction(txnID, previousSnap, hadPrevious)
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s staged_messages=1 topic=%s partition=%d", txnID, topicName, partition)
}

func (ch *CommandHandler) handleSendOffsetsToTxn(cmd string, contexts ...*ClientContext) string {
	ctx := firstClientContext(contexts)
	args := parseKeyValueArgs(cmd[len("SEND_OFFSETS_TO_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=SEND_OFFSETS_TO_TXN"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

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
	if resp := ch.requireTransactionMode(txnID, ctx); resp != "" {
		return resp
	}
	offsetTopic, offsetTopicErr := ch.resolveGroupOffsetTopic(groupID, topicName)
	if offsetTopicErr != "" {
		return offsetTopicErr
	}
	offsetPairs, err := wire.DecodeOffsetPairs(args["offsets"])
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_txn_offsets reason=%q", err.Error())
	}
	registrationEpoch := ch.Coordinator.GetRegistrationEpoch(groupID)
	ops := make([]transaction.OffsetOperation, 0, len(offsetPairs))
	for _, pair := range offsetPairs {
		op := transaction.OffsetOperation{Topic: offsetTopic, Group: groupID, Member: memberID, Generation: generation, Partition: pair.Partition, Offset: pair.Offset, RegistrationEpoch: registrationEpoch}
		if err := ch.validateTransactionOffset(op, false); err != nil {
			return err.Error()
		}
		ops = append(ops, op)
	}
	previousSnap, hadPrevious := ch.snapshotTransaction(txnID)
	if err := ch.TxnManager.AddOffsets(txnID, producerID, epoch, ops); err != nil {
		return fmt.Sprintf("ERROR: transaction_offsets_failed reason=%q", err.Error())
	}
	if err := ch.syncTransactionState(txnID); err != nil {
		ch.restoreTransaction(txnID, previousSnap, hadPrevious)
		return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
	}
	return fmt.Sprintf("OK transactional_id=%s staged_offsets=%d", txnID, len(ops))
}

func (ch *CommandHandler) handleEndTxn(cmd string, contexts ...*ClientContext) string {
	ctx := firstClientContext(contexts)
	args := parseKeyValueArgs(cmd[len("END_TXN "):])
	txnID := firstNonEmpty(args["transactional_id"], args["txn"], args["transaction"])
	if txnID == "" {
		return "ERROR: missing_transactional_id command=END_TXN"
	}
	if resp := ch.ensureTransactionCoordinator(txnID); resp != "" {
		return resp
	}
	stateLock := ch.transactionStateLock(txnID)
	stateLock.Lock()
	defer stateLock.Unlock()

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
	if resp := requireTransactionSnapshotMode(current, ctx); resp != "" {
		return resp
	}
	if result == "abort" {
		if current.State == transaction.StateCommitted {
			return fmt.Sprintf("ERROR: transaction_already_committed transactional_id=%s", txnID)
		}
		if current.State == transaction.StateAborted {
			return fmt.Sprintf("OK transactional_id=%s state=aborted", txnID)
		}
		if current.State == transaction.StateCommitting || current.State == transaction.StatePrepareCommit {
			return fmt.Sprintf("ERROR: transaction_not_abortable transactional_id=%s state=%s", txnID, current.State)
		}
		if current.Mode == transaction.ModeProcessingV1 {
			prepared, err := ch.TxnManager.PrepareAbort(txnID, producerID, epoch)
			if err != nil {
				return fmt.Sprintf("ERROR: transaction_abort_failed reason=%q", err.Error())
			}
			if prepared.State == transaction.StatePrepareAbort {
				if err := ch.syncTransactionState(txnID); err != nil {
					return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
				}
				if err := ch.appendTransactionMarkers(prepared, types.TransactionMarkerAbort); err != nil {
					return fmt.Sprintf("ERROR: transaction_abort_failed state=prepare_abort reason=%q", err.Error())
				}
			}
		}
		if err := ch.abortTransactionDecision(txnID, producerID, epoch); err != nil {
			return fmt.Sprintf("ERROR: transaction_abort_failed reason=%q", err.Error())
		}
		return fmt.Sprintf("OK transactional_id=%s state=aborted", txnID)
	}
	if result != "commit" {
		return fmt.Sprintf("ERROR: invalid_transaction_result value=%s", result)
	}

	if current.State == transaction.StateCommitted {
		if current.Mode == transaction.ModeProcessingV1 && !current.OffsetsMaterialized {
			if err := ch.materializeAndCheckpointTransactionOffsets(current.ID, current.Offsets); err != nil {
				return fmt.Sprintf("ERROR: transaction_offset_materialization_failed reason=%q", err.Error())
			}
		}
		return fmt.Sprintf("OK transactional_id=%s state=committed messages=%d offsets=%d", txnID, len(current.Messages), len(current.Offsets))
	}
	if current.State == transaction.StateAborted {
		return fmt.Sprintf("ERROR: transaction_aborted transactional_id=%s", txnID)
	}
	if current.State == transaction.StateOpen {
		if err := ch.validateTransaction(current); err != nil {
			return fmt.Sprintf("ERROR: transaction_commit_failed state=open reason=%q", err.Error())
		}
		if current.Mode == transaction.ModeProcessingV1 && len(current.Offsets) > 0 {
			var err error
			current, err = ch.prepareTransactionOffsetRecords(current)
			if err != nil {
				return fmt.Sprintf("ERROR: transaction_offset_prepare_failed state=open reason=%q", err.Error())
			}
		}
	}

	tx := current
	if current.State == transaction.StateOpen {
		var err error
		tx, err = ch.TxnManager.PrepareCommit(txnID, producerID, epoch)
		if err != nil {
			return fmt.Sprintf("ERROR: transaction_prepare_failed reason=%q", err.Error())
		}
	}
	if tx.State == transaction.StateCommitting || tx.State == transaction.StatePrepareCommit {
		if err := ch.syncTransactionState(txnID); err != nil {
			return fmt.Sprintf("ERROR: transaction_sync_failed reason=%q", err.Error())
		}
	}
	if err := ch.applyTransaction(tx); err != nil {
		if syncErr := ch.syncTransactionState(txnID); syncErr != nil {
			return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q sync_reason=%q", err.Error(), syncErr.Error())
		}
		return fmt.Sprintf("ERROR: transaction_commit_failed state=committing reason=%q", err.Error())
	}
	if err := ch.commitTransactionDecision(txnID); err != nil {
		return fmt.Sprintf("ERROR: transaction_commit_failed reason=%q", err.Error())
	}
	if tx.Mode == transaction.ModeProcessingV1 {
		if err := ch.materializeAndCheckpointTransactionOffsets(tx.ID, tx.Offsets); err != nil {
			return fmt.Sprintf("ERROR: transaction_offset_materialization_failed reason=%q", err.Error())
		}
	}
	return fmt.Sprintf("OK transactional_id=%s state=committed messages=%d offsets=%d", txnID, len(tx.Messages), len(tx.Offsets))
}

func (ch *CommandHandler) RecoverPreparedTransactions() error {
	_, err := ch.recoverPreparedTransactionsBatch(ch.transactionRecoveryShards(), ch.transactionRecoveryBatchSize())
	return err
}

func (ch *CommandHandler) recoverPreparedTransactionsBatch(shards []int, limit int) (bool, error) {
	if ch.TxnManager == nil {
		return false, nil
	}
	pending, more := ch.TxnManager.PreparedTransactions(shards, limit)
	if len(pending) == 0 {
		return more, nil
	}
	for _, pendingTx := range pending {
		if pendingTx == nil {
			continue
		}
		stateLock := ch.transactionStateLock(pendingTx.ID)
		stateLock.Lock()

		tx, err := ch.TxnManager.Status(pendingTx.ID)
		if err != nil {
			stateLock.Unlock()
			return more, fmt.Errorf("reload transaction %s for recovery: %w", pendingTx.ID, err)
		}
		if tx.State == transaction.StateCommitted && tx.Mode == transaction.ModeProcessingV1 && !tx.OffsetsMaterialized {
			err = ch.materializeAndCheckpointTransactionOffsets(tx.ID, tx.Offsets)
			stateLock.Unlock()
			if err != nil {
				return more, fmt.Errorf("materialize recovered transaction %s offsets: %w", tx.ID, err)
			}
			continue
		}
		if tx.State != transaction.StateCommitting && tx.State != transaction.StatePrepareCommit && tx.State != transaction.StatePrepareAbort {
			stateLock.Unlock()
			continue
		}
		if resp := ch.ensureTransactionCoordinator(tx.ID); resp != "" {
			stateLock.Unlock()
			util.Debug("Skipping transaction recovery for %s on non-coordinator: %s", tx.ID, resp)
			continue
		}
		if tx.State == transaction.StatePrepareAbort {
			if err := ch.appendTransactionMarkers(tx, types.TransactionMarkerAbort); err != nil {
				stateLock.Unlock()
				return more, fmt.Errorf("recover aborted transaction %s: %w", tx.ID, err)
			}
			if err := ch.abortTransactionDecision(tx.ID, tx.Producer, tx.Epoch); err != nil {
				stateLock.Unlock()
				return more, fmt.Errorf("mark recovered transaction %s aborted: %w", tx.ID, err)
			}
		} else {
			if err := ch.applyTransaction(tx); err != nil {
				stateLock.Unlock()
				return more, fmt.Errorf("recover transaction %s: %w", tx.ID, err)
			}
			if err := ch.commitTransactionDecision(tx.ID); err != nil {
				stateLock.Unlock()
				return more, fmt.Errorf("mark recovered transaction %s committed: %w", tx.ID, err)
			}
		}
		stateLock.Unlock()
		util.Info("Recovered prepared transaction %s", tx.ID)
	}
	return more, nil
}

// AbortTimedOutTransactions resolves expired open v1 transactions so their
// unresolved records cannot hold read_committed consumers indefinitely.
func (ch *CommandHandler) AbortTimedOutTransactions(now time.Time) error {
	_, err := ch.abortTimedOutTransactionsBatch(ch.transactionRecoveryShards(), now, ch.transactionRecoveryBatchSize())
	return err
}

func (ch *CommandHandler) abortTimedOutTransactionsBatch(shards []int, now time.Time, limit int) (bool, error) {
	candidates, more := ch.TxnManager.TimedOutTransactions(shards, now, limit)
	for _, candidate := range candidates {
		stateLock := ch.transactionStateLock(candidate.ID)
		stateLock.Lock()
		tx, err := ch.TxnManager.Status(candidate.ID)
		if err != nil {
			stateLock.Unlock()
			continue
		}
		if tx.State == transaction.StateOpen && !now.Before(tx.Deadline) {
			if resp := ch.ensureTransactionCoordinator(tx.ID); resp == "" {
				tx, err = ch.TxnManager.PrepareAbort(tx.ID, tx.Producer, tx.Epoch)
				if err == nil {
					err = ch.syncTransactionState(tx.ID)
				}
				if err == nil {
					err = ch.appendTransactionMarkers(tx, types.TransactionMarkerAbort)
				}
				if err == nil {
					err = ch.abortTransactionDecision(tx.ID, tx.Producer, tx.Epoch)
				}
			}
		}
		stateLock.Unlock()
		if err != nil {
			return more, fmt.Errorf("abort timed out transaction %s: %w", candidate.ID, err)
		}
	}
	return more, nil
}

func (ch *CommandHandler) StartTransactionTimeoutMonitor(ctx context.Context) {
	interval := transactionTimeout(ch.Config) / 4
	if interval < time.Second {
		interval = time.Second
	}
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	var ownershipChanges <-chan []int
	if ch.isDistributed() && ch.Cluster.RaftManager.GetFSM() != nil {
		ownershipChanges = ch.Cluster.RaftManager.GetFSM().TransactionCoordinatorChanges()
	}
	reconcile := func(ctx context.Context, shards []int, now time.Time) {
		limit := ch.transactionRecoveryBatchSize()
		for pages := 0; pages < maxTransactionReconcilePagesPerPass; pages++ {
			if ctx.Err() != nil {
				return
			}
			preparedMore, err := ch.recoverPreparedTransactionsBatch(shards, limit)
			if err != nil {
				util.Error("Prepared transaction recovery failed: %v", err)
				return
			}
			timeoutMore, err := ch.abortTimedOutTransactionsBatch(shards, now, limit)
			if err != nil {
				util.Error("Transaction timeout resolution failed: %v", err)
				return
			}
			if !preparedMore && !timeoutMore {
				return
			}
			runtime.Gosched()
		}
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case changed := <-ownershipChanges:
				reconcile(ctx, intersectTransactionShards(changed, ch.transactionRecoveryShards()), time.Now())
			case now := <-ticker.C:
				reconcile(ctx, ch.transactionRecoveryShards(), now)
			}
		}
	}()
}

func intersectTransactionShards(changed, owned []int) []int {
	if changed == nil || owned == nil {
		return owned
	}
	ownedSet := make(map[int]struct{}, len(owned))
	for _, shardID := range owned {
		ownedSet[shardID] = struct{}{}
	}
	result := make([]int, 0, len(changed))
	for _, shardID := range changed {
		if _, ok := ownedSet[shardID]; ok {
			result = append(result, shardID)
		}
	}
	return result
}

func (ch *CommandHandler) transactionRecoveryShards() []int {
	if ch != nil && ch.isDistributed() {
		return ch.Cluster.OwnedTransactionCoordinatorShards()
	}
	return nil
}

func (ch *CommandHandler) transactionRecoveryBatchSize() int {
	if ch == nil || ch.Config == nil || ch.Config.TransactionRecoveryBatchSize <= 0 {
		return 256
	}
	return ch.Config.TransactionRecoveryBatchSize
}
func (ch *CommandHandler) handleTxnStatus(cmd string, contexts ...*ClientContext) string {
	ctx := firstClientContext(contexts)
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
	if resp := requireTransactionSnapshotMode(tx, ctx); resp != "" {
		return resp
	}
	mode := string(tx.Mode)
	if tx.Mode == transaction.ModeProcessingV1 {
		mode = "exactly_once"
	}
	return fmt.Sprintf("OK transactional_id=%s mode=%s state=%s messages=%d participants=%d offsets=%d", tx.ID, mode, tx.State, len(tx.Messages), len(tx.Participants), len(tx.Offsets))
}

func (ch *CommandHandler) applyTransaction(tx *transaction.Transaction) error {
	if err := ch.validateTransaction(tx); err != nil {
		return err
	}
	apply := func() error {
		for _, op := range tx.Messages {
			if err := ch.publishCommittedTransactionMessage(op); err != nil {
				return err
			}
		}
		if tx.Mode != transaction.ModeProcessingV1 {
			if err := ch.commitTransactionOffsets(tx.Offsets); err != nil {
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

func (ch *CommandHandler) prepareTransactionOffsetRecords(tx *transaction.Transaction) (*transaction.Transaction, error) {
	if tx == nil || tx.State != transaction.StateOpen || tx.Mode != transaction.ModeProcessingV1 {
		return tx, nil
	}
	records, err := transactionalOffsetRecords(tx)
	if err != nil {
		return nil, err
	}
	topic := ch.TopicManager.GetTopic("__consumer_offsets")
	if topic == nil {
		return nil, fmt.Errorf("consumer offset topic is unavailable")
	}
	type preparedRecord struct {
		record    coordinator.ConsumerMetadataRecord
		payload   []byte
		key       string
		partition int
	}
	prepared := make([]preparedRecord, 0, len(records))
	for _, record := range records {
		payload, key, encodeErr := coordinator.EncodeConsumerMetadataRecord(record)
		if encodeErr != nil {
			return nil, encodeErr
		}
		partition := topic.GetPartitionForMessage(types.Message{Key: key})
		if err := ch.TxnManager.AddParticipant(tx.ID, tx.Producer, tx.Epoch, transaction.Participant{Topic: "__consumer_offsets", Partition: partition}, tx.Deadline); err != nil {
			return nil, err
		}
		prepared = append(prepared, preparedRecord{record: record, payload: payload, key: key, partition: partition})
	}
	if err := ch.syncTransactionState(tx.ID); err != nil {
		return nil, err
	}
	current, err := ch.TxnManager.Status(tx.ID)
	if err != nil {
		return nil, err
	}
	finalRecords, err := transactionalOffsetRecords(current)
	if err != nil {
		return nil, err
	}
	if len(finalRecords) != len(prepared) {
		return nil, fmt.Errorf("transaction offset record set changed while preparing")
	}
	for i, record := range finalRecords {
		payload, key, encodeErr := coordinator.EncodeConsumerMetadataRecord(record)
		if encodeErr != nil {
			return nil, encodeErr
		}
		if key != prepared[i].key {
			return nil, fmt.Errorf("transaction offset record partition key changed while preparing")
		}
		prepared[i].record = record
		prepared[i].payload = payload
	}
	seqByPartition := make(map[int]uint64)
	for _, item := range prepared {
		seqByPartition[item.partition]++
		msg := types.Message{
			Payload:          string(item.payload),
			Key:              item.key,
			ProducerID:       tx.Producer,
			SeqNum:           seqByPartition[item.partition],
			Epoch:            tx.Epoch,
			TransactionalID:  tx.ID,
			TransactionState: types.TransactionStateOpen,
		}
		if err := ch.publishCommittedTransactionMessage(transaction.MessageOperation{Topic: "__consumer_offsets", Partition: item.partition, Message: msg}); err != nil {
			return nil, err
		}
	}
	return current, nil
}

func transactionalOffsetRecords(tx *transaction.Transaction) ([]coordinator.ConsumerMetadataRecord, error) {
	if tx == nil || len(tx.Offsets) == 0 {
		return nil, nil
	}
	byTopic := make(map[string][]coordinator.OffsetItem)
	scope := tx.Offsets[0]
	for _, op := range tx.Offsets {
		if op.Group != scope.Group || op.Member != scope.Member || op.Generation != scope.Generation || op.RegistrationEpoch != scope.RegistrationEpoch {
			return nil, fmt.Errorf("transaction offset scope mismatch")
		}
		byTopic[op.Topic] = append(byTopic[op.Topic], coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
	}
	topics := make([]string, 0, len(byTopic))
	for topicName := range byTopic {
		topics = append(topics, topicName)
	}
	sort.Strings(topics)
	records := make([]coordinator.ConsumerMetadataRecord, 0, len(topics))
	for _, topicName := range topics {
		records = append(records, coordinator.ConsumerMetadataRecord{
			Version:          coordinator.ConsumerMetadataRecordVersionTransactions,
			Type:             coordinator.ConsumerMetadataRecordTransactionalOffsetSnapshot,
			Group:            scope.Group,
			Topic:            topicName,
			Epoch:            scope.RegistrationEpoch,
			Revision:         tx.Revision,
			Offsets:          byTopic[topicName],
			TransactionalID:  tx.ID,
			ProducerID:       tx.Producer,
			ProducerEpoch:    tx.Epoch,
			CoordinatorEpoch: tx.CoordinatorEpoch,
			Timestamp:        tx.CreatedAt.UTC(),
		})
	}
	return records, nil
}

func (ch *CommandHandler) validateTransaction(tx *transaction.Transaction) error {
	for _, op := range tx.Messages {
		t := ch.TopicManager.GetTopic(op.Topic)
		if t == nil {
			return fmt.Errorf("topic %s not found", op.Topic)
		}
		if !t.PolicySnapshot().CanWrite() {
			return fmt.Errorf("NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", op.Topic)
		}
		if _, err := t.GetPartition(op.Partition); err != nil {
			return err
		}
	}
	for _, participant := range tx.Participants {
		t := ch.TopicManager.GetTopic(participant.Topic)
		if t == nil {
			return fmt.Errorf("topic %s not found", participant.Topic)
		}
		if !t.PolicySnapshot().CanWrite() {
			return fmt.Errorf("NOT_AUTHORIZED_FOR_TOPIC topic=%s operation=write", participant.Topic)
		}
		if _, err := t.GetPartition(participant.Partition); err != nil {
			return err
		}
	}
	for _, op := range tx.Offsets {
		if err := ch.validateTransactionOffset(op, true); err != nil {
			return err
		}
	}
	return nil
}

type transactionOffsetFence struct {
	Group             string
	Member            string
	Generation        int
	Partitions        []coordinator.TopicPartition
	RegistrationEpoch uint64
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
		if fence.RegistrationEpoch != 0 && ch.Coordinator.GetRegistrationEpoch(fence.Group) != fence.RegistrationEpoch {
			return fmt.Errorf("ERROR: group_epoch_mismatch group=%s expected=%d actual=%d", fence.Group, fence.RegistrationEpoch, ch.Coordinator.GetRegistrationEpoch(fence.Group))
		}
		return ch.Coordinator.WithTopicOwnershipFence(fence.Group, fence.Member, fence.Generation, fence.Partitions, func() error {
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
		resp, err := ch.Cluster.Router.ForwardToCoordinator(op.Group, cmd)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(resp, "OK") {
			return fmt.Errorf("%s", resp)
		}
		return nil
	}
	if op.RegistrationEpoch != 0 && ch.Coordinator.GetRegistrationEpoch(op.Group) != op.RegistrationEpoch {
		return fmt.Errorf("ERROR: group_epoch_mismatch group=%s expected=%d actual=%d", op.Group, op.RegistrationEpoch, ch.Coordinator.GetRegistrationEpoch(op.Group))
	}
	if errResp := ch.Coordinator.ValidateTopicPartitionOwnershipFailure(op.Group, op.Member, op.Generation, op.Topic, op.Partition); errResp != "" {
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
		group             string
		member            string
		generation        int
		registrationEpoch uint64
	}
	index := make(map[key]int)
	partitionSeen := make(map[key]map[coordinator.TopicPartition]struct{})
	fences := make([]transactionOffsetFence, 0)
	for _, op := range ops {
		k := key{group: op.Group, member: op.Member, generation: op.Generation, registrationEpoch: op.RegistrationEpoch}
		idx, ok := index[k]
		if !ok {
			idx = len(fences)
			index[k] = idx
			partitionSeen[k] = make(map[coordinator.TopicPartition]struct{})
			fences = append(fences, transactionOffsetFence{Group: op.Group, Member: op.Member, Generation: op.Generation, RegistrationEpoch: op.RegistrationEpoch})
		}
		tp := coordinator.TopicPartition{Topic: op.Topic, Partition: op.Partition}
		if _, ok := partitionSeen[k][tp]; ok {
			continue
		}
		partitionSeen[k][tp] = struct{}{}
		fences[idx].Partitions = append(fences[idx].Partitions, tp)
	}
	return fences
}
func (ch *CommandHandler) appendTransactionMarkers(tx *transaction.Transaction, marker string) error {
	if tx == nil || (len(tx.Messages) == 0 && len(tx.Participants) == 0) {
		return nil
	}
	state := types.TransactionStateCommitted
	if marker == types.TransactionMarkerAbort {
		state = types.TransactionStateAborted
	}

	partitions := touchedTransactionPartitions(tx)
	for _, partition := range partitions {
		coordinatorEpoch := tx.Epoch
		if tx.Mode == transaction.ModeProcessingV1 {
			coordinatorEpoch = tx.CoordinatorEpoch
		}
		controlKey, controlValue, err := transactionMarkerControlBytes(marker, coordinatorEpoch)
		if err != nil {
			return err
		}
		msg := types.Message{
			Payload:                      transactionControlMarkerPayload,
			ProducerID:                   transactionMarkerProducerID(tx, marker),
			SeqNum:                       1,
			Epoch:                        tx.Epoch,
			TransactionalID:              tx.ID,
			TransactionState:             state,
			TransactionMarker:            marker,
			ControlBatchType:             types.ControlBatchTransaction,
			ControlBatchVersion:          types.ControlBatchVersionCursusV2,
			ControlBatchCoordinatorEpoch: coordinatorEpoch,
			ControlBatchKey:              controlKey,
			ControlBatchValue:            controlValue,
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
	if tx.Mode != transaction.ModeProcessingV1 {
		return fmt.Sprintf("txn-marker:%s:%s", tx.ID, marker)
	}
	return fmt.Sprintf("txn-marker:%s:%d:%s", tx.ID, tx.CoordinatorEpoch, marker)
}
func touchedTransactionPartitions(tx *transaction.Transaction) []transactionPartition {
	seen := make(map[transactionPartition]struct{})
	for _, op := range tx.Messages {
		seen[transactionPartition{Topic: op.Topic, Partition: op.Partition}] = struct{}{}
	}
	for _, participant := range tx.Participants {
		seen[transactionPartition{Topic: participant.Topic, Partition: participant.Partition}] = struct{}{}
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
		cmd := fmt.Sprintf("PUBLISH topic=%s acks=all producerId=%s partition=%d seqNum=%d epoch=%d isIdempotent=true transactional_id=%s transaction_state=%s transaction_marker=%s control_batch_type=%s control_batch_version=%d control_batch_coordinator_epoch=%d control_batch_key=%s control_batch_value=%s internal_txn_publish=true message=%s", topicName, msg.ProducerID, partition, msg.SeqNum, msg.Epoch, msg.TransactionalID, msg.TransactionState, msg.TransactionMarker, msg.ControlBatchType, msg.ControlBatchVersion, msg.ControlBatchCoordinatorEpoch, base64.StdEncoding.EncodeToString(msg.ControlBatchKey), base64.StdEncoding.EncodeToString(msg.ControlBatchValue), msg.Payload)
		return ch.publishInternalTransactionCommand(cmd)
	}
	return ch.TopicManager.PublishToPartitionWithAckIdempotent(topicName, partition, &msg)
}
func (ch *CommandHandler) publishCommittedTransactionMessage(op transaction.MessageOperation) error {
	msg := op.Message
	msg.TransactionState = types.TransactionStateOpen
	msg.TransactionMarker = types.TransactionMarkerNone
	if ch.isDistributed() {
		cmd := fmt.Sprintf("PUBLISH topic=%s acks=all producerId=%s partition=%d seqNum=%d epoch=%d isIdempotent=true internal_txn_publish=true", op.Topic, msg.ProducerID, op.Partition, msg.SeqNum, msg.Epoch)
		if msg.Key != "" {
			cmd += fmt.Sprintf(" key=%s", msg.Key)
		}
		if msg.TransactionalID != "" {
			cmd += fmt.Sprintf(" transactional_id=%s transaction_state=%s", msg.TransactionalID, msg.TransactionState)
		}
		cmd += " message=" + msg.Payload
		return ch.publishInternalTransactionCommand(cmd)
	}
	return ch.TopicManager.PublishToPartitionWithAckIdempotent(op.Topic, op.Partition, &msg)
}

func (ch *CommandHandler) publishInternalTransactionCommand(cmd string) error {
	deadline := time.Now().Add(DefaultFSMApplyTimeout)
	var lastResp string
	for {
		lastResp = ch.handlePublish(cmd, NewInternalClientContext("default-group", 0))
		if strings.HasPrefix(lastResp, "OK") || strings.HasPrefix(lastResp, "{") {
			return nil
		}
		if !isRetryableTransactionStateLag(lastResp) || !time.Now().Before(deadline) {
			return fmt.Errorf("%s", lastResp)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func isRetryableTransactionStateLag(resp string) bool {
	normalized := strings.ToLower(strings.TrimSpace(resp))
	for _, code := range []string{
		"transaction_not_found",
		"transaction_not_committing",
		"transaction_record_not_staged",
		"transaction_marker_partition_not_touched",
		"transaction_not_abortable",
		"producer_fenced",
	} {
		marker := "error: " + code
		if strings.HasPrefix(normalized, marker+" ") || strings.Contains(normalized, " "+marker+" ") {
			return true
		}
	}
	return false
}
func (ch *CommandHandler) validateTransactionPublishMetadata(args map[string]string, topicName string, partition int, msg *types.Message) string {
	if msg == nil {
		return ""
	}
	hasTxnMetadata := msg.TransactionalID != "" || msg.TransactionState != "" || msg.TransactionMarker != ""
	if !hasTxnMetadata {
		return ""
	}
	if !strings.EqualFold(args["internal_txn_publish"], "true") {
		return "ERROR: transaction_metadata_forbidden command=PUBLISH"
	}
	if msg.TransactionalID == "" {
		return "ERROR: missing_transactional_id command=PUBLISH"
	}
	if ch.TxnManager == nil {
		return "ERROR: transaction_manager_not_available command=PUBLISH"
	}
	tx, err := ch.TxnManager.Status(msg.TransactionalID)
	if err != nil {
		return fmt.Sprintf("ERROR: transaction_not_found transactional_id=%s", msg.TransactionalID)
	}
	if tx.Epoch != msg.Epoch {
		return fmt.Sprintf("ERROR: producer_fenced transactional_id=%s current_epoch=%d requested_epoch=%d", msg.TransactionalID, tx.Epoch, msg.Epoch)
	}
	if msg.TransactionMarker != types.TransactionMarkerNone {
		return ch.validateTransactionMarkerPublish(tx, topicName, partition, msg)
	}
	if tx.Producer != msg.ProducerID {
		return fmt.Sprintf("ERROR: producer_fenced transactional_id=%s current_epoch=%d requested_epoch=%d", msg.TransactionalID, tx.Epoch, msg.Epoch)
	}
	return ch.validateTransactionRecordPublish(tx, topicName, partition, msg)
}

func (ch *CommandHandler) validateTransactionRecordPublish(tx *transaction.Transaction, topicName string, partition int, msg *types.Message) string {
	if tx.Mode == transaction.ModeProcessingV1 {
		if tx.State != transaction.StateOpen {
			return fmt.Sprintf("ERROR: transaction_not_open transactional_id=%s state=%s", tx.ID, tx.State)
		}
		for _, participant := range tx.Participants {
			if participant.Topic == topicName && participant.Partition == partition {
				return ""
			}
		}
		return fmt.Sprintf("ERROR: transaction_record_not_staged transactional_id=%s", tx.ID)
	}
	if tx.State != transaction.StateCommitting && tx.State != transaction.StateCommitted {
		return fmt.Sprintf("ERROR: transaction_not_committing transactional_id=%s state=%s", tx.ID, tx.State)
	}
	if msg.TransactionState != types.TransactionStateOpen {
		return fmt.Sprintf("ERROR: invalid_transaction_state state=%s", msg.TransactionState)
	}
	for _, op := range tx.Messages {
		if op.Topic == topicName && op.Partition == partition && op.Message.ProducerID == msg.ProducerID && op.Message.SeqNum == msg.SeqNum && op.Message.Epoch == msg.Epoch && op.Message.Payload == msg.Payload && op.Message.Key == msg.Key {
			return ""
		}
	}
	return fmt.Sprintf("ERROR: transaction_record_not_staged transactional_id=%s", tx.ID)
}

func (ch *CommandHandler) validateTransactionMarkerPublish(tx *transaction.Transaction, topicName string, partition int, msg *types.Message) string {
	if msg.ControlBatchType != types.ControlBatchTransaction || msg.ControlBatchVersion != types.ControlBatchVersionCursusV2 {
		return fmt.Sprintf("ERROR: invalid_transaction_control_batch transactional_id=%s type=%s version=%d", tx.ID, msg.ControlBatchType, msg.ControlBatchVersion)
	}
	expectedCoordinatorEpoch := tx.Epoch
	if tx.Mode == transaction.ModeProcessingV1 {
		expectedCoordinatorEpoch = tx.CoordinatorEpoch
	}
	if msg.ControlBatchCoordinatorEpoch != expectedCoordinatorEpoch {
		return fmt.Sprintf("ERROR: invalid_transaction_control_epoch transactional_id=%s current_epoch=%d control_epoch=%d", tx.ID, expectedCoordinatorEpoch, msg.ControlBatchCoordinatorEpoch)
	}
	expectedKey, expectedValue, err := transactionMarkerControlBytes(msg.TransactionMarker, expectedCoordinatorEpoch)
	if err != nil {
		return fmt.Sprintf("ERROR: invalid_transaction_control_epoch transactional_id=%s reason=%q", tx.ID, err.Error())
	}
	if !bytes.Equal(msg.ControlBatchKey, expectedKey) || !bytes.Equal(msg.ControlBatchValue, expectedValue) {
		return fmt.Sprintf("ERROR: invalid_transaction_control_record transactional_id=%s", tx.ID)
	}
	if msg.TransactionMarker != types.TransactionMarkerCommit && msg.TransactionMarker != types.TransactionMarkerAbort {
		return fmt.Sprintf("ERROR: invalid_transaction_marker marker=%s", msg.TransactionMarker)
	}
	if msg.ProducerID != transactionMarkerProducerID(tx, msg.TransactionMarker) || msg.SeqNum != 1 {
		return fmt.Sprintf("ERROR: invalid_transaction_marker_producer transactional_id=%s", tx.ID)
	}
	if msg.TransactionMarker == types.TransactionMarkerCommit && tx.State != transaction.StateCommitting && tx.State != transaction.StatePrepareCommit && tx.State != transaction.StateCommitted {
		return fmt.Sprintf("ERROR: transaction_not_committing transactional_id=%s state=%s", tx.ID, tx.State)
	}
	if msg.TransactionMarker == types.TransactionMarkerAbort && tx.State != transaction.StateOpen && tx.State != transaction.StatePrepareAbort && tx.State != transaction.StateAborted {
		return fmt.Sprintf("ERROR: transaction_not_abortable transactional_id=%s state=%s", tx.ID, tx.State)
	}
	for _, touched := range touchedTransactionPartitions(tx) {
		if touched.Topic == topicName && touched.Partition == partition {
			return ""
		}
	}
	return fmt.Sprintf("ERROR: transaction_marker_partition_not_touched transactional_id=%s topic=%s partition=%d", tx.ID, topicName, partition)
}

func (ch *CommandHandler) requireTransactionMode(txnID string, ctx *ClientContext) string {
	tx, err := ch.TxnManager.Status(txnID)
	if err != nil {
		return ""
	}
	return requireTransactionSnapshotMode(tx, ctx)
}

func requireTransactionSnapshotMode(_ *transaction.Transaction, _ *ClientContext) string {
	return ""
}
func (ch *CommandHandler) validateReplicatedTransactionMessage(topicName string, partition int, msg *types.Message) string {
	if msg == nil {
		return ""
	}
	hasTxnMetadata := msg.TransactionalID != "" || msg.TransactionState != "" || msg.TransactionMarker != ""
	if !hasTxnMetadata {
		return ""
	}
	if msg.TransactionalID == "" {
		return "ERROR: missing_transactional_id command=REPLICATE_MESSAGE"
	}
	if ch.TxnManager == nil {
		return "ERROR: transaction_manager_not_available command=REPLICATE_MESSAGE"
	}
	tx, err := ch.TxnManager.Status(msg.TransactionalID)
	if err != nil {
		return fmt.Sprintf("ERROR: transaction_not_found transactional_id=%s", msg.TransactionalID)
	}
	if tx.Epoch != msg.Epoch {
		return fmt.Sprintf("ERROR: producer_fenced transactional_id=%s current_epoch=%d requested_epoch=%d", msg.TransactionalID, tx.Epoch, msg.Epoch)
	}
	if msg.TransactionMarker != types.TransactionMarkerNone {
		return ch.validateTransactionMarkerPublish(tx, topicName, partition, msg)
	}
	if tx.Producer != msg.ProducerID {
		return fmt.Sprintf("ERROR: producer_fenced transactional_id=%s current_epoch=%d requested_epoch=%d", msg.TransactionalID, tx.Epoch, msg.Epoch)
	}
	return ch.validateTransactionRecordPublish(tx, topicName, partition, msg)
}
func (ch *CommandHandler) commitTransactionOffsets(ops []transaction.OffsetOperation) error {
	if len(ops) == 0 {
		return nil
	}

	ordered := append([]transaction.OffsetOperation(nil), ops...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Topic != ordered[j].Topic {
			return ordered[i].Topic < ordered[j].Topic
		}
		return ordered[i].Partition < ordered[j].Partition
	})
	scope := ordered[0]
	offsetsByTopic := make(map[string][]coordinator.OffsetItem)
	for _, op := range ordered {
		if op.Group != scope.Group || op.Member != scope.Member || op.Generation != scope.Generation || op.RegistrationEpoch != scope.RegistrationEpoch {
			return fmt.Errorf(
				"transaction offset scope mismatch: expected group=%s member=%s generation=%d registration_epoch=%d",
				scope.Group, scope.Member, scope.Generation, scope.RegistrationEpoch,
			)
		}
		offsetsByTopic[op.Topic] = append(offsetsByTopic[op.Topic], coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
	}

	if ch.Config != nil && ch.Config.EnabledDistribution {
		if ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.Cluster.Router == nil {
			return fmt.Errorf("distributed transaction offset commit requires cluster coordinator router")
		}
		payload, err := json.Marshal(offsetsByTopic)
		if err != nil {
			return fmt.Errorf("encode transaction offsets: %w", err)
		}
		cmd := fmt.Sprintf("BATCH_COMMIT group=%s member=%s generation=%d registration_epoch=%d topic_offsets=%s", scope.Group, scope.Member, scope.Generation, scope.RegistrationEpoch, base64.RawURLEncoding.EncodeToString(payload))
		resp, err := ch.Cluster.Router.ForwardToCoordinator(scope.Group, cmd)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(resp, "OK") {
			return fmt.Errorf("%s", resp)
		}
		return nil
	}
	if ch.Coordinator == nil {
		return fmt.Errorf("transaction offset commit requires coordinator")
	}
	return ch.Coordinator.ValidateAndCommitTopicOffsetsBulkForEpoch(scope.Group, scope.Member, scope.Generation, scope.RegistrationEpoch, offsetsByTopic)
}

func (ch *CommandHandler) materializeCommittedTransactionOffsets(ops []transaction.OffsetOperation) error {
	if len(ops) == 0 || ch.Coordinator == nil {
		return nil
	}
	scope := ops[0]
	offsets := make(map[string][]coordinator.OffsetItem)
	for _, op := range ops {
		if op.Group != scope.Group || op.RegistrationEpoch != scope.RegistrationEpoch {
			return fmt.Errorf("transaction offset scope mismatch during materialization")
		}
		offsets[op.Topic] = append(offsets[op.Topic], coordinator.OffsetItem{Partition: op.Partition, Offset: op.Offset})
	}
	return ch.Coordinator.MaterializeCommittedTransactionOffsets(scope.Group, scope.RegistrationEpoch, offsets)
}

func (ch *CommandHandler) materializeAndCheckpointTransactionOffsets(txnID string, ops []transaction.OffsetOperation) error {
	if len(ops) == 0 {
		return nil
	}
	if err := ch.materializeCommittedTransactionOffsets(ops); err != nil {
		return err
	}
	if err := ch.TxnManager.MarkOffsetsMaterialized(txnID); err != nil {
		return err
	}
	return ch.syncTransactionState(txnID)
}

func (ch *CommandHandler) ensureTransactionCoordinator(txnID string) string {
	if !ch.isDistributed() {
		return ""
	}
	coordAddr, isCoord, coordErr := ch.checkTransactionCoordinator(txnID)
	if coordErr != nil {
		return coordinatorUnavailableResponse
	}
	if !isCoord {
		return notCoordinatorResponse(coordAddr)
	}
	return ""
}
