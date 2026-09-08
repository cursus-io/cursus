package controller

import (
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/transaction"
)

func (ch *CommandHandler) snapshotTransaction(txnID string) (*transaction.Snapshot, bool) {
	state := ch.TxnManager.ExportState()
	snapshot, ok := state[txnID]
	return snapshot, ok
}

func (ch *CommandHandler) restoreTransaction(txnID string, snapshot *transaction.Snapshot, hadPrevious bool) {
	if hadPrevious {
		ch.TxnManager.ApplySnapshot(snapshot)
		return
	}
	ch.TxnManager.Delete(txnID)
}

func (ch *CommandHandler) ConfigureTransactionJournal(path string) error {
	if ch.isDistributed() {
		return fmt.Errorf("standalone transaction journal cannot be enabled in distributed mode")
	}
	journal, err := transaction.OpenJournal(path)
	if err != nil {
		return err
	}
	state, err := journal.Load()
	if err != nil {
		return err
	}
	if err := ch.TxnManager.ImportState(state); err != nil {
		return fmt.Errorf("validate recovered transaction journal: %w", err)
	}
	if ch.TxnManager.PruneExpired(time.Now()) > 0 {
		if err := journal.Rewrite(ch.TxnManager.ExportState()); err != nil {
			return fmt.Errorf("prune recovered transaction journal: %w", err)
		}
	}
	ch.txnJournal = journal
	if err := ch.RecoverPendingTruncations(); err != nil {
		return fmt.Errorf("recover pending topic truncation: %w", err)
	}
	return nil
}

func (ch *CommandHandler) syncTransactionState(txnID string) error {
	if ch.transactionStateSyncHook != nil {
		return ch.transactionStateSyncHook(txnID)
	}
	snapshots := ch.TxnManager.ExportState()
	snapshot := snapshots[txnID]
	if snapshot == nil {
		return fmt.Errorf("transaction %s not found", txnID)
	}
	if !ch.isDistributed() {
		if ch.txnJournal == nil {
			return nil
		}
		if err := ch.txnJournal.Append(snapshot); err != nil {
			return err
		}
		if ch.TxnManager.PruneExpired(time.Now()) > 0 {
			return ch.txnJournal.Rewrite(ch.TxnManager.ExportState())
		}
		return nil
	}
	payload, err := ch.transactionSyncPayload(snapshot)
	if err != nil {
		return err
	}
	_, err = ch.applyViaLeader("TXN_SYNC", payload)
	return err
}

func (ch *CommandHandler) commitTransactionDecision(txnID string) error {
	snapshot, err := ch.TxnManager.BuildCommittedSnapshot(txnID)
	if err != nil {
		return err
	}
	return ch.persistFinalTransactionDecision(snapshot)
}

func (ch *CommandHandler) abortTransactionDecision(txnID, producerID string, epoch int64) error {
	snapshot, err := ch.TxnManager.BuildAbortedSnapshot(txnID, producerID, epoch)
	if err != nil {
		return err
	}
	return ch.persistFinalTransactionDecision(snapshot)
}

func (ch *CommandHandler) persistFinalTransactionDecision(snapshot *transaction.Snapshot) error {
	if ch.isDistributed() {
		payload, err := ch.transactionSyncPayload(snapshot)
		if err != nil {
			return err
		}
		_, err = ch.applyViaLeader("TXN_SYNC", payload)
		return err
	}
	if ch.txnJournal != nil {
		if err := ch.txnJournal.Append(snapshot); err != nil {
			return err
		}
	}
	return ch.TxnManager.ApplyReplicatedSnapshot(snapshot)
}

func (ch *CommandHandler) transactionSyncPayload(snapshot *transaction.Snapshot) (map[string]interface{}, error) {
	payload := map[string]interface{}{"transaction": snapshot}
	if snapshot == nil || snapshot.Mode != transaction.ModeProcessingV1 || !ch.isDistributed() {
		return payload, nil
	}
	if ch.Cluster.Router == nil {
		return nil, fmt.Errorf("transaction coordinator router is unavailable")
	}
	owner, _, epoch, err := ch.Cluster.Router.FindTransactionCoordinator(snapshot.ID)
	if err != nil {
		return nil, err
	}
	localOwner := ch.Cluster.Router.BrokerID()
	if owner != localOwner || snapshot.CoordinatorEpoch != epoch {
		return nil, fmt.Errorf(
			"transaction coordinator fenced transactional_id=%s current_owner=%s current_epoch=%d local_owner=%s local_epoch=%d",
			snapshot.ID, owner, epoch, localOwner, snapshot.CoordinatorEpoch,
		)
	}
	payload["coordinator_owner"] = owner
	payload["coordinator_epoch"] = epoch
	return payload, nil
}
