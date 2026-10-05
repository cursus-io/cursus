package controller

import (
	"fmt"
	"time"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
)

func (ch *CommandHandler) snapshotTransaction(txnID string) (*transaction.Snapshot, bool) {
	return ch.TxnManager.Snapshot(txnID)
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
	if ch.TopicManager == nil {
		return fmt.Errorf("standalone transaction recovery requires a topic manager")
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
	if err := ch.TxnManager.RestoreProducerEpochWatermark(journal.NextProducerEpoch()); err != nil {
		return fmt.Errorf("restore producer epoch watermark: %w", err)
	}
	if !journal.HasProducerEpochWatermark() {
		next, err := ch.TopicManager.RetainedTransactionProducerEpoch()
		if err != nil {
			return fmt.Errorf("migrate transaction journal epoch watermark: %w", err)
		}
		if ch.Coordinator != nil {
			for groupName, group := range ch.Coordinator.ExportState() {
				for _, reservation := range group.OffsetReservations {
					next, err = advanceRecoveredProducerEpoch(next, reservation.ProducerEpoch)
					if err != nil {
						return fmt.Errorf("migrate transaction journal epoch watermark from group %q reservation: %w", groupName, err)
					}
				}
				for _, decision := range group.ReservationDecisions {
					next, err = advanceRecoveredProducerEpoch(next, decision.ProducerEpoch)
					if err != nil {
						return fmt.Errorf("migrate transaction journal epoch watermark from group %q decision: %w", groupName, err)
					}
				}
			}
		}
		if err := ch.TxnManager.RestoreProducerEpochWatermark(next); err != nil {
			return fmt.Errorf("migrate transaction producer epoch: %w", err)
		}
		state, next := ch.TxnManager.ExportStateWithProducerEpoch()
		if err := journal.RewriteWithProducerEpoch(state, next); err != nil {
			return fmt.Errorf("persist migrated producer epoch watermark: %w", err)
		}
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

func advanceRecoveredProducerEpoch(current uint64, producerEpoch int64) (uint64, error) {
	epoch, ok := util.SafeInt64ToUint64(producerEpoch)
	if !ok {
		return current, fmt.Errorf("negative producer epoch %d", producerEpoch)
	}
	return max(current, epoch+1), nil
}

func (ch *CommandHandler) syncTransactionState(txnID string) error {
	if ch.transactionStateSyncHook != nil {
		return ch.transactionStateSyncHook(txnID)
	}
	snapshot, ok := ch.TxnManager.Snapshot(txnID)
	if !ok {
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
	if err := ch.persistFinalTransactionDecision(snapshot); err != nil {
		return err
	}
	tx, err := ch.TxnManager.Status(txnID)
	if err != nil {
		return err
	}
	return ch.resolveAndCheckpointTransactionReservations(tx)
}

func (ch *CommandHandler) persistFinalTransactionDecision(snapshot *transaction.Snapshot) error {
	if ch.isDistributed() {
		payload, err := ch.transactionSyncPayload(snapshot)
		if err != nil {
			return err
		}
		_, err = ch.applyViaLeader("TXN_SYNC", payload)
		if err != nil {
			return err
		}
		// The transaction coordinator can be a Raft follower. A successful
		// forwarded apply confirms the leader's decision, but does not mean
		// this node has applied it yet. Do not initialize another epoch or
		// materialize offsets while the local state is still prepared.
		return ch.waitForTransactionDecision(snapshot)
	}
	if ch.txnJournal != nil {
		if err := ch.txnJournal.Append(snapshot); err != nil {
			return err
		}
	}
	return ch.TxnManager.ApplyReplicatedSnapshot(snapshot)
}

func (ch *CommandHandler) waitForTransactionDecision(snapshot *transaction.Snapshot) error {
	timer := time.NewTimer(DefaultFSMApplyTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, ok := ch.TxnManager.Snapshot(snapshot.ID)
		if ok && current.Epoch == snapshot.Epoch && current.Revision >= snapshot.Revision && current.State == snapshot.State &&
			(snapshot.OffsetReservationsPending || !current.OffsetReservationsPending) && (!snapshot.OffsetsMaterialized || current.OffsetsMaterialized) {
			return nil
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("timed out waiting for local transaction decision transactional_id=%s epoch=%d revision=%d", snapshot.ID, snapshot.Epoch, snapshot.Revision)
		}
	}
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
	if owner != localOwner || (snapshot.CoordinatorEpoch != epoch && !ch.TxnManager.IsOffsetReservationCleanupSnapshot(snapshot)) {
		return nil, fmt.Errorf(
			"transaction coordinator fenced transactional_id=%s current_owner=%s current_epoch=%d local_owner=%s local_epoch=%d",
			snapshot.ID, owner, epoch, localOwner, snapshot.CoordinatorEpoch,
		)
	}
	payload["coordinator_owner"] = owner
	payload["coordinator_epoch"] = epoch
	return payload, nil
}
