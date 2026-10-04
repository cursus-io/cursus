package fsm

import (
	"encoding/json"
	"fmt"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
)

func (f *BrokerFSM) OffsetReservationsEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.offsetReservationsActivated
}

func (f *BrokerFSM) applyTransactionSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Transaction      *transaction.Snapshot `json:"transaction"`
		CoordinatorOwner string                `json:"coordinator_owner,omitempty"`
		CoordinatorEpoch int64                 `json:"coordinator_epoch,omitempty"`
	}
	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("FSM: Failed to unmarshal TXN_SYNC: %v", err)
		return err
	}
	if cmd.Transaction == nil || cmd.Transaction.ID == "" {
		return fmt.Errorf("invalid transaction sync payload")
	}
	f.mu.RLock()
	txn := f.txn
	if cmd.Transaction.OffsetReservationsPending {
		for _, broker := range f.brokers {
			if broker.LifecycleProtocol < OffsetReservationsProtocolVersion {
				f.mu.RUnlock()
				return fmt.Errorf("offset reservations require broker protocol %d on every registered broker", OffsetReservationsProtocolVersion)
			}
		}
	}
	for _, operation := range cmd.Transaction.Messages {
		if f.topicState[operation.Topic] == nil {
			f.mu.RUnlock()
			return fmt.Errorf("transaction topic %q is not present in cluster state", operation.Topic)
		}
	}
	for _, operation := range cmd.Transaction.Offsets {
		if f.topicState[operation.Topic] == nil {
			f.mu.RUnlock()
			return fmt.Errorf("transaction offset topic %q is not present in cluster state", operation.Topic)
		}
	}
	shard := transaction.CoordinatorShardForCount(cmd.Transaction.ID, f.effectiveTransactionCoordinatorShardCountLocked())
	ownership := f.transactionCoordinatorShards[shard]
	f.mu.RUnlock()
	if txn == nil {
		return fmt.Errorf("transaction manager not available")
	}
	if cmd.Transaction.Mode == transaction.ModeProcessingV1 {
		if current, ok := txn.Snapshot(cmd.Transaction.ID); ok && !current.Ready &&
			(current.State == transaction.StateCommitted || current.State == transaction.StateAborted) &&
			current.Epoch == cmd.Transaction.Epoch && current.CoordinatorEpoch != cmd.Transaction.CoordinatorEpoch {
			return fmt.Errorf("terminal transaction decision epoch is immutable transactional_id=%s", cmd.Transaction.ID)
		}
		if ownership.Owner == "" || ownership.Epoch <= 0 {
			return fmt.Errorf("transaction coordinator unavailable for shard %d", shard)
		}
		if cmd.CoordinatorOwner != ownership.Owner || cmd.CoordinatorEpoch != ownership.Epoch ||
			(cmd.Transaction.CoordinatorEpoch != ownership.Epoch && !txn.IsOffsetReservationCleanupSnapshot(cmd.Transaction)) {
			return fmt.Errorf(
				"transaction coordinator fenced transactional_id=%s current_owner=%s current_epoch=%d requested_owner=%s requested_epoch=%d",
				cmd.Transaction.ID, ownership.Owner, ownership.Epoch, cmd.CoordinatorOwner, cmd.CoordinatorEpoch,
			)
		}
	}
	if err := txn.ApplyReplicatedSnapshot(cmd.Transaction); err != nil {
		return err
	}
	if cmd.Transaction.OffsetReservationsPending {
		f.mu.Lock()
		f.offsetReservationsActivated = true
		f.mu.Unlock()
	}
	return nil
}
