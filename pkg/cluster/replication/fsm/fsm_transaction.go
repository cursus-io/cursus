package fsm

import (
	"encoding/json"
	"fmt"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
)

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
		if ownership.Owner == "" || ownership.Epoch <= 0 {
			return fmt.Errorf("transaction coordinator unavailable for shard %d", shard)
		}
		if cmd.CoordinatorOwner != ownership.Owner || cmd.CoordinatorEpoch != ownership.Epoch || cmd.Transaction.CoordinatorEpoch != ownership.Epoch {
			return fmt.Errorf(
				"transaction coordinator fenced transactional_id=%s current_owner=%s current_epoch=%d requested_owner=%s requested_epoch=%d",
				cmd.Transaction.ID, ownership.Owner, ownership.Epoch, cmd.CoordinatorOwner, cmd.CoordinatorEpoch,
			)
		}
	}
	return txn.ApplyReplicatedSnapshot(cmd.Transaction)
}
