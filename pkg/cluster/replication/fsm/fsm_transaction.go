package fsm

import (
	"encoding/json"
	"fmt"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
)

func (f *BrokerFSM) applyTransactionSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Transaction *transaction.Snapshot `json:"transaction"`
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
	f.mu.RUnlock()
	if txn == nil {
		return fmt.Errorf("transaction manager not available")
	}
	return txn.ApplyReplicatedSnapshot(cmd.Transaction)
}
