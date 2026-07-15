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
	f.mu.RUnlock()
	if txn == nil {
		return fmt.Errorf("transaction manager not available")
	}
	txn.ApplySnapshot(cmd.Transaction)
	return nil
}
