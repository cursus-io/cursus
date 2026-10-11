package controller

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func TestCommandHandlerPreservesRecoveredTransactionManager(t *testing.T) {
	for _, decision := range []transaction.State{transaction.StateAborted, transaction.StateCommitted} {
		t.Run(string(decision), func(t *testing.T) {
			const id = "recovered-decision"
			recovered := transaction.NewManager()
			producer, epoch, err := recovered.InitProducerWithMode(id, transaction.ModeProcessingV1)
			require.NoError(t, err)
			require.NoError(t, recovered.Begin(id, producer, epoch))
			var terminal *transaction.Snapshot
			if decision == transaction.StateAborted {
				_, err = recovered.PrepareAbort(id, producer, epoch)
				require.NoError(t, err)
				terminal, err = recovered.BuildAbortedSnapshot(id, producer, epoch)
			} else {
				_, err = recovered.PrepareCommit(id, producer, epoch)
				require.NoError(t, err)
				terminal, err = recovered.BuildCommittedSnapshot(id)
			}
			require.NoError(t, err)
			require.NoError(t, recovered.ApplyReplicatedSnapshot(terminal))
			brokerFSM := fsm.NewBrokerFSM(nil, nil)
			brokerFSM.SetTransactionManager(recovered)
			handler := newCoordinatorRoutingHandler("node-a", brokerFSM, nil)
			require.Same(t, recovered, handler.TxnManager)
			require.Same(t, recovered, brokerFSM.GetTransactionManager())
			actual, ok := handler.TxnManager.Snapshot(id)
			require.True(t, ok)
			require.Equal(t, terminal, actual)
		})
	}
}
