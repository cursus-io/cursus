package controller

import (
	"context"
	"testing"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestReplicaCatchupPreservesHistoricalTransactionsAfterStateChanges(t *testing.T) {
	for _, phase := range []string{"prepared", "committed", "aborted", "reinitialized", "expired", "coordinator_changed"} {
		t.Run(phase, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.EnabledDistribution = true
			dm := disk.NewDiskManager(cfg)
			tm := topic.NewTopicManager(cfg, dm, nil)
			require.NoError(t, tm.CreateTopic("orders", 1, false, false))
			state := fsm.NewBrokerFSM(tm, nil)
			applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{
				Leader: "broker-1", LeaderEpoch: 7, CommittedHWM: 2, CommittedHWMKnown: true,
				Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1"}, PartitionCount: 1,
			})
			rm := &MockRaftManagerForForward{state: state}
			cluster := clusterController.NewClusterController(context.Background(), cfg, rm, nil, "broker-2", "broker-2:9001")
			ch := NewCommandHandler(tm, cfg, nil, nil, cluster)
			t.Cleanup(func() {
				_ = ch.Close()
				tm.Stop()
				dm.CloseAllHandlers()
			})
			producer, epoch, err := ch.TxnManager.InitProducerWithMode("history", transaction.ModeProcessingV1)
			require.NoError(t, err)
			require.NoError(t, ch.TxnManager.SetCoordinatorEpoch("history", 4))
			require.NoError(t, ch.TxnManager.Begin("history", producer, epoch))
			marker := types.TransactionMarkerCommit
			markerState := types.TransactionStateCommitted
			if phase == "aborted" {
				marker, markerState = types.TransactionMarkerAbort, types.TransactionStateAborted
				require.NoError(t, ch.TxnManager.Abort("history", producer, epoch))
			} else {
				_, err = ch.TxnManager.PrepareCommit("history", producer, epoch)
				require.NoError(t, err)
				if phase != "prepared" {
					require.NoError(t, ch.TxnManager.Commit("history"))
				}
			}
			switch phase {
			case "reinitialized":
				_, _, err = ch.TxnManager.InitProducerWithMode("history", transaction.ModeProcessingV1)
				require.NoError(t, err)
			case "expired":
				ch.TxnManager.Delete("history")
			case "coordinator_changed":
				require.NoError(t, ch.TxnManager.SetCoordinatorEpoch("history", 9))
			}
			key, value, err := transactionMarkerControlBytes(marker, 4)
			require.NoError(t, err)
			messages := []types.Message{
				{Offset: 0, Payload: "durable-input", ProducerID: producer, SeqNum: 1, Epoch: epoch, TransactionalID: "history", TransactionState: types.TransactionStateOpen},
				{Offset: 1, Payload: transactionControlMarkerPayload, ProducerID: "txn-marker:history:4:" + marker, SeqNum: 1, Epoch: epoch, TransactionalID: "history", TransactionState: markerState, TransactionMarker: marker,
					ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 4, ControlBatchKey: key, ControlBatchValue: value},
			}
			batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{
				Topic: "orders", Partition: 0, BrokerID: "broker-2", StartOffset: 0, EndOffset: 2, CommittedHWM: 2,
				Leader: "broker-1", SourceBroker: "broker-1", LeaderEpoch: 7, LifecycleEpoch: topic.InitialLifecycleEpoch, Messages: messages,
			})
			require.NoError(t, err)
			require.NoError(t, ch.ApplyReplicaCatchup(context.Background(), batch))
			partition, err := tm.GetTopic("orders").GetPartition(0)
			require.NoError(t, err)
			require.Equal(t, uint64(2), partition.NextOffset())
			require.Equal(t, uint64(2), partition.GetHWM())
			got, err := partition.ReadMessages(0, 10)
			require.NoError(t, err)
			require.Len(t, got, 2)
			require.Equal(t, messages[0].Payload, got[0].Payload)
			require.Equal(t, marker, got[1].TransactionMarker)
			require.Equal(t, int64(4), got[1].ControlBatchCoordinatorEpoch)
		})
	}
}
