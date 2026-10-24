package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryControllerPublishesAndRestores(t *testing.T) {
	for _, mode := range []string{"local", "nonreplica", "transactions"} {
		t.Run(mode, func(t *testing.T) { testConsumerHistoryController(t, mode) })
	}
}

type historyStagedTestReader struct {
	*stagedConsumerTestReader
}

func (r historyStagedTestReader) ConsumerHistoryIdentity(int) (string, error) {
	return "fixture-certified-generation", nil
}
func (r historyStagedTestReader) ReadRawConsumerHistory(p int, next uint64, max int) ([]types.Message, error) {
	return r.ReadCommittedConsumerMetadata(config.ConsumerOffsetsTopicName, p, next, max)
}

func testConsumerHistoryController(t *testing.T, mode string) {
	remote := mode == "nonreplica"
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution, cfg.ConsumerRecoveryIndexEnabled, cfg.ConsumerHistoryCompactionEnabled = true, true, true
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreePercent = 0
	cfg.SegmentSize = 2048
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	tm := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(tm.Stop)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	state := fsm.NewBrokerFSM(tm, c)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"local","addr":"127.0.0.1:7000","status":"active","incarnation_id":"local-process","lifecycle_protocol":11,"recovery_protocol":2}`)}))
	definition := topic.DefaultDefinition(config.ConsumerOffsetsTopicName, cfg)
	definition.Partitions, definition.ReplicationFactor = 4, 1
	definition.Policy = topic.ConsumerMetadataPolicy()
	require.Empty(t, definition.InstanceID, "match auto-created internal topic")
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition, LeaderID: "local"})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Index: 2, Data: append([]byte("TOPIC:"), payload...)}))
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager, h.Config = tm, cfg
	h.Cluster = clusterController.NewClusterController(context.Background(), cfg, h.Cluster.RaftManager, nil, "local", "127.0.0.1:7000")
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	for _, p := range tm.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		p.SetHWM(0)
	}
	p, err := tm.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	for i := 0; i <= 100; i++ {
		record := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: "g", Topic: "orders", Epoch: 1, Revision: uint64(i), Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: uint64(i)}}}
		if i == 0 {
			record.Type, record.PartitionCount, record.Offsets = coordinator.ConsumerMetadataRecordRegistration, 1, nil
			record.Timestamp = time.Unix(1, 0)
		}
		message := consumerMetadataMessage(t, record, uint64(i))
		message.ProducerID = "consumer-offset-coordinator"
		require.NoError(t, p.ReplicaAppend([]types.Message{message}))
		p.FlushDisk()
	}
	end := uint64(101)
	if mode == "transactions" {
		manager := transaction.NewManager()
		state.SetTransactionManager(manager)
		h.TxnManager = manager
		tm.SetTransactionDecisionResolver(manager)
		for i := 0; i < 100; i++ {
			producer, epoch, err := manager.InitProducerWithMode("tx", transaction.ModeProcessingV1)
			require.NoError(t, err)
			record := coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersionTransactionHistory, Type: coordinator.ConsumerMetadataRecordTransactionalOffsetSnapshot, Group: "g", Topic: "orders", Epoch: 1, Revision: uint64(i + 1), TransactionalID: "tx", ProducerID: producer, ProducerEpoch: epoch, CoordinatorEpoch: 7, Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: uint64(i + 1)}}}
			intent := consumerMetadataMessage(t, record, end)
			intent.TransactionalID, intent.ProducerID, intent.Epoch, intent.SeqNum, intent.TransactionState = "tx", producer, epoch, 1, types.TransactionStateOpen
			markerType, txState := types.TransactionMarkerCommit, types.TransactionStateCommitted
			if i%2 == 0 {
				markerType, txState = types.TransactionMarkerAbort, types.TransactionStateAborted
			}
			key, value, err := types.TransactionMarkerControlBytes(markerType, 7)
			require.NoError(t, err)
			marker := types.Message{Offset: end + 1, Payload: types.TransactionControlMarkerPayload, TransactionalID: "tx", ProducerID: fmt.Sprintf("txn-history:tx:%s", markerType), Epoch: epoch, SeqNum: 7, TransactionState: txState, TransactionMarker: markerType, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 7, ControlBatchKey: key, ControlBatchValue: value}
			require.NoError(t, p.ReplicaAppend([]types.Message{intent, marker}))
			p.FlushDisk()
			end += 2
		}
		_, _, err = manager.InitProducerWithMode("tx", transaction.ModeProcessingV1)
		require.NoError(t, err)
	}
	require.NoError(t, p.ApplyReplicaHWM(end))
	metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	metadata.CommittedHWM = end
	applyProjectionMetadata(t, state, 0, metadata)
	var stages int
	if remote {
		require.Nil(t, state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"remote","addr":"127.0.0.1:7001","status":"active","incarnation_id":"remote-process","lifecycle_protocol":10,"recovery_protocol":2}`)}))
		remoteMetadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-1")
		remoteMetadata.Leader, remoteMetadata.Replicas, remoteMetadata.ISR, remoteMetadata.CommittedHWM = "remote", []string{"remote"}, []string{"remote"}, 1
		remoteMetadata.LeaderEpoch++
		applyProjectionMetadata(t, state, 1, remoteMetadata)
		broker := state.GetBroker("remote")
		h.SetConsumerMetadataRecoveryProvider(func(ctx context.Context, request ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error) {
			require.Equal(t, []int{1}, request.Partitions)
			stages++
			// Network staging may acquire admission; the cleaner cannot own
			// that lease or the FSM fence while it calls the provider.
			release, err := c.BeginConsumerMetadataRecovery(ctx)
			require.NoError(t, err)
			release()
			record := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "remote-group", Topic: "orders", Epoch: 1, PartitionCount: 1, Timestamp: time.Unix(1, 0)}
			return historyStagedTestReader{&stagedConsumerTestReader{ends: []uint64{101, 1, 0, 0}, sources: map[int]ConsumerMetadataRecoverySource{1: {BrokerID: broker.ID, IncarnationID: broker.IncarnationID, IncarnationEpoch: broker.IncarnationEpoch}}, records: map[int][]types.Message{1: {consumerMetadataMessage(t, record, 0)}}}}, nil
		})
	}
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	cursor := consumerHistoryCursor{}
	result, err := h.compactConsumerHistory(context.Background(), &cursor)
	require.NoError(t, err)
	require.Equal(t, "activation_pending", result.SkippedReason)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`CONSUMER_ROUTING_ACTIVATE:{"voters":["local"]}`)}))
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`CONSUMER_HISTORY_ACTIVATE:{"voters":["local"]}`)}))
	if mode == "transactions" {
		_, err := h.compactConsumerHistory(context.Background(), &cursor)
		require.ErrorContains(t, err, "consumer transaction history activation pending")
		require.Nil(t, state.Apply(&raft.Log{Data: []byte(`CONSUMER_TRANSACTION_HISTORY_ACTIVATE:{"voters":["local"]}`)}))
	}
	before := c.ExportState()
	var removed int
	for i := 0; i < 40; i++ {
		result, err = h.compactConsumerHistory(context.Background(), &cursor)
		require.NoError(t, err)
		removed += result.RecordsRemoved
	}
	require.Greater(t, removed, 80)
	if mode == "transactions" {
		require.Greater(t, removed, 270, "retired raw intents and controls must actually leave disk")
	}
	if remote {
		require.Greater(t, stages, 2, "certification and final source refresh use the staged nonreplica")
	}
	require.Equal(t, before, c.ExportState(), "maintenance must not replace live groups")
	files, err := filepath.Glob(filepath.Join(cfg.LogDir, config.ConsumerOffsetsTopicName, "partition_0_segment_*.log"))
	require.NoError(t, err)
	limit := 3
	if mode == "transactions" {
		limit = 5 // ordinary state plus raw producer/control witnesses
	}
	require.LessOrEqual(t, len(files), limit, "only state witnesses, anchor and active tail remain")
	if !remote {
		require.NoError(t, os.Remove(filepath.Join(cfg.LogDir, ".consumer-replay-index.json")))
	} else {
		require.NoFileExists(t, filepath.Join(cfg.LogDir, ".consumer-replay-index.json"))
	}
	require.NoError(t, h.recoverDistributedConsumerMetadata(context.Background(), true))
	require.False(t, c.RecoverySnapshot().IndexHit)
	value, ok := c.GetOffset("g", "orders", 0)
	require.True(t, ok)
	require.Equal(t, uint64(100), value)
	if mode == "transactions" {
		// Production read views resolve the cached raw evidence without reading
		// hidden frames again; this exercises the resolver omitted by mock readers.
		require.NoError(t, h.recoverDistributedConsumerMetadata(context.Background(), true))
		status := c.RecoverySnapshot()
		require.True(t, status.IndexHit)
		require.Positive(t, status.IndexTransactionChecks)
		require.Zero(t, status.IndexSourceRecords)
		value, ok = c.GetOffset("g", "orders", 0)
		require.True(t, ok)
		require.Equal(t, uint64(100), value)
	}
	snapshot, err := state.Snapshot()
	require.NoError(t, err)
	sink := &consumerRecoverySnapshotSink{}
	require.NoError(t, snapshot.Persist(sink))
	snapshot.Release()
	require.NoError(t, state.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))
	require.True(t, state.ConsumerHistoryEnabled())
	require.NoError(t, state.ReconcileTopicMaterializations())
	require.NoError(t, state.FinalizeRecoveredPartitions())
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	value, ok = c.GetOffset("g", "orders", 0)
	require.True(t, ok)
	require.Equal(t, uint64(100), value)
}
