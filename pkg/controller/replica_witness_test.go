package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func localRecoveryRequest(t *testing.T, state *fsm.BrokerFSM, broker, name string, partition int) fsm.ReplicaCatchupRequest {
	t.Helper()
	for _, request := range state.BuildReplicaCatchupRequests(broker) {
		if request.Topic == name && request.Partition == partition {
			return request
		}
	}
	t.Fatalf("no local recovery request for %s-%d", name, partition)
	return fsm.ReplicaCatchupRequest{}
}

func receiverWitnessFixture(t *testing.T, count, end, protocol int, event bool, wrappers ...func(*disk.DiskManager) topic.HandlerProvider) (*CommandHandler, *fsm.BrokerFSM, *topic.Partition, *disk.DiskHandler) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	dm := disk.NewDiskManager(cfg)
	var provider topic.HandlerProvider = dm
	if len(wrappers) > 0 {
		provider = wrappers[0](dm)
	}
	tm := topic.NewTopicManager(cfg, provider, nil)
	state := fsm.NewBrokerFSM(tm, nil)
	for _, id := range []string{"source", "receiver"} {
		data, err := json.Marshal(fsm.BrokerInfo{ID: id, Addr: id + ":9001", Status: "active", RecoveryProtocol: protocol})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
	}
	definition := topic.DefaultDefinition("orders", cfg)
	definition.Partitions = 1
	definition.ReplicationFactor = 2
	definition.EventSourcing = event
	data, err := json.Marshal(fsm.TopicCommand{Definition: &definition, LeaderID: "source"})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), data...)}))
	metadata := state.GetPartitionMetadata("orders-0")
	metadata.ISR = []string{"source"}
	metadata.CommittedHWM = uint64(end)
	metadata.CommittedHWMKnown = true
	applyPartitionMetadata(t, state, "orders", 0, *metadata)
	cc := clusterController.NewClusterController(context.Background(), cfg, &MockRaftManagerForForward{state: state}, nil, "receiver", "receiver:9001")
	handler := NewCommandHandler(tm, cfg, nil, nil, cc)
	t.Cleanup(func() { _ = handler.Close(); tm.Stop(); dm.CloseAllHandlers() })
	p, err := tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	for i := 0; i < count; i++ {
		require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: uint64(i), Payload: fmt.Sprintf("record-%d", i)}}))
	}
	require.NoError(t, p.ApplyReplicaHWM(uint64(count)))
	p.FlushDisk()
	storage, err := dm.GetHandler("orders", 0)
	require.NoError(t, err)
	return handler, state, p, storage.(*disk.DiskHandler)
}

func rewriteReceiverPrefix(t *testing.T, d *disk.DiskHandler, end int) {
	t.Helper()
	require.NoError(t, d.TruncateTo(0))
	records := make([]types.DiskMessage, end)
	for i := range records {
		records[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: fmt.Sprintf("record-%d", i)}
	}
	if end > 1 {
		records[0].Payload = "different earlier record"
	}
	require.NoError(t, d.WriteBatchSync(records))
}

func TestReceiverWitnessOriginalRequestRejectsEarlierRewriteWithIdenticalLastRecord(t *testing.T) {
	for _, protocol := range []int{0, 1} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			h, state, p, d := receiverWitnessFixture(t, 2, 3, protocol, false)
			_, source, _, _ := receiverWitnessFixture(t, 3, 3, protocol, false)
			request := localRecoveryRequest(t, state, "receiver", "orders", 0)
			encoded, err := json.Marshal(request)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "LocalWitness")
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			rewriteReceiverPrefix(t, d, 2)
			witness, err := h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
			require.ErrorIs(t, err, disk.ErrStaleContentGeneration)
			require.Nil(t, witness)
			require.Equal(t, uint64(2), p.NextOffset())
			require.Empty(t, state.BuildISRCatchupProofs("receiver"))
		})
	}
}

func TestReceiverWitnessActualTerminalApplyThenMarkAndHeartbeatRejectRewrite(t *testing.T) {
	for _, phase := range []string{"before_mark", "after_mark"} {
		t.Run(phase, func(t *testing.T) {
			h, state, p, d := receiverWitnessFixture(t, 2, 3, 1, false)
			_, source, _, _ := receiverWitnessFixture(t, 3, 3, 1, false)
			request := localRecoveryRequest(t, state, "receiver", "orders", 0)
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			witness, err := h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
			require.NoError(t, err)
			require.NotNil(t, witness)
			require.NoError(t, fsm.AdvanceReplicaCatchupRequest(&request, batch))
			if phase == "after_mark" {
				require.NoError(t, state.MarkReplicaCatchupVerified(request, witness))
				require.Len(t, state.BuildISRCatchupProofs("receiver"), 1)
			}
			rewriteReceiverPrefix(t, d, 3)
			require.Equal(t, uint64(3), p.GetHWM())
			require.Equal(t, uint64(3), p.NextOffset())
			require.Error(t, state.MarkReplicaCatchupVerified(request, witness))
			require.Empty(t, state.BuildISRCatchupProofs("receiver"))
		})
	}
}

type witnessSourceFetcher struct {
	calls      int
	failSecond bool
	source     *fsm.BrokerFSM
	after      func()
}

func (f *witnessSourceFetcher) FetchReplicaCatchup(ctx context.Context, _ string, _ int, request fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	f.calls++
	if f.failSecond && f.calls > 1 {
		return fsm.ReplicaCatchupBatch{}, fmt.Errorf("second fetch stopped after stale witness rejection")
	}
	batch, err := f.source.FetchReplicaCatchupContext(ctx, request)
	if f.after != nil {
		f.after()
	}
	return batch, err
}

func TestReceiverWitnessSchedulerUsesActualApplyAndRechecksAfterSnapshotWork(t *testing.T) {
	for _, phase := range []string{"success", "old_callback", "snapshot_rewrite"} {
		t.Run(phase, func(t *testing.T) {
			event := phase == "snapshot_rewrite"
			count, end := 2, 3
			if event {
				count, end = 0, 0
			}
			h, state, _, d := receiverWitnessFixture(t, count, end, 1, event)
			_, source, _, _ := receiverWitnessFixture(t, end, end, 1, event)
			submitted := 0
			h.Cluster.SetReplicaCatchupProofSubmitter(func(context.Context, fsm.ISRCatchupProof) error { submitted++; return nil })

			if event {
				h.Cluster.SetReplicaSnapshotCatchup(func(context.Context, fsm.ReplicaCatchupRequest, *topic.ReplicaCatchupWitness) error {
					_, err := d.AppendMessageSync("orders", 0, &types.Message{Payload: "temporary rewritten tail"})
					require.NoError(t, err)
					require.NoError(t, d.TruncateTo(0))
					return nil
				})
			}
			fetcher := &witnessSourceFetcher{source: source, failSecond: event}
			var err error
			if phase == "old_callback" {
				err = h.Cluster.RunReplicaCatchupOnce(context.Background(), fetcher, h.ApplyReplicaCatchup)
			} else {
				err = h.Cluster.RunReplicaCatchupOnceVerified(context.Background(), fetcher, h.ApplyReplicaCatchupVerified)
			}
			if event {
				require.Equal(t, 2, fetcher.calls, "successful snapshot transfer must reach stale Mark, then replan")
			}
			if phase == "success" {
				require.NoError(t, err)
				require.Equal(t, 1, submitted)
				require.Len(t, state.BuildISRCatchupProofs("receiver"), 1)
			} else {
				require.Error(t, err)
				require.Zero(t, submitted)
				require.Empty(t, state.BuildISRCatchupProofs("receiver"))
			}
		})
	}
}

func TestReceiverWitnessEmptyTerminalRequiresEventIndexAndContext(t *testing.T) {
	for _, phase := range []string{"missing_index", "canceled"} {
		t.Run(phase, func(t *testing.T) {
			h, state, _, _ := receiverWitnessFixture(t, 0, 0, 0, true)
			_, source, _, _ := receiverWitnessFixture(t, 0, 0, 0, true)
			request := localRecoveryRequest(t, state, "receiver", "orders", 0)
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if phase == "missing_index" {
				h.ESHandler = nil
			} else {
				cancel()
			}
			witness, err := h.ApplyReplicaCatchupVerified(ctx, request, batch)
			require.Error(t, err)
			require.Nil(t, witness)
			require.Empty(t, state.BuildISRCatchupProofs("receiver"))
		})
	}
}

func TestReceiverWitnessModeZeroRejectsReceiverRejoiningISRDuringFetch(t *testing.T) {
	h, state, p, _ := receiverWitnessFixture(t, 2, 3, 0, false)
	_, source, _, _ := receiverWitnessFixture(t, 3, 3, 0, false)
	request := localRecoveryRequest(t, state, "receiver", "orders", 0)
	batch, err := source.FetchReplicaCatchup(request)
	require.NoError(t, err)
	metadata := state.GetPartitionMetadata("orders-0")
	metadata.ISR = append(metadata.ISR, "receiver")
	applyPartitionMetadata(t, state, "orders", 0, *metadata)
	witness, err := h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
	require.Error(t, err)
	require.Nil(t, witness)
	require.Equal(t, uint64(2), p.NextOffset())
}

func TestReceiverWitnessRepairsEventIndexBeforeClearingSameEpochMarker(t *testing.T) {
	for _, phase := range []string{"missing_cache", "empty_cache", "divergent_cache"} {
		t.Run(phase, func(t *testing.T) {
			h, state, p, _ := receiverWitnessFixture(t, 0, 2, 1, true)
			_, source, sourcePartition, _ := receiverWitnessFixture(t, 0, 2, 1, true)
			canonical := []types.Message{{Offset: 0, Key: "common", Payload: "common", EventType: "created", SchemaVersion: 1, AggregateVersion: 1}, {Offset: 1, Key: "canonical", Payload: "replacement", EventType: "created", SchemaVersion: 1, AggregateVersion: 1}}
			require.NoError(t, sourcePartition.ReplicaAppend(canonical))
			require.NoError(t, sourcePartition.ApplyReplicaHWM(2))
			if phase == "empty_cache" {
				require.NoError(t, h.ESHandler.PrepareCommittedIndex("orders", 0))
			}
			local := append([]types.Message(nil), canonical...)
			if phase == "divergent_cache" {
				local[1].Key = "divergent"
			}
			require.NoError(t, p.ReplicaAppend(local))
			require.NoError(t, p.ApplyReplicaHWM(2))
			if phase == "divergent_cache" {
				require.NoError(t, h.ESHandler.PrepareCommittedIndex("orders", 0))
				require.Equal(t, "OK version=1", h.ESHandler.HandleStreamVersion("STREAM_VERSION topic=orders key=divergent"))
				request := localRecoveryRequest(t, state, "receiver", "orders", 0)
				batch, err := source.FetchReplicaCatchup(request)
				require.NoError(t, err)
				require.NotNil(t, batch.TruncateTo)
				_, err = h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
				require.NoError(t, err)
			}
			marker := errors.New("same-epoch canonical repair pending")
			p.MarkReconciliationPending(marker)
			request := localRecoveryRequest(t, state, "receiver", "orders", 0)
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			witness, err := h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
			require.NoError(t, err)
			require.NoError(t, fsm.AdvanceReplicaCatchupRequest(&request, batch))
			require.ErrorIs(t, p.ReconciliationError(), marker, "only final witness admission may clear the marker")
			require.NoError(t, state.MarkReplicaCatchupVerified(request, witness))
			require.NoError(t, p.ReconciliationError())
			require.Equal(t, "OK version=1", h.ESHandler.HandleStreamVersion("STREAM_VERSION topic=orders key=canonical"))
			require.Equal(t, "OK version=0", h.ESHandler.HandleStreamVersion("STREAM_VERSION topic=orders key=divergent"))
		})
	}
}
