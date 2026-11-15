package controller

import (
	"context"
	"encoding/json"
	"errors"
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

func compactedQuarantineFixture(t *testing.T, source bool) (*CommandHandler, *fsm.BrokerFSM, *topic.Partition, *disk.DiskHandler) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	cfg.CleanupPolicy = config.CleanupPolicyCompact
	cfg.CompactionCheckIntervalMS = 60000
	cfg.RetentionCheckIntervalMS = 60000
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	state := fsm.NewBrokerFSM(tm, nil)
	for _, id := range []string{"source", "receiver"} {
		data, err := json.Marshal(fsm.BrokerInfo{ID: id, Addr: id + ":9001", Status: "active", RecoveryProtocol: fsm.BootstrapRecoveryProtocolVersion, LifecycleProtocol: fsm.BrokerProtocolVersionCurrent, IncarnationID: id + "-process", IncarnationEpoch: 1})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
	}
	def := topic.DefaultDefinition("orders", cfg)
	def.Partitions = 1
	def.ReplicationFactor = 2
	data, err := json.Marshal(fsm.TopicCommand{Definition: &def, LeaderID: "source"})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), data...)}))
	meta := state.GetPartitionMetadata("orders-0")
	meta.ISR = []string{"source", "receiver"}
	meta.CommittedHWM = 3
	meta.CommittedHWMKnown = true
	applyPartitionMetadata(t, state, "orders", 0, *meta)
	cc := clusterController.NewClusterController(context.Background(), cfg, &MockRaftManagerForForward{state: state}, nil, "receiver", "receiver:9001")
	h := NewCommandHandler(tm, cfg, nil, nil, cc)
	t.Cleanup(func() { _ = h.Close(); tm.Stop(); dm.CloseAllHandlers() })
	p, err := tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	anchor := types.Message{Offset: 0, Key: "anchor", Payload: "anchor"}
	if source {
		require.NoError(t, p.ReplicaAppendCompactedRange([]types.Message{anchor, {Offset: 2, Key: "state", Payload: "current"}}, 3))
		require.NoError(t, p.ApplyReplicaHWM(3))
	} else {
		require.NoError(t, p.ReplicaAppend([]types.Message{anchor}))
		require.NoError(t, p.ApplyReplicaHWM(1))
	}
	p.FlushDisk()
	storage, err := dm.GetHandler("orders", 0)
	require.NoError(t, err)
	return h, state, p, storage.(*disk.DiskHandler)
}

type compactedQuarantineFetcher struct {
	source           *fsm.BrokerFSM
	responses, bytes int
}

func (f *compactedQuarantineFetcher) FetchReplicaCatchup(ctx context.Context, _ string, _ int, request fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	batch, err := f.source.FetchReplicaCatchupContext(ctx, request)
	if err == nil {
		encoded, encodeErr := json.Marshal(batch)
		if encodeErr != nil {
			return batch, encodeErr
		}
		f.responses++
		f.bytes += len(encoded)
	}
	return batch, err
}

type quarantineCatchupRaft struct {
	*MockRaftManagerForForward
	source      *fsm.BrokerFSM
	quarantines int
	reject      func() error
}

func (m *quarantineCatchupRaft) ApplyCommand(prefix string, data []byte) error {
	if prefix == "ISR_QUARANTINE" {
		m.quarantines++
		if m.reject != nil {
			return m.reject()
		}
	}
	if result := m.source.Apply(&raft.Log{Data: append([]byte(prefix+":"), data...)}); result != nil {
		if err, ok := result.(error); ok {
			return err
		}
	}
	return m.MockRaftManagerForForward.ApplyCommand(prefix, data)
}

// TestCompactedISRCatchupAutomaticallyQuarantines verifies recovery without a
// new producer write, followed by retained proof and normal ISR re-admission.
func TestCompactedISRCatchupAutomaticallyQuarantines(t *testing.T) {
	h, receiver, p, d := compactedQuarantineFixture(t, false)
	_, source, _, _ := compactedQuarantineFixture(t, true)
	ctx := context.Background()
	manager := &quarantineCatchupRaft{MockRaftManagerForForward: &MockRaftManagerForForward{state: receiver, isLeader: true}, source: source}
	h.Cluster.RaftManager = manager
	h.Cluster.SetReplicaCatchupProofSubmitter(func(_ context.Context, proof fsm.ISRCatchupProof) error {
		data, err := json.Marshal(proof)
		if err != nil {
			return err
		}
		return manager.ApplyCommand("ISR_CATCHUP", data)
	})
	fetcher := &compactedQuarantineFetcher{source: source}
	request := localRecoveryRequest(t, receiver, "receiver", "orders", 0)
	require.Zero(t, request.RecoveryProtocol)
	batch, err := source.FetchReplicaCatchup(request)
	require.NoError(t, err)
	require.True(t, batch.Compacted)
	require.Nil(t, batch.TruncateTo)
	err = h.Cluster.RunReplicaCatchupOnceVerified(ctx, fetcher, h.ApplyReplicaCatchupVerified)
	require.ErrorContains(t, err, "replica catch-up quarantine requested")
	require.Equal(t, uint64(1), p.NextOffset())
	require.Equal(t, uint64(1), p.GetHWM())
	require.NotContains(t, receiver.GetPartitionMetadata("orders-0").ISR, "receiver")
	require.Equal(t, 1, fetcher.responses)
	require.Equal(t, 1, manager.quarantines)
	request = localRecoveryRequest(t, receiver, "receiver", "orders", 0)
	require.Equal(t, fsm.BootstrapRecoveryProtocolVersion, request.RecoveryProtocol)
	batch, err = source.FetchReplicaCatchup(request)
	require.NoError(t, err)
	require.True(t, batch.Compacted)
	require.False(t, batch.BootstrapRequired)
	require.Nil(t, batch.TruncateTo)
	require.Len(t, batch.Messages, 1)
	require.Equal(t, uint64(2), batch.Messages[0].Offset)
	encoded, err := json.Marshal(batch)
	require.NoError(t, err)
	require.NoError(t, h.Cluster.RunReplicaCatchupOnceVerified(ctx, fetcher, h.ApplyReplicaCatchupVerified))
	require.Equal(t, uint64(3), p.NextOffset())
	require.Equal(t, uint64(3), p.GetHWM())
	require.NoError(t, p.RecoveryError())
	records, err := d.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Equal(t, "anchor", records[0].Payload)
	require.Equal(t, "current", records[1].Payload)
	for _, state := range []*fsm.BrokerFSM{source, receiver} {
		require.Contains(t, state.GetPartitionMetadata("orders-0").ISR, "receiver")
		require.Empty(t, state.GetPartitionMetadata("orders-0").RecoveryReplicas)
	}
	require.NoError(t, receiver.ReconcileReplicaMaterializations("receiver"))
	t.Logf("scheduler_passes=2 transferred_response_bytes=%d repaired_records=%d repaired_payload_bytes=%d repaired_response_bytes=%d automatic_quarantine=true", fetcher.bytes, len(batch.Messages), len(batch.Messages[0].Payload), len(encoded))
}

// TestCompactedISRQuarantineRejectsStaleAuthority ensures membership never changes
// on a stale request, invalid response, canceled owner or changed local content.
func TestCompactedISRQuarantineRejectsStaleAuthority(t *testing.T) {
	for _, mode := range []string{"digest", "leader", "lifecycle", "membership", "generation", "missing_authority", "canceled", "receiver_is_leader", "legacy_source", "legacy_receiver"} {
		t.Run(mode, func(t *testing.T) {
			h, receiver, p, d := compactedQuarantineFixture(t, false)
			_, source, _, _ := compactedQuarantineFixture(t, true)
			if mode == "receiver_is_leader" {
				for _, state := range []*fsm.BrokerFSM{receiver, source} {
					meta := state.GetPartitionMetadata("orders-0")
					meta.Leader = "receiver"
					meta.LeaderEpoch++
					applyPartitionMetadata(t, state, "orders", 0, *meta)
				}
			}

			if mode == "legacy_source" || mode == "legacy_receiver" {
				id := "source"
				if mode == "legacy_receiver" {
					id = "receiver"
				}
				info := receiver.GetBroker(id)
				info.RecoveryProtocol = 1
				data, err := json.Marshal(info)
				require.NoError(t, err)
				require.Nil(t, receiver.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
			}
			request := localRecoveryRequest(t, receiver, "receiver", "orders", 0)
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			manager := &quarantineCatchupRaft{MockRaftManagerForForward: &MockRaftManagerForForward{state: receiver, isLeader: true}, source: source}
			h.Cluster.RaftManager = manager
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "digest":
				batch.Digest = "bad"
			case "lifecycle":
				request.LifecycleEpoch++
				batch.LifecycleEpoch++
				batch, err = fsm.SealReplicaCatchupBatch(batch)
				require.NoError(t, err)
			case "leader", "membership":
				meta := receiver.GetPartitionMetadata("orders-0")
				if mode == "leader" {
					meta.LeaderEpoch++
				} else {
					meta.ISR = []string{"receiver"}
				}
				applyPartitionMetadata(t, receiver, "orders", 0, *meta)
			case "generation":
				require.NoError(t, d.TruncateTo(0))
				require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 0, Key: "anchor", Payload: "changed"}}))
			case "missing_authority":
				encoded, err := json.Marshal(request)
				require.NoError(t, err)
				var decoded fsm.ReplicaCatchupRequest
				require.NoError(t, json.Unmarshal(encoded, &decoded))
				decoded.LocalWitness = request.LocalWitness
				request = decoded
			case "canceled":
				cancel()
			}
			witness, err := h.ApplyReplicaCatchupVerified(ctx, request, batch)
			require.Error(t, err)
			require.Nil(t, witness)
			require.Zero(t, manager.quarantines)
			require.Contains(t, receiver.GetPartitionMetadata("orders-0").ISR, "receiver")
			require.Equal(t, uint64(1), p.NextOffset())
		})
	}
}

// A failed submission must not apply the old batch or prevent a fresh attempt.
func TestCompactedISRQuarantineFailedSubmission(t *testing.T) {
	h, receiver, p, _ := compactedQuarantineFixture(t, false)
	_, source, _, _ := compactedQuarantineFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := &quarantineCatchupRaft{MockRaftManagerForForward: &MockRaftManagerForForward{state: receiver, isLeader: true}, source: source}
	manager.reject = func() error { cancel(); return errors.New("submission result unavailable") }
	h.Cluster.RaftManager = manager
	request := localRecoveryRequest(t, receiver, "receiver", "orders", 0)
	batch, err := source.FetchReplicaCatchup(request)
	require.NoError(t, err)
	witness, err := h.ApplyReplicaCatchupVerified(ctx, request, batch)
	require.Error(t, err)
	require.Nil(t, witness)
	require.Equal(t, 1, manager.quarantines)
	require.Contains(t, receiver.GetPartitionMetadata("orders-0").ISR, "receiver")
	require.Equal(t, uint64(1), p.NextOffset())
	require.NoError(t, p.RecoveryError())
	manager.reject = nil
	_, err = h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
	require.ErrorIs(t, err, ErrReplicaCatchupQuarantined)
	require.Equal(t, uint64(1), p.NextOffset())
}

func TestDivergentISRCatchupQuarantinesBeforeTruncation(t *testing.T) {
	h, receiver, p, d := compactedQuarantineFixture(t, false)
	_, source, sourcePartition, _ := compactedQuarantineFixture(t, false)
	require.NoError(t, sourcePartition.ReplicaAppend([]types.Message{{Offset: 1, Payload: "source one"}, {Offset: 2, Payload: "source two"}}))
	require.NoError(t, sourcePartition.ApplyReplicaHWM(3))
	sourcePartition.FlushDisk()
	require.NoError(t, d.TruncateTo(0))
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 0, Key: "anchor", Payload: "divergent"}}))
	manager := &quarantineCatchupRaft{MockRaftManagerForForward: &MockRaftManagerForForward{state: receiver, isLeader: true}, source: source}
	h.Cluster.RaftManager = manager
	request := localRecoveryRequest(t, receiver, "receiver", "orders", 0)
	batch, err := source.FetchReplicaCatchup(request)
	require.NoError(t, err)
	require.NotNil(t, batch.TruncateTo)
	witness, err := h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
	require.ErrorIs(t, err, ErrReplicaCatchupQuarantined)
	require.Nil(t, witness)
	require.Equal(t, uint64(1), p.NextOffset())
	records, err := d.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, "divergent", records[0].Payload)
	require.NotContains(t, receiver.GetPartitionMetadata("orders-0").ISR, "receiver")
	request = localRecoveryRequest(t, receiver, "receiver", "orders", 0)
	require.Equal(t, fsm.BootstrapRecoveryProtocolVersion, request.RecoveryProtocol)
	batch, err = source.FetchReplicaCatchup(request)
	require.NoError(t, err)
	require.True(t, batch.BootstrapRequired)
	// The bootstrap protocol now owns replacement; the legacy batch never did.
	require.Equal(t, 1, manager.quarantines)
}
