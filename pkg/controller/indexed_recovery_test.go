package controller

import (
	"context"
	"encoding/json"
	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func indexedApplyFixture(t *testing.T) (*CommandHandler, *fsm.BrokerFSM, *topic.Partition, fsm.ReplicaCatchupBatch) {
	return indexedApplyFixtureWithProvider(t, nil)
}

func indexedApplyFixtureWithProvider(t *testing.T, wrap func(*disk.DiskManager) topic.HandlerProvider) (*CommandHandler, *fsm.BrokerFSM, *topic.Partition, fsm.ReplicaCatchupBatch) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	dm := disk.NewDiskManager(cfg)
	var provider topic.HandlerProvider = dm
	if wrap != nil {
		provider = wrap(dm)
	}
	tm := topic.NewTopicManager(cfg, provider, nil)
	require.NoError(t, tm.CreateTopic("orders", 1, false, false))
	state := fsm.NewBrokerFSM(tm, nil)
	for _, id := range []string{"broker-1", "broker-2"} {
		data, err := json.Marshal(fsm.BrokerInfo{ID: id, Addr: id + ":9001", Status: "active", RecoveryProtocol: fsm.IndexedRecoveryProtocolVersion})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
	}
	applyPartitionMetadata(t, state, "orders", 0, fsm.PartitionMetadata{Leader: "broker-1", LeaderEpoch: 8, CommittedHWM: 3, CommittedHWMKnown: true, Replicas: []string{"broker-1", "broker-2"}, ISR: []string{"broker-1"}, PartitionCount: 1})
	cluster := clusterController.NewClusterController(context.Background(), cfg, &MockRaftManagerForForward{state: state}, nil, "broker-2", "broker-2:9001")
	handler := NewCommandHandler(tm, cfg, nil, nil, cluster)
	t.Cleanup(func() { _ = handler.Close(); tm.Stop(); dm.CloseAllHandlers() })
	partition, err := tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	partition.SetHWM(0)
	require.NoError(t, partition.EnqueueBatchLeader([]types.Message{{Payload: "common"}, {Payload: "divergent"}}))
	require.NoError(t, partition.ApplyReplicaHWM(2))
	proofs, err := partition.ReplicaLineageCandidates(1)
	require.NoError(t, err)
	to := uint64(1)
	batch := fsm.ReplicaCatchupBatch{Topic: "orders", BrokerID: "broker-2", Leader: "broker-1", SourceBroker: "broker-1", LeaderEpoch: 8, LifecycleEpoch: topic.InitialLifecycleEpoch, StartOffset: 2, EndOffset: 2, CommittedHWM: 3, RecoveryProtocol: 1, MatchedPrefix: &proofs[0], TruncateTo: &to}
	batch, err = fsm.SealReplicaCatchupBatch(batch)
	require.NoError(t, err)
	return handler, state, partition, batch
}

func TestApplyIndexedRecoveryValidatesDurablePrefixAndCurrentFences(t *testing.T) {
	for _, name := range []string{"valid", "altered prefix", "rejoined ISR", "inactive source", "stale leader", "stale lifecycle", "checksum"} {
		t.Run(name, func(t *testing.T) {
			handler, state, p, batch := indexedApplyFixture(t)
			switch name {
			case "altered prefix":
				prefix := *batch.MatchedPrefix
				prefix.Digest = strings.Repeat("b", 64)
				batch.MatchedPrefix = &prefix
			case "rejoined ISR":
				meta := state.GetPartitionMetadata("orders-0")
				meta.ISR = append(meta.ISR, "broker-2")
				applyPartitionMetadata(t, state, "orders", 0, *meta)
			case "inactive source":
				data, err := json.Marshal(fsm.BrokerInfo{ID: "broker-1", Addr: "broker-1:9001", Status: "inactive", RecoveryProtocol: 1})
				require.NoError(t, err)
				require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
			case "stale leader":
				batch.LeaderEpoch++
			case "stale lifecycle":
				batch.LifecycleEpoch++
			}
			var err error
			batch, err = fsm.SealReplicaCatchupBatch(batch)
			require.NoError(t, err)
			if name == "checksum" {
				batch.Digest = "invalid"
			}
			err = handler.ApplyReplicaCatchup(context.Background(), batch)
			if name == "valid" {
				require.NoError(t, err)
				require.Equal(t, uint64(1), p.NextOffset())
				require.Equal(t, uint64(1), p.GetHWM())
			} else {
				require.Error(t, err)
				require.Equal(t, uint64(2), p.NextOffset())
			}
		})
	}
}

func TestApplyIndexedRecoveryVerifiedRejectsSparseRollbackBeforeMutation(t *testing.T) {
	handler, _, p, batch := indexedApplyFixture(t)
	proofs, err := p.ReplicaLineageCandidates(2)
	require.NoError(t, err)
	// Omit the actual common boundary at one, as a sparse set can do for a
	// longer tail. A matching anchor alone must not authorize verified apply.
	request := fsm.ReplicaCatchupRequest{Topic: batch.Topic, BrokerID: batch.BrokerID,
		Leader: batch.Leader, SourceBroker: batch.SourceBroker, LeaderEpoch: batch.LeaderEpoch,
		LifecycleEpoch: batch.LifecycleEpoch, NextOffset: 2, CommittedHWM: batch.CommittedHWM,
		RecoveryProtocol: 1, PrefixCandidates: []disk.LineagePrefix{proofs[0], proofs[len(proofs)-1]}}
	zero := uint64(0)
	batch.TruncateTo, batch.MatchedPrefix = &zero, &request.PrefixCandidates[1]
	batch, err = fsm.SealReplicaCatchupBatch(batch)
	require.NoError(t, err)
	_, err = handler.ApplyReplicaCatchupVerified(context.Background(), request, batch)
	require.ErrorContains(t, err, "exact common-prefix refinement")
	require.Equal(t, uint64(2), p.NextOffset())
	require.Equal(t, uint64(2), p.GetHWM())
}

func TestApplyIndexedRecoveryDoesNotHoldRaftFenceWhileWaitingForWriteGate(t *testing.T) {
	handler, state, _, batch := indexedApplyFixture(t)
	gate := handler.partitionWriteLock("orders", 0)
	gate.Lock()
	released := false
	defer func() {
		if !released {
			gate.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- handler.ApplyReplicaCatchup(context.Background(), batch) }()
	// Give recovery a chance to block on the gate; the writer owning it must
	// still be able to complete a Raft transition before releasing ownership.
	select {
	case err := <-done:
		t.Fatalf("recovery escaped held gate: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	transitioned := make(chan interface{}, 1)
	go func() {
		transitioned <- state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"observer","addr":"observer:9001","status":"active"}`)})
	}()
	select {
	case result := <-transitioned:
		require.Nil(t, result)
	case <-time.After(3 * time.Second):
		t.Fatal("recovery blocked Raft while waiting for controller write gate")
	}
	gate.Unlock()
	released = true
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("recovery did not finish")
	}
}

func TestApplyIndexedRecoveryRejectsUnverifiedFinalBatchBeforeAppend(t *testing.T) {
	handler, _, partition, batch := indexedApplyFixture(t)
	before, err := partition.ReadMessages(0, 10)
	require.NoError(t, err)
	proofs, err := partition.ReplicaLineageCandidates(2)
	require.NoError(t, err)
	batch.MatchedPrefix = &proofs[0]
	batch.TruncateTo = nil
	batch.EndOffset = batch.CommittedHWM
	batch.Messages = []types.Message{{Offset: 2, Payload: "must-not-append"}}
	batch.Verified = false
	batch, err = fsm.SealReplicaCatchupBatch(batch)
	require.NoError(t, err)
	require.ErrorContains(t, handler.ApplyReplicaCatchup(context.Background(), batch), "without prefix verification")
	require.Equal(t, uint64(2), partition.NextOffset())
	require.Equal(t, uint64(2), partition.GetHWM())
	after, err := partition.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
