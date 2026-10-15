package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func consumerRollbackFixture(t *testing.T) (*CommandHandler, *topic.Partition, uint64, *disk.DiskHandler) {
	return consumerRollbackFixtureWithProvider(t, nil)
}
func consumerRollbackFixtureWithProvider(t *testing.T, makeProvider func(*disk.DiskManager) topic.HandlerProvider) (*CommandHandler, *topic.Partition, uint64, *disk.DiskHandler) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	var provider topic.HandlerProvider = dm
	if makeProvider != nil {
		provider = makeProvider(dm)
	}
	tm := topic.NewTopicManager(cfg, provider, nil)
	c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, &dummyPublisher{})
	require.NoError(t, err)
	state := fsm.NewBrokerFSM(tm, c)
	registerRoutingBroker(t, state, "local")
	installRoutingOffsetsTopology(t, state, "local")
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager, h.Config = tm, cfg
	t.Cleanup(func() { _ = h.Close(); c.Stop(); tm.Stop(); dm.CloseAllHandlers() })
	local := tm.GetTopic(config.ConsumerOffsetsTopicName)
	p, err := local.GetPartition(0)
	require.NoError(t, err)
	d, err := dm.GetHandler(config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	return h, p, local.Definition().LifecycleEpoch, d.(*disk.DiskHandler)
}

func TestConsumerReplicaRollbackStrictReplayDiscardsDeletedOverlay(t *testing.T) {
	for _, owned := range []bool{true, false} {
		t.Run(fmt.Sprintf("owned_%v", owned), func(t *testing.T) {
			h, p, lifecycle, _ := consumerRollbackFixture(t)
			c, state := h.Coordinator, h.Cluster.RaftManager.GetFSM()
			registerRoutingBroker(t, state, "source")
			for i := 0; i < 4; i++ {
				m := state.GetPartitionMetadata(fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, i))
				m.Replicas, m.ISR = []string{"local", "source"}, []string{"source"}
				if !owned {
					m.Leader = "source"
					m.LeaderEpoch++
				}
				if i == 0 {
					m.CommittedHWM = 4
				}
				applyPartitionMetadata(t, state, config.ConsumerOffsetsTopicName, i, *m)
			}
			for _, name := range []string{"raft-only", "merged"} {
				require.NoError(t, c.RegisterGroupFromFSM("orders", name, 1))
				require.NoError(t, c.ApplyOffsetUpdateFromFSM(name, "orders", []coordinator.OffsetItem{{Partition: 0, Offset: 5}}))
			}
			c.SetRoutedOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
				c.RecordConsumerMetadataSource(record.Group, 0)
				message := consumerMetadataMessage(t, record, p.NextOffset())
				if err := p.ReplicaAppend([]types.Message{message}); err != nil {
					return err
				}
				return p.ApplyReplicaHWM(message.Offset + 1)
			})
			require.NoError(t, c.RegisterGroup("orders", "retained", 1))
			require.NoError(t, c.CommitOffset("merged", "orders", 0, 100))
			require.NoError(t, c.CommitOffset("merged", "orders", 0, 101))
			require.NoError(t, c.RegisterGroup("orders", "divergent", 1))
			require.Equal(t, uint64(4), p.NextOffset())
			live, baseline, err := c.ExportStateWithRecovery()
			require.NoError(t, err)
			require.True(t, baseline.KnownComplete)
			require.Equal(t, uint64(101), live["merged"].Offsets["orders"][0])
			require.Equal(t, uint64(5), baseline.RaftCompatibilityState["merged"].Offsets["orders"][0])
			require.NotContains(t, baseline.RaftCompatibilityState, "divergent")
			require.NoError(t, h.CaptureGroupRecoveryOwnership())
			if !owned {
				require.Empty(t, h.groupRecoveryEpoch)
			}
			m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
			boundary := uint64(1)
			batch := fsm.ReplicaCatchupBatch{Topic: config.ConsumerOffsetsTopicName, Partition: 0, BrokerID: "local", SourceBroker: "source", Leader: m.Leader, LeaderEpoch: m.LeaderEpoch, LifecycleEpoch: lifecycle, CommittedHWM: 4, StartOffset: 4, EndOffset: 4, TruncateTo: &boundary}
			sealed, err := fsm.SealReplicaCatchupBatch(batch)
			require.NoError(t, err)
			require.NoError(t, h.ApplyReplicaCatchup(context.Background(), sealed))
			generation := c.ConsumerRecoveryRequirement().RequiredGeneration
			require.True(t, c.ConsumerRecoveryRequirement().Pending())
			require.False(t, c.ConsumerRecoveryRequirement().Installing)
			require.NoError(t, c.ConsumerRecoveryRequirement().Failure)
			require.Error(t, h.refreshLocalGroupRecovery(), "incomplete local coverage cannot finish replay")
			require.Contains(t, c.ExportState(), "divergent")
			release, err := c.BeginConsumerMetadataRecovery(context.Background())
			require.NoError(t, err)
			release()
			require.ErrorIs(t, c.ConsumerOperationsReadinessError(), coordinator.ErrConsumerMetadataRecovering)
			batch.TruncateTo, batch.StartOffset, batch.Verified = nil, 1, true
			batch.Messages = []types.Message{
				consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: "merged", Topic: "orders", Epoch: 1, Revision: 1, Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: 7}}}, 1),
				consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "canonical", Topic: "orders", Epoch: 1, PartitionCount: 1}, 2),
				consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: "canonical", Topic: "orders", Epoch: 1, Revision: 1, Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: 9}}}, 3),
			}
			sealed, err = fsm.SealReplicaCatchupBatch(batch)
			require.NoError(t, err)
			request := localRecoveryRequest(t, state, "local", config.ConsumerOffsetsTopicName, 0)
			witness, err := h.ApplyReplicaCatchupVerified(context.Background(), request, sealed)
			require.NoError(t, err, "infrastructure catch-up continues while consumers are closed")
			require.NotNil(t, witness)
			require.NoError(t, fsm.AdvanceReplicaCatchupRequest(&request, sealed))
			require.NoError(t, state.MarkReplicaCatchupVerified(request, witness))
			require.Equal(t, generation, c.ConsumerRecoveryRequirement().RequiredGeneration)
			require.ErrorIs(t, c.ConsumerOperationsReadinessError(), coordinator.ErrConsumerMetadataRecovering)
			require.NoError(t, h.refreshLocalGroupRecovery())
			require.NoError(t, c.ConsumerOperationsReadinessError())
			live = c.ExportState()
			require.NotContains(t, live, "divergent")
			require.Equal(t, uint64(7), live["merged"].Offsets["orders"][0])
			require.Equal(t, uint64(1), live["merged"].OffsetRevisions["orders"])
			require.Equal(t, uint64(5), live["raft-only"].Offsets["orders"][0])
			require.Equal(t, uint64(9), live["canonical"].Offsets["orders"][0])
			current := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
			require.Equal(t, m.Leader, current.Leader)
			require.Equal(t, m.LeaderEpoch, current.LeaderEpoch)
		})
	}
}

func TestConsumerReplicaRollbackUnknownAuthorityRejectsBeforeMutation(t *testing.T) {
	h, p, lifecycle, _ := consumerRollbackFixture(t)
	state := h.Cluster.RaftManager.GetFSM()
	registerRoutingBroker(t, state, "source")
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	m.Replicas, m.ISR, m.CommittedHWM = []string{"local", "source"}, []string{"source"}, 2
	applyPartitionMetadata(t, state, config.ConsumerOffsetsTopicName, 0, *m)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Payload: "common"}, {Offset: 1, Payload: "divergent"}}))
	require.NoError(t, p.ApplyReplicaHWM(2))
	require.NoError(t, h.Coordinator.ImportStateWithRecovery(nil, nil))
	before := h.Coordinator.ConsumerRecoveryRequirement().RequiredGeneration
	boundary := uint64(1)
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{Topic: config.ConsumerOffsetsTopicName, Partition: 0, BrokerID: "local", SourceBroker: "source", Leader: m.Leader, LeaderEpoch: m.LeaderEpoch, LifecycleEpoch: lifecycle, CommittedHWM: 2, StartOffset: 2, EndOffset: 2, TruncateTo: &boundary})
	require.NoError(t, err)
	require.ErrorIs(t, h.ApplyReplicaCatchup(context.Background(), batch), coordinator.ErrConsumerMetadataBootstrapUnknown)
	require.Equal(t, uint64(2), p.NextOffset())
	require.Equal(t, uint64(2), p.GetHWM())
	require.Equal(t, before, h.Coordinator.ConsumerRecoveryRequirement().RequiredGeneration)
}

func TestConsumerAppendOnlyCatchupDoesNotAcquireAdmissionLease(t *testing.T) {
	h, p, lifecycle, _ := consumerRollbackFixture(t)
	state := h.Cluster.RaftManager.GetFSM()
	registerRoutingBroker(t, state, "source")
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	m.Replicas, m.ISR, m.CommittedHWM = []string{"local", "source"}, []string{"source"}, 1
	applyPartitionMetadata(t, state, config.ConsumerOffsetsTopicName, 0, *m)
	before := h.Coordinator.ConsumerRecoveryRequirement().RequiredGeneration
	release, err := h.Coordinator.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	defer release()
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{Topic: config.ConsumerOffsetsTopicName, Partition: 0, BrokerID: "local", SourceBroker: "source", Leader: m.Leader, LeaderEpoch: m.LeaderEpoch, LifecycleEpoch: lifecycle, CommittedHWM: 1, StartOffset: 0, EndOffset: 1, Verified: true, Messages: []types.Message{{Offset: 0, Payload: "canonical"}}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, h.ApplyReplicaCatchup(ctx, batch), "append cannot drain the already-held admission lease")
	require.Equal(t, uint64(1), p.GetHWM())
	require.Equal(t, before, h.Coordinator.ConsumerRecoveryRequirement().RequiredGeneration)
}

func TestConsumerReplicaRollbackFailureDoesNotReopenOnLaterAttempt(t *testing.T) {
	h, p, lifecycle, d := consumerRollbackFixture(t)
	state := h.Cluster.RaftManager.GetFSM()
	registerRoutingBroker(t, state, "source")
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	m.Replicas, m.ISR, m.CommittedHWM = []string{"local", "source"}, []string{"source"}, 2
	applyPartitionMetadata(t, state, config.ConsumerOffsetsTopicName, 0, *m)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Payload: "common"}, {Offset: 1, Payload: "divergent"}}))
	require.NoError(t, p.ApplyReplicaHWM(2))
	path := filepath.Join(filepath.Dir(d.GetSegmentPath(0)), "partition_0.producers.tmp")
	require.NoError(t, os.Mkdir(path, 0700))
	boundary := uint64(1)
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{Topic: config.ConsumerOffsetsTopicName, Partition: 0, BrokerID: "local", SourceBroker: "source", Leader: m.Leader, LeaderEpoch: m.LeaderEpoch, LifecycleEpoch: lifecycle, CommittedHWM: 2, StartOffset: 2, EndOffset: 2, TruncateTo: &boundary})
	require.NoError(t, err)
	require.ErrorIs(t, h.ApplyReplicaCatchup(context.Background(), batch), topic.ErrReplicaRollbackFailed)
	require.Equal(t, uint64(1), p.NextOffset())
	require.Equal(t, uint64(1), p.GetHWM())
	require.ErrorIs(t, h.Coordinator.ConsumerRecoveryRequirement().Failure, topic.ErrReplicaRollbackFailed)
	require.Error(t, h.refreshLocalGroupRecovery())
	release, err := h.Coordinator.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	_, err = h.Coordinator.BeginConsumerLogRepair(0)
	require.Error(t, err)
	release()
	require.Error(t, h.ApplyReplicaCatchup(context.Background(), batch))
	require.ErrorIs(t, h.Coordinator.ConsumerOperationsReadinessError(), coordinator.ErrConsumerMetadataRecovering)
	require.ErrorIs(t, h.Coordinator.ConsumerRecoveryRequirement().Failure, topic.ErrReplicaRollbackFailed)
}

func TestConsumerReplicaRollbackPreservedReconciliationDoesNotPoisonGeneration(t *testing.T) {
	h, p, lifecycle, _ := consumerRollbackFixture(t)
	state := h.Cluster.RaftManager.GetFSM()
	registerRoutingBroker(t, state, "source")
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	m.Replicas, m.ISR, m.CommittedHWM = []string{"local", "source"}, []string{"source"}, 2
	applyPartitionMetadata(t, state, config.ConsumerOffsetsTopicName, 0, *m)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Payload: "common"}, {Offset: 1, Payload: "divergent"}}))
	require.NoError(t, p.ApplyReplicaHWM(2))
	failure := errors.New("independent reconciliation pending")
	p.MarkReconciliationPending(failure)
	boundary := uint64(1)
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{Topic: config.ConsumerOffsetsTopicName, Partition: 0, BrokerID: "local", SourceBroker: "source", Leader: m.Leader, LeaderEpoch: m.LeaderEpoch, LifecycleEpoch: lifecycle, CommittedHWM: 2, StartOffset: 2, EndOffset: 2, TruncateTo: &boundary})
	require.NoError(t, err)
	require.NoError(t, h.ApplyReplicaCatchup(context.Background(), batch))
	require.ErrorIs(t, p.RecoveryError(), failure)
	require.NoError(t, h.Coordinator.ConsumerRecoveryRequirement().Failure)
	require.True(t, h.Coordinator.ConsumerRecoveryRequirement().Pending())
	require.Error(t, h.refreshLocalGroupRecovery())
}

func TestConsumerReplicaRollbackInvalidatesUnchangedOwnership(t *testing.T) {
	h, p, lifecycle, _ := consumerRollbackFixture(t)
	state := h.Cluster.RaftManager.GetFSM()
	registerRoutingBroker(t, state, "source")
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	m.Replicas = []string{"local", "source"}
	m.ISR = []string{"source"}
	m.CommittedHWM = 2
	applyPartitionMetadata(t, state, config.ConsumerOffsetsTopicName, 0, *m)
	p.SetHWM(0)
	require.NoError(t, p.EnqueueBatchLeader([]types.Message{{Payload: "common"}, {Payload: "divergent"}}))
	require.NoError(t, p.ApplyReplicaHWM(2))
	require.NoError(t, h.CaptureGroupRecoveryOwnership())
	before := h.Coordinator.ConsumerRecoveryRequirement().RequiredGeneration
	boundary := uint64(1)
	batch, err := fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{Topic: config.ConsumerOffsetsTopicName, Partition: 0, BrokerID: "local", SourceBroker: "source", Leader: m.Leader, LeaderEpoch: m.LeaderEpoch, LifecycleEpoch: lifecycle, CommittedHWM: 2, StartOffset: 2, EndOffset: 2, TruncateTo: &boundary})
	require.NoError(t, err)
	require.NoError(t, h.ApplyReplicaCatchup(context.Background(), batch))
	require.Greater(t, h.Coordinator.ConsumerRecoveryRequirement().RequiredGeneration, before)
	require.ErrorIs(t, h.Coordinator.ConsumerOperationsReadinessError(), coordinator.ErrConsumerMetadataRecovering)
	require.Error(t, h.refreshLocalGroupRecovery())
	release, err := h.Coordinator.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	release()
	require.ErrorIs(t, h.Coordinator.ConsumerOperationsReadinessError(), coordinator.ErrConsumerMetadataRecovering)
}
