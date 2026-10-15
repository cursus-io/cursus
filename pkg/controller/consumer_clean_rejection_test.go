package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type cleanRejectRepairStorage struct {
	*disk.DiskHandler
	reject *atomic.Bool
}

func TestConsumerRepairSchedulerWaitsForFullReplayBeforeISRAdmission(t *testing.T) {
	h, _, _, _ := consumerRollbackFixture(t)
	state, c := h.Cluster.RaftManager.GetFSM(), h.Coordinator
	h.Cluster = clusterController.NewClusterController(context.Background(), h.Config, h.Cluster.RaftManager, nil, "local", "127.0.0.1:7000")
	registerRoutingBroker(t, state, "source")
	generations := make(map[int]uint64)
	for id := 0; id < 2; id++ {
		metadata := state.GetPartitionMetadata(fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, id))
		metadata.Replicas, metadata.ISR = []string{"local", "source"}, []string{"source"}
		applyPartitionMetadata(t, state, config.ConsumerOffsetsTopicName, id, *metadata)
		release, err := c.BeginConsumerMetadataRecovery(context.Background())
		require.NoError(t, err)
		generations[id], err = c.BeginConsumerLogRepair(id)
		require.NoError(t, err)
		c.FinishConsumerLogRepair(generations[id], nil)
		release()
	}
	submitted := 0
	h.Cluster.SetReplicaCatchupProofSubmitter(func(_ context.Context, proof fsm.ISRCatchupProof) error {
		submitted++
		encoded, err := json.Marshal(proof)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("ISR_CATCHUP:"), encoded...), Index: uint64(100 + submitted)}))
		return nil
	})
	err := h.Cluster.RunReplicaCatchupOnceVerified(context.Background(), &witnessSourceFetcher{source: state}, h.ApplyReplicaCatchupVerified)
	require.Zero(t, submitted, "verified logs must remain outside ISR until every pending overlay repair is replayed")
	require.ErrorContains(t, err, "replica proof eligibility changed")
	require.Empty(t, state.BuildISRCatchupProofs("local"), "heartbeat uses the same pending-repair gate")
	for id, generation := range generations {
		p, err := h.TopicManager.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(id)
		require.NoError(t, err)
		metadata := state.GetPartitionMetadata(fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, id))
		require.NoError(t, p.ValidateReplicaCatchupEligibility(metadata.LeaderEpoch, metadata.LifecycleEpoch, 0, generation), "scheduler must still accept actual terminal verification for every pending partition")
	}
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	require.Len(t, state.BuildISRCatchupProofs("local"), 2)
	require.NoError(t, h.Cluster.RunReplicaCatchupOnceVerified(context.Background(), &witnessSourceFetcher{source: state}, h.ApplyReplicaCatchupVerified))
	require.Equal(t, 2, submitted)
	metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	message := consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "later", Topic: "orders", Epoch: 1, PartitionCount: 1}, 0)
	message.LeaderEpoch = int64(metadata.LeaderEpoch)
	payload, err := json.Marshal(types.MessageCommand{Topic: config.ConsumerOffsetsTopicName, Partition: 0, LeaderID: metadata.Leader, LeaderEpoch: metadata.LeaderEpoch, Messages: []types.Message{message}})
	require.NoError(t, err)
	internalContext := internalReplicationTestContext(h)
	require.Equal(t, "OK leo=1 hwm=0", h.HandleCommand("REPLICATE_MESSAGE "+h.internalAuthPrefix()+"payload="+string(payload), internalContext))
	for id := range generations {
		require.Zero(t, c.PendingConsumerLogRepair(id))
	}
	require.NoError(t, c.ConsumerOperationsReadinessError(), "normal append after admission must not strand an old repair witness")
}

func (s *cleanRejectRepairStorage) TruncateToIfGenerationContext(ctx context.Context, offset uint64, generation disk.ContentGeneration) error {
	if s.reject.Load() {
		return errors.Join(disk.ErrTruncateNotApplied, errors.New("injected clean rejection after repair admission"))
	}
	return s.DiskHandler.TruncateToIfGenerationContext(ctx, offset, generation)
}

type cleanRejectRepairProvider struct {
	*disk.DiskManager
	reject *atomic.Bool
}

func (p *cleanRejectRepairProvider) wrap(name string, id int, handler types.StorageHandler, err error) (types.StorageHandler, error) {
	if err != nil {
		return nil, err
	}
	if name == config.ConsumerOffsetsTopicName && id == 0 {
		return &cleanRejectRepairStorage{handler.(*disk.DiskHandler), p.reject}, nil
	}
	return handler, nil
}
func (p *cleanRejectRepairProvider) GetHandler(name string, id int) (types.StorageHandler, error) {
	handler, err := p.DiskManager.GetHandler(name, id)
	return p.wrap(name, id, handler, err)
}
func (p *cleanRejectRepairProvider) GetHandlerWithPolicy(name string, id int, policy string, hours int, bytes int64) (types.StorageHandler, error) {
	handler, err := p.DiskManager.GetHandlerWithPolicy(name, id, policy, hours, bytes)
	return p.wrap(name, id, handler, err)
}

func TestConsumerRepairCleanDiskRejectionRequiresCanonicalVerificationBeforeReplay(t *testing.T) {
	for _, markerPending := range []bool{false, true} {
		name := "without_marker"
		if markerPending {
			name = "same_epoch_marker"
		}
		t.Run(name, func(t *testing.T) {
			var reject atomic.Bool
			reject.Store(true)
			h, p, _, d := consumerRollbackFixtureWithProvider(t, func(dm *disk.DiskManager) topic.HandlerProvider { return &cleanRejectRepairProvider{dm, &reject} })
			sourceHandler, sourcePartition, _, _ := consumerRollbackFixture(t)
			state, c := h.Cluster.RaftManager.GetFSM(), h.Coordinator
			source := sourceHandler.Cluster.RaftManager.GetFSM()
			for _, current := range []*fsm.BrokerFSM{state, source} {
				registerRoutingBroker(t, current, "source")
				metadata := current.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
				metadata.Replicas, metadata.ISR, metadata.CommittedHWM = []string{"local", "source"}, []string{"source"}, 2
				applyPartitionMetadata(t, current, config.ConsumerOffsetsTopicName, 0, *metadata)
			}
			c.SetRoutedOffsetRecordWriter(func(record coordinator.ConsumerMetadataRecord) error {
				c.RecordConsumerMetadataSource(record.Group, 0)
				message := consumerMetadataMessage(t, record, p.NextOffset())
				if err := p.ReplicaAppend([]types.Message{message}); err != nil {
					return err
				}
				return p.ApplyReplicaHWM(message.Offset + 1)
			})
			require.NoError(t, c.RegisterGroup("orders", "common", 1))
			require.NoError(t, c.RegisterGroup("orders", "divergent", 1))
			canonical, err := p.ReadMessages(0, 2)
			require.NoError(t, err)
			canonical[1] = consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "canonical", Topic: "orders", Epoch: 1, PartitionCount: 1}, 1)
			require.NoError(t, sourcePartition.ReplicaAppend(canonical))
			require.NoError(t, sourcePartition.ApplyReplicaHWM(2))
			if markerPending {
				p.MarkReconciliationPending(errors.New("same-epoch repair pending"))
			}
			generation, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			request := localRecoveryRequest(t, state, "local", config.ConsumerOffsetsTopicName, 0)
			batch, err := source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			require.NotNil(t, batch.TruncateTo)
			_, err = h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
			require.ErrorIs(t, err, disk.ErrTruncateNotApplied)
			require.NoError(t, d.CheckContentGeneration(generation), "clean rejection preserves the original disk generation")
			require.Equal(t, uint64(2), p.NextOffset())
			require.Equal(t, uint64(2), p.GetHWM())
			require.Positive(t, c.PendingConsumerLogRepair(0))
			require.Error(t, h.RecoverDistributedConsumerMetadata(context.Background()))
			require.Contains(t, c.ExportState(), "divergent")
			require.Error(t, c.ConsumerOperationsReadinessError())
			reject.Store(false)
			request = localRecoveryRequest(t, state, "local", config.ConsumerOffsetsTopicName, 0)
			batch, err = source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			_, err = h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
			require.NoError(t, err)
			request = localRecoveryRequest(t, state, "local", config.ConsumerOffsetsTopicName, 0)
			batch, err = source.FetchReplicaCatchup(request)
			require.NoError(t, err)
			require.True(t, batch.Verified)
			witness, err := h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
			require.NoError(t, err)
			require.NoError(t, fsm.AdvanceReplicaCatchupRequest(&request, batch))
			require.NoError(t, state.MarkReplicaCatchupVerified(request, witness))
			require.NoError(t, p.ReconciliationError())
			require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
			require.NotContains(t, c.ExportState(), "divergent")
			require.Contains(t, c.ExportState(), "canonical")
			require.Zero(t, c.PendingConsumerLogRepair(0))
			require.NoError(t, c.ConsumerOperationsReadinessError())
		})
	}
}
