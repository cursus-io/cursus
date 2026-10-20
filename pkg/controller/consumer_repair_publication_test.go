package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type rewritingRepairReader struct {
	*consumerRecoveryReader
	beforePublish func()
}
type rewritingRepairPublication struct {
	coordinator.ConsumerLogRepairPublication
	before func()
}

func (p *rewritingRepairPublication) Publish(fn func()) error {
	p.before()
	// Keep the original tokens after rewriting. A background flush can delay
	// publication, but must never let the stale generation publish state.
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := p.ConsumerLogRepairPublication.Publish(fn)
		if !errors.Is(err, disk.ErrContentPublicationBusy) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
}
func (r *rewritingRepairReader) AcquireConsumerLogRepairPublication(repairs map[int]uint64) (coordinator.ConsumerLogRepairPublication, error) {
	publication, err := r.consumerRecoveryReader.AcquireConsumerLogRepairPublication(repairs)
	if err != nil {
		return nil, err
	}
	return &rewritingRepairPublication{publication, r.beforePublish}, nil
}

func TestConsumerRepairRawRewriteAtFinalPublicationCannotInstallOrReopen(t *testing.T) {
	h, p, _, d := consumerRollbackFixture(t)
	state, c := h.Cluster.RaftManager.GetFSM(), h.Coordinator
	registerRoutingBroker(t, state, "source")
	metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	metadata.Replicas, metadata.ISR = []string{"local", "source"}, []string{"source"}
	metadata.CommittedHWM = 1
	applyPartitionMetadata(t, state, config.ConsumerOffsetsTopicName, 0, *metadata)
	canonical := consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "canonical", Topic: "orders", Epoch: 1, PartitionCount: 1}, 0)
	require.NoError(t, p.ReplicaAppend([]types.Message{canonical}))
	require.NoError(t, p.ApplyReplicaHWM(1))
	release, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	generation, err := c.BeginConsumerLogRepair(0)
	require.NoError(t, err)
	c.FinishConsumerLogRepair(generation, nil)
	release()
	require.Error(t, h.RecoverDistributedConsumerMetadata(context.Background()), "equal empty boundaries cannot discharge unverified repair")
	request := localRecoveryRequest(t, state, "local", config.ConsumerOffsetsTopicName, 0)
	batch, err := state.FetchReplicaCatchup(request)
	require.NoError(t, err)
	witness, err := h.ApplyReplicaCatchupVerified(context.Background(), request, batch)
	require.NoError(t, err)
	require.NoError(t, state.MarkReplicaCatchupVerified(request, witness))
	require.NoError(t, p.ValidateReplicaCatchupEligibility(request.LeaderEpoch, request.LifecycleEpoch, 1, generation))
	snapshot, err := state.ConsumerMetadataRecoverySnapshot()
	require.NoError(t, err)
	plan, err := h.prepareConsumerRecoveryPlan(context.Background(), snapshot, -1, false)
	require.NoError(t, err)
	defer plan.close()
	release, err = c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	defer release()
	before := c.ExportState()
	err = state.WithConsumerMetadataRecoveryFence(func(_ []fsm.PartitionMetadata) error {
		return h.withConsumerRecoveryPlan(context.Background(), plan, -1, nil, func(reader *consumerRecoveryReader) error {
			racing := &rewritingRepairReader{reader, func() {
				replacement := consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "divergent", Topic: "orders", Epoch: 1, PartitionCount: 1}, 0)
				require.NoError(t, d.TruncateTo(0))
				require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: config.ConsumerOffsetsTopicName, Offset: 0, Payload: replacement.Payload}}))
			}}
			prepared, err := c.PrepareFullConsumerMetadataReplay(racing)
			if err != nil {
				return err
			}
			return c.ApplyFullConsumerMetadataReplayAndComplete(prepared)
		})
	})
	require.ErrorIs(t, err, disk.ErrStaleContentGeneration)
	require.Equal(t, before, c.ExportState())
	require.Equal(t, generation, c.PendingConsumerLogRepair(0))
	require.Error(t, c.CompleteConsumerRecovery(generation))
	release()
	require.Error(t, c.ConsumerOperationsReadinessError())
}
