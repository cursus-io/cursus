package controller

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
)

func TestConsumerRequirementBlocksCachedOwnershipAndSemantics(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, &coordinatorRoutingTopicHandler{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	state := fsm.NewBrokerFSM(nil, c)
	registerRoutingBroker(t, state, "node-1")
	installRoutingOffsetsTopology(t, state, "node-1")
	h := newCoordinatorRoutingHandler("node-1", state, c)
	require.NoError(t, h.CaptureGroupRecoveryOwnership())
	generation := c.BeginConsumerRestore()
	c.FinishConsumerRestore(generation, nil)
	require.ErrorIs(t, h.ensureGroupRecovery(0, 1), coordinator.ErrConsumerMetadataRecovering)
	require.False(t, h.IsGroupCoordinator("workers"))
	_, err = h.ResolveGroupCoordinators([]string{"workers"})
	require.ErrorIs(t, err, coordinator.ErrConsumerMetadataRecovering)
	require.Error(t, h.refreshLocalGroupRecovery())
	for _, command := range []string{"REGISTER_GROUP", "JOIN_GROUP", "SYNC_GROUP", "LEAVE_GROUP", "LIST_GROUPS", "GROUP_STATUS", "COMMIT", "BATCH_COMMIT", "FETCH_OFFSET", "HEARTBEAT", "FETCH", "INIT_PRODUCER_ID", "SEND_OFFSETS_TO_TXN", "END_TXN", "CONSUME", "STREAM"} {
		t.Run(command, func(t *testing.T) {
			require.Contains(t, h.HandleCommand(command+" topic=orders group=workers", NewClientContext("", 0)), "consumer_metadata_recovering")
		})
	}
	require.NotContains(t, h.HandleCommand("PING", NewClientContext("", 0)), "consumer_metadata_recovering")
	internal := NewClientContext("", 0)
	internal.Internal = true
	require.Contains(t, h.handlePublish("PUBLISH topic=__consumer_offsets message=x producerId=p", internal), "consumer_metadata_recovering")
}

func TestStartupDriverRequiresAuthoritativeFSMInsteadOfLocalHWM(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, &coordinatorRoutingTopicHandler{})
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	h := newCoordinatorRoutingHandler("node-1", fsm.NewBrokerFSM(nil, c), c)
	require.Error(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	require.True(t, c.ConsumerRecoveryRequirement().Pending())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release, err := c.BeginConsumerMetadataRecovery(ctx)
	require.NoError(t, err)
	release()
}

func TestStrictStartupAndUnownedRestoreDiscardSnapshotOverlay(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	tm := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
	t.Cleanup(tm.Stop)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	state := fsm.NewBrokerFSM(tm, c)
	registerRoutingBroker(t, state, "remote")
	installRoutingOffsetsTopology(t, state, "remote")
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager = tm
	proof := &coordinator.ConsumerRecoveryEnvelope{Version: 1, KnownComplete: true, RaftCompatibilityKnown: true, SourcePartitionsKnown: true, RaftCompatibilityState: map[string]*coordinator.GroupStateSnapshot{}, SourcePartitions: map[string][]int{}}
	stale := map[string]*coordinator.GroupStateSnapshot{"stale": {TopicName: "orders", RegistrationEpoch: 1, Partitions: []int{0}, LastActivity: time.Unix(1, 0)}}
	require.NoError(t, c.ImportStateWithRecovery(stale, proof))
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	require.Empty(t, c.ExportState())
	require.NoError(t, c.RecoveryReadinessError())
	for range 2 {
		generation := c.BeginConsumerRestore()
		require.NoError(t, c.ImportStateWithRecovery(stale, proof))
		c.FinishConsumerRestore(generation, nil)
		require.NoError(t, h.refreshLocalGroupRecovery())
		require.Empty(t, c.ExportState())
		require.NoError(t, c.RecoveryReadinessError())
	}
}

type consumerRecoverySnapshotSink struct{ bytes.Buffer }

func (*consumerRecoverySnapshotSink) ID() string    { return "consumer-recovery-test" }
func (*consumerRecoverySnapshotSink) Close() error  { return nil }
func (*consumerRecoverySnapshotSink) Cancel() error { return nil }

func TestUnknownAuthorityLiveRestoreCompletesWithoutRestart(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	tm := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
	t.Cleanup(tm.Stop)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	state := fsm.NewBrokerFSM(tm, c)
	registerRoutingBroker(t, state, "remote")
	installRoutingOffsetsTopology(t, state, "remote")
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager = tm
	overlay := map[string]*coordinator.GroupStateSnapshot{"legacy": {TopicName: "orders", RegistrationEpoch: 1, Partitions: []int{0}, Offsets: map[string]map[int]uint64{"orders": {0: 17}}, LastActivity: time.Unix(1, 0)}}
	require.NoError(t, c.ImportStateWithRecovery(overlay, nil))
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	require.False(t, c.ConsumerRecoveryRequirement().StartupPending)

	for range 2 {
		snapshot, err := state.Snapshot()
		require.NoError(t, err)
		sink := &consumerRecoverySnapshotSink{}
		require.NoError(t, snapshot.Persist(sink))
		snapshot.Release()
		before := c.ConsumerRecoveryRequirement().RequiredGeneration
		require.NoError(t, state.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))
		require.Greater(t, c.ConsumerRecoveryRequirement().RequiredGeneration, before)
		require.ErrorIs(t, c.ConsumerOperationsReadinessError(), coordinator.ErrConsumerMetadataRecovering)
		require.ErrorIs(t, h.RecoverDistributedConsumerMetadata(context.Background()), ErrConsumerRecoveryPending)
		require.NoError(t, state.ReconcileTopicMaterializations())
		require.NoError(t, state.FinalizeRecoveredPartitions())
		require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
		require.NoError(t, c.RecoveryReadinessError())
		require.False(t, c.ConsumerRecoveryRequirement().Pending())
		require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
		_, proof, err := c.ExportStateWithRecovery()
		require.NoError(t, err)
		require.False(t, proof.KnownComplete, "legacy replay must never manufacture authority")
		offset, found := c.GetOffset("legacy", "orders", 0)
		require.True(t, found)
		require.Equal(t, uint64(17), offset)
		t.Logf("restored snapshot_bytes=%d generation=%d", sink.Len(), c.ConsumerRecoveryRequirement().RequiredGeneration)
	}
}

func TestLiveFSMRestoreRequiresFinalizationBeforeUnchangedOwnershipRecovery(t *testing.T) {
	for _, owner := range []string{"local", "remote"} {
		t.Run(owner, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			cfg.LogDir = t.TempDir()
			tm := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
			t.Cleanup(tm.Stop)
			c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
			require.NoError(t, err)
			t.Cleanup(c.Stop)
			state := fsm.NewBrokerFSM(tm, c)
			registerRoutingBroker(t, state, owner)
			installRoutingOffsetsTopology(t, state, owner)
			h := newCoordinatorRoutingHandler("local", state, c)
			h.TopicManager = tm
			t.Cleanup(func() { require.NoError(t, h.Close()) })
			proof := &coordinator.ConsumerRecoveryEnvelope{Version: 1, KnownComplete: true, RaftCompatibilityKnown: true, SourcePartitionsKnown: true, RaftCompatibilityState: map[string]*coordinator.GroupStateSnapshot{}, SourcePartitions: map[string][]int{}}
			stale := map[string]*coordinator.GroupStateSnapshot{"stale": {TopicName: "orders", RegistrationEpoch: 1, Partitions: []int{0}, LastActivity: time.Unix(1, 0)}}
			require.NoError(t, c.ImportStateWithRecovery(stale, proof))
			snapshot, err := state.Snapshot()
			require.NoError(t, err)
			defer snapshot.Release()
			sink := &consumerRecoverySnapshotSink{}
			require.NoError(t, snapshot.Persist(sink))
			require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
			require.Empty(t, c.ExportState())
			ownership, err := h.localGroupRecoveryEpochs()
			require.NoError(t, err)
			if owner == "remote" {
				require.Empty(t, ownership)
			} else {
				require.Len(t, ownership, 4)
			}
			for range 2 {
				previous := c.ConsumerRecoveryRequirement().RequiredGeneration
				require.NoError(t, state.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))
				require.Greater(t, c.ConsumerRecoveryRequirement().RequiredGeneration, previous)
				require.True(t, state.HasPendingPartitionRecovery(), "real Restore must fence replay finalization")
				require.Contains(t, c.ExportState(), "stale")
				require.Error(t, c.RecoveryReadinessError())
				require.Equal(t, ownership, h.groupRecoveryEpoch, "ownership cache does not change across Restore")
				require.ErrorIs(t, h.refreshLocalGroupRecovery(), ErrConsumerRecoveryPending)
				require.Contains(t, c.ExportState(), "stale", "monitor must wait for the FSM replay fence")
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				release, err := c.BeginConsumerMetadataRecovery(ctx)
				cancel()
				require.NoError(t, err, "waiting for finalization cannot retain the recovery lease")
				release()
				require.NoError(t, state.ReconcileTopicMaterializations())
				require.NoError(t, state.FinalizeRecoveredPartitions())
				require.False(t, state.HasPendingPartitionRecovery())
				require.NoError(t, h.refreshLocalGroupRecovery())
				require.Empty(t, c.ExportState(), "strict monitor replay must discard the imported overlay")
				require.NoError(t, c.RecoveryReadinessError())
				require.Equal(t, ownership, h.groupRecoveryEpoch)
			}
		})
	}
}
