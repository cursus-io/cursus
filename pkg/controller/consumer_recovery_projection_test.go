package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func projectionRecoveryFixture(t *testing.T, activated bool) (*CommandHandler, *fsm.BrokerFSM, *stagedConsumerTestReader) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	tm := topic.NewTopicManager(cfg, dm, nil)
	t.Cleanup(tm.Stop)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	state := fsm.NewBrokerFSM(tm, c)
	transferRegister(t, state, "remote", "remote-process", "active", 2)
	transferRegister(t, state, "local", "local-process", "active", 2)
	installRoutingOffsetsTopology(t, state, "remote")
	metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	metadata.CommittedHWM = 5
	applyProjectionMetadata(t, state, 0, metadata)
	if activated {
		require.Nil(t, state.Apply(&raft.Log{Data: []byte(`CONSUMER_ROUTING_ACTIVATE:{"voters":["remote","local"]}`)}))
	}
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager = tm
	h.Config = cfg
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	remote := state.GetBroker("remote")
	reader := &stagedConsumerTestReader{ends: []uint64{5, 0, 0, 0}, records: map[int][]types.Message{0: stagedConsumerRecords(t, "historical-group")}, sources: map[int]ConsumerMetadataRecoverySource{0: {BrokerID: remote.ID, IncarnationID: remote.IncarnationID, IncarnationEpoch: remote.IncarnationEpoch}}}
	return h, state, reader
}

func applyProjectionMetadata(t *testing.T, state *fsm.BrokerFSM, partition int, metadata *fsm.PartitionMetadata) {
	t.Helper()
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte(fmt.Sprintf("PARTITION:%s-%d:", config.ConsumerOffsetsTopicName, partition)), data...)}))
}

func TestConsumerFiniteProjectionRequiresActivationAndRejectsNewOwnership(t *testing.T) {
	for _, mutation := range []string{"preactivation", "remote writes", "new ownership", "partition count"} {
		t.Run(mutation, func(t *testing.T) {
			h, state, reader := projectionRecoveryFixture(t, mutation != "preactivation")
			calls := 0
			h.SetConsumerMetadataRecoveryProvider(func(context.Context, ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error) {
				calls++
				metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
				metadata.CommittedHWM++
				if mutation == "new ownership" {
					metadata.Leader = "local"
					metadata.LeaderEpoch++
					metadata.Replicas = []string{"local"}
					metadata.ISR = []string{"local"}
				}
				if mutation == "partition count" {
					metadata.PartitionCount++
				}
				applyProjectionMetadata(t, state, 0, metadata)
				return reader, nil
			})
			err := h.RecoverDistributedConsumerMetadata(context.Background())
			require.Equal(t, 1, calls)
			require.True(t, reader.closed)
			if mutation != "remote writes" {
				require.Error(t, err)
				require.Empty(t, h.groupRecoveryOwnership, "newly observed ownership must not be certified")
				return
			}
			require.NoError(t, err)
			require.NoError(t, h.Coordinator.RecoveryReadinessError())
			value, ok := h.Coordinator.GetOffset("historical-group", "orders", 0)
			require.True(t, ok)
			require.Equal(t, uint64(37), value, "fixed historical physical sources remain part of replay")
			require.Equal(t, 2, reader.reads)
		})
	}
}

func TestConsumerFiniteProjectionUsesLatestLocalOwnedEnd(t *testing.T) {
	h, state, reader := projectionRecoveryFixture(t, true)
	metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-1")
	metadata.Leader = "local"
	metadata.LeaderEpoch++
	metadata.Replicas = []string{"local"}
	metadata.ISR = []string{"local"}
	applyProjectionMetadata(t, state, 1, metadata)
	group := ""
	for i := 0; i < 100; i++ {
		candidate := fmt.Sprintf("owned-%d", i)
		_, _, partition, _, err := h.Cluster.Router.FindCoordinatorWithEpoch(candidate)
		require.NoError(t, err)
		if partition == 1 {
			group = candidate
			break
		}
	}
	require.NotEmpty(t, group)
	h.SetConsumerMetadataRecoveryProvider(func(context.Context, ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error) {
		p, err := h.TopicManager.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(1)
		require.NoError(t, err)
		message := consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: group, Topic: "orders", PartitionCount: 1, Epoch: 1}, 0)
		require.NoError(t, p.ReplicaAppend([]types.Message{message}))
		require.NoError(t, p.ApplyReplicaHWM(1))
		p.FlushDisk()
		metadata.CommittedHWM = 1
		applyProjectionMetadata(t, state, 1, metadata)
		return reader, nil
	})
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	require.Contains(t, h.Coordinator.ExportState(), group)
	require.Equal(t, uint64(1), h.groupRecoveryOwnership[1].End)
	require.True(t, h.consumerOwnershipRecovered(1, metadata.LeaderEpoch))
	metadata.PartitionCount++
	applyProjectionMetadata(t, state, 1, metadata)
	require.False(t, h.consumerOwnershipRecovered(1, metadata.LeaderEpoch), "count changes invalidate ownership even if leader epoch is unchanged")
}

func TestConsumerProjectionCompletesAmidRemoteCommitsWithoutOmittingOwnedTail(t *testing.T) {
	h, state, reader := projectionRecoveryFixture(t, true)
	metadata := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-1")
	metadata.Leader = "local"
	metadata.LeaderEpoch++
	metadata.Replicas, metadata.ISR = []string{"local"}, []string{"local"}
	applyProjectionMetadata(t, state, 1, metadata)
	groupFor := func(partition uint64) string {
		for i := 0; i < 100; i++ {
			candidate := fmt.Sprintf("projection-%d", i)
			_, _, p, _, err := h.Cluster.Router.FindCoordinatorWithEpoch(candidate)
			require.NoError(t, err)
			if p == partition {
				return candidate
			}
		}
		t.Fatal("no group for requested partition")
		return ""
	}
	ownedGroup, remoteGroup := groupFor(1), groupFor(0)
	// Physical history predating routing activation may live on another owner.
	reader.records[0] = stagedConsumerRecords(t, ownedGroup)
	expectedBytes := len(reader.records[0][0].Payload) + len(reader.records[0][1].Payload)
	routes, _, err := state.ConsumerMetadataRoutes([]string{ownedGroup, remoteGroup})
	require.NoError(t, err)
	commit := func(route types.ConsumerMetadataRoute, end uint64) {
		command := map[string]any{"topic": config.ConsumerOffsetsTopicName, "partition": route.Partition, "leader": route.Owner, "leader_epoch": route.LeaderEpoch, "lifecycle_epoch": route.LifecycleEpoch, "hwm": end, "consumer_route": route}
		data, err := json.Marshal(command)
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION_COMMIT:"), data...)}))
	}
	calls := 0
	h.SetConsumerMetadataRecoveryProvider(func(_ context.Context, request ConsumerMetadataRecoveryStageRequest) (StagedConsumerMetadataRecovery, error) {
		calls++
		require.Equal(t, uint64(5), request.Metadata[0].CommittedHWM)
		for end := uint64(6); end <= 105; end++ {
			commit(routes[remoteGroup], end)
		}
		p, err := h.TopicManager.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(1)
		require.NoError(t, err)
		records := stagedConsumerRecords(t, ownedGroup)
		records[1] = consumerMetadataMessage(t, coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: ownedGroup, Topic: "orders", Epoch: 1, Revision: 2, Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: 99}}}, 1)
		require.NoError(t, p.ReplicaAppend(records))
		require.NoError(t, p.ApplyReplicaHWM(2))
		p.FlushDisk()
		commit(routes[ownedGroup], 2)
		// The fixed remote cut is safe only because new records for this group
		// cannot be committed to the remote partition after routing activation.
		forged := routes[remoteGroup]
		forged.Group = ownedGroup
		require.ErrorIs(t, state.ValidateConsumerMetadataRoute(config.ConsumerOffsetsTopicName, 0, &forged), fsm.ErrPartitionCommitFenced)
		return reader, nil
	})
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	require.Equal(t, 1, calls, "remote commits must not cause unbounded re-staging")
	require.True(t, reader.closed)
	require.Equal(t, 2, reader.reads)
	require.Equal(t, expectedBytes, reader.bytes, "only the certified historical range is replayed")
	offset, found := h.Coordinator.GetOffset(ownedGroup, "orders", 0)
	require.True(t, found)
	require.Equal(t, uint64(99), offset, "owned state includes the newer committed tail")
	require.Equal(t, uint64(2), h.groupRecoveryOwnership[1].End)
	resolved, err := h.ResolveGroupCoordinators([]string{ownedGroup, remoteGroup})
	require.NoError(t, err)
	require.True(t, resolved[ownedGroup])
	require.False(t, resolved[remoteGroup], "a historical remote view must not authorize serving its groups")
	require.False(t, h.IsGroupCoordinator(remoteGroup))
	t.Logf("remote_commits=100 stages=%d staged_reads=%d staged_payload_bytes=%d owned_end=%d", calls, reader.reads, reader.bytes, h.groupRecoveryOwnership[1].End)
}
