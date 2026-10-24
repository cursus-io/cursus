package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// Legacy snapshots remain recoverable across historical routing activation.
func TestLegacyRecoveryAcrossRoutingActivation(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	tm := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
	t.Cleanup(tm.Stop)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	state := fsm.NewBrokerFSM(tm, c)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"local","addr":"localhost:7000","status":"active","incarnation_id":"process-1","lifecycle_protocol":6,"recovery_protocol":2}`)}))
	installRoutingOffsetsTopology(t, state, "local")
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager = tm
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	overlay := map[string]*coordinator.GroupStateSnapshot{"legacy": {TopicName: "orders", RegistrationEpoch: 1, Partitions: []int{0}, Offsets: map[string]map[int]uint64{"orders": {0: 17}}, LastActivity: time.Unix(1, 0).UTC()}}
	require.NoError(t, c.ImportStateWithRecovery(overlay, nil))
	require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
	require.NoError(t, c.RecoveryReadinessError())
	require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	require.True(t, h.consumerOwnershipRecovered(0, m.LeaderEpoch))
	require.NoError(t, h.refreshLocalGroupRecovery())
	before := c.ExportState()

	// All declared capabilities pass, although the recovery authority is unknown.
	require.Nil(t, state.Apply(&raft.Log{Data: []byte(`CONSUMER_ROUTING_ACTIVATE:{"voters":["local"]}`)}))
	require.True(t, state.ConsumerMetadataRoutingEnabled())
	require.False(t, h.consumerOwnershipRecovered(0, m.LeaderEpoch))
	require.ErrorIs(t, h.ensureGroupRecovery(0, m.LeaderEpoch), coordinator.ErrConsumerMetadataRecovering)
	require.Contains(t, h.HandleCommand("GROUP_STATUS group=legacy", NewClientContext("legacy", 0)), "consumer_metadata_recovering")
	require.NoError(t, h.refreshLocalGroupRecovery())
	require.True(t, h.consumerOwnershipRecovered(0, m.LeaderEpoch))
	require.Equal(t, before, c.ExportState(), "ownership recovery must preserve offsets and group state")
	require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
	require.False(t, h.ConsumerRoutingRecoveryReady(), "legacy replay cannot certify new strict activation")
	for range 2 {
		snapshot, err := state.Snapshot()
		require.NoError(t, err)
		sink := &consumerRecoverySnapshotSink{}
		require.NoError(t, snapshot.Persist(sink))
		snapshot.Release()
		require.NoError(t, state.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))
		require.True(t, state.ConsumerMetadataRoutingEnabled())
		require.ErrorIs(t, h.ensureGroupRecovery(0, m.LeaderEpoch), coordinator.ErrConsumerMetadataRecovering)
		require.NoError(t, state.ReconcileTopicMaterializations())
		require.NoError(t, state.FinalizeRecoveredPartitions())
		require.NoError(t, h.RecoverDistributedConsumerMetadata(context.Background()))
		require.True(t, h.consumerOwnershipRecovered(0, m.LeaderEpoch))
		require.Equal(t, before, c.ExportState())
		require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
	}
}

func TestLegacyRecoveryRequiresPhysicalCommittedCoverage(t *testing.T) {
	for _, fault := range []string{"none", "gap", "prefix", "tail", "corrupt", "uncommitted tail"} {
		t.Run(fault, func(t *testing.T) {
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
			require.NoError(t, c.ImportStateWithRecovery(nil, nil))
			state := fsm.NewBrokerFSM(tm, c)
			registerRoutingBroker(t, state, "local")
			installRoutingOffsetsTopology(t, state, "local")
			h := newCoordinatorRoutingHandler("local", state, c)
			h.TopicManager = tm
			t.Cleanup(func() { require.NoError(t, h.Close()) })
			p, err := tm.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
			require.NoError(t, err)
			records := stagedConsumerRecords(t, "workers")
			records[1].Offset = 1
			end := uint64(2)
			switch fault {
			case "gap":
				records[1].Offset = 2
				end = 3
			case "prefix":
				records[0].Offset = 1
				records[1].Offset = 2
				end = 3
			case "tail":
				records = records[:1]
			case "corrupt":
				records[1].Payload = `{"version":`
			case "uncommitted tail":
				end = 1
			}
			// Compacted-range installation deliberately constructs missing
			// physical records while retaining a plausible logical end/HWM.
			physicalEnd := max(end, uint64(2))
			require.NoError(t, p.ReplicaAppendCompactedRange(records, physicalEnd))
			require.NoError(t, p.ApplyReplicaHWM(end))
			p.FlushDisk()
			m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
			m.CommittedHWM = end
			data, err := json.Marshal(m)
			require.NoError(t, err)
			require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-0:"), data...)}))
			before := c.ExportState()
			err = h.RecoverDistributedConsumerMetadata(context.Background())
			if fault == "none" || fault == "uncommitted tail" {
				require.NoError(t, err)
				offset, found := c.GetOffset("workers", "orders", 0)
				if fault == "none" {
					require.True(t, found)
					require.Equal(t, uint64(37), offset)
				} else {
					require.False(t, found, "uncommitted offset snapshot must not be replayed")
				}
			} else {
				require.Error(t, err)
				require.True(t, c.ConsumerRecoveryRequirement().Pending())
				require.Equal(t, before, c.ExportState())
			}
			require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
		})
	}
}

// Missing committed records must keep requests fenced and preserve the snapshot.
func TestLegacyRecoveryMissingCoveragePreservesSnapshot(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	tm := topic.NewTopicManager(cfg, &testMockHandlerProvider{}, nil)
	t.Cleanup(tm.Stop)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	state := fsm.NewBrokerFSM(tm, c)
	registerRoutingBroker(t, state, "local")
	installRoutingOffsetsTopology(t, state, "local")
	h := newCoordinatorRoutingHandler("local", state, c)
	h.TopicManager = tm
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	overlay := map[string]*coordinator.GroupStateSnapshot{"legacy": {TopicName: "orders", RegistrationEpoch: 1, Partitions: []int{0}, Offsets: map[string]map[int]uint64{"orders": {0: 17}}, LastActivity: time.Unix(1, 0).UTC()}}
	require.NoError(t, c.ImportStateWithRecovery(overlay, nil))
	m := state.GetPartitionMetadata(config.ConsumerOffsetsTopicName + "-0")
	m.CommittedHWM = 5
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("PARTITION:"+config.ConsumerOffsetsTopicName+"-0:"), data...)}))
	p, err := tm.GetTopic(config.ConsumerOffsetsTopicName).GetPartition(0)
	require.NoError(t, err)
	require.Equal(t, uint64(0), p.OffsetRange().HWM)
	before := c.ExportState()
	require.ErrorIs(t, h.RecoverDistributedConsumerMetadata(context.Background()), ErrConsumerRecoveryPending)
	require.Error(t, c.RecoveryReadinessError())
	require.False(t, h.consumerOwnershipRecovered(0, m.LeaderEpoch))
	require.Equal(t, before, c.ExportState())
	require.ErrorIs(t, c.ValidateConsumerMetadataBootstrapEligibility(0), coordinator.ErrConsumerMetadataBootstrapUnknown)
}
