package fsm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestTruncateCleanupBoundarySnapshotAndMixedVersion(t *testing.T) {
	f := NewBrokerFSM(nil, nil)
	d := topic.DefaultDefinition("orders", nil)
	d.Partitions = 1
	d.LifecycleEpoch, d.Revision = 99, 2
	internal := topic.DefaultDefinition(config.ConsumerOffsetsTopicName, nil)
	internal.Partitions = 1
	f.topicState[d.Name], f.topicState[internal.Name] = &d, &internal
	f.partitionMetadata[d.Name+"-0"] = authoritativePartitionMetadata(1)
	f.partitionMetadata[d.Name+"-0"].LifecycleEpoch = 99
	f.partitionMetadata[internal.Name+"-0"] = authoritativePartitionMetadata(1)
	f.partitionMetadata[internal.Name+"-0"].CommittedHWM = 17
	f.captureTopicCleanupBoundaryLocked(d)
	f.brokers["new"] = &BrokerInfo{ID: "new", LifecycleProtocol: TruncateCleanupBoundaryProtocolVersion}
	s, err := f.Snapshot()
	require.NoError(t, err)
	// Mutating current state after snapshot must not move its historical cut.
	f.topicCleanupBoundaries[d.Name].Ends[0] = 18
	var buf bytes.Buffer
	require.NoError(t, s.Persist(&MockSnapshotSink{Writer: &buf}))
	var saved BrokerFSMState
	require.NoError(t, json.Unmarshal(buf.Bytes(), &saved))
	require.Equal(t, []uint64{17}, saved.TopicCleanupBoundaries[d.Name].Ends)
	restored := NewBrokerFSM(nil, nil)
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
	require.Equal(t, saved.TopicCleanupBoundaries, restored.topicCleanupBoundaries)
	for _, mutation := range []string{"topic_epoch", "metadata_epoch", "end", "missing_partition"} {
		bad := copyTopicCleanupBoundaries(saved.TopicCleanupBoundaries)
		switch mutation {
		case "topic_epoch":
			bad[d.Name].TopicEpoch++
		case "metadata_epoch":
			bad[d.Name].MetadataEpoch++
		case "end":
			bad[d.Name].Ends[0] = 100
		case "missing_partition":
			bad[d.Name].Ends = append(bad[d.Name].Ends, 0)
		}
		require.Error(t, validateTopicCleanupBoundaries(bad, saved.TopicState, saved.PartitionMetadata))
	}
	f.brokers["offline-old"] = &BrokerInfo{ID: "offline-old", Status: "inactive", LifecycleProtocol: TruncateReceiptProtocolVersion}
	require.Nil(t, f.snapshotTopicCleanupBoundariesLocked(), "old strict readers must receive the old snapshot shape")
	require.NoError(t, validateTopicCleanupBoundaries(nil, saved.TopicState, saved.PartitionMetadata))
	legacy := NewBrokerFSM(nil, nil)
	require.Equal(t, &coordinator.TopicCleanupBoundary{TopicEpoch: 99}, legacy.topicCleanupBoundaryLocked(&d))
}

func TestTruncateSnapshotRestoreRetainsConsumerCleanupUntilProof(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	m := durableCleanupTestManager(t, cfg)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, m)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	configureLifecycleCleanupOwner(t, m, c)
	recoverCleanupCoordinator(t, m, c)
	f := NewBrokerFSM(m, c)
	configureDurableCleanupOwner(t, f)
	registerLifecycleBroker(t, f, "broker-1", TruncateCleanupBoundaryProtocolVersion)
	create, err := json.Marshal(testTopicCommand("orders", 1, 1))
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: append([]byte("TOPIC:"), create...), Index: 2}))
	require.NoError(t, c.RegisterGroup("orders", "workers", 1))
	r := f.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`), Index: 3})
	require.True(t, r.(topic.TruncateResult).CleanupPending)
	// Simulate the owning broker completing the old deletion and accepting a
	// fresh active registration while this node's topic remains unmaterialized.
	require.NoError(t, c.DeleteGroup("workers"))
	require.NoError(t, c.RegisterGroup("orders", "workers", 1))
	_, err = c.AddConsumer("workers", "new-member")
	require.NoError(t, err)
	require.NoError(t, c.CommitOffset("workers", "orders", 0, 17))
	s, err := f.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, s.Persist(&MockSnapshotSink{Writer: &buf}))
	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
	require.True(t, m.IsTruncationPending("orders"), "snapshot restoration must not bypass group cleanup")
	require.Error(t, f.TopicMaterializationReadinessError())
	require.NoError(t, f.FinalizeRecoveredPartitions())
	recoverCleanupCoordinator(t, m, c)
	for i := 0; i < 20; i++ {
		err = f.ReconcileTopicMaterializations()
		if err == nil {
			break
		}
	}
	require.NoError(t, err)
	require.False(t, m.IsTruncationPending("orders"))
	require.Equal(t, uint64(3), c.GetRegistrationEpoch("workers"))
	require.Equal(t, []int{0}, c.GetMemberAssignments("workers", "new-member"))
	offset, found := c.GetOffset("workers", "orders", 0)
	require.True(t, found)
	require.Equal(t, uint64(17), offset)
}
