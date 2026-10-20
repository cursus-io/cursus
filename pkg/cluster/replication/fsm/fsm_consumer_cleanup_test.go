package fsm

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// These existing FSM tests model an owning coordinator with an acknowledged
// writer. Their MockStorageHandler intentionally does not retain record bodies.
// Follower replay is exercised with real disk storage below and in the E2E.
func configureLifecycleCleanupOwner(t *testing.T, manager *topic.TopicManager, c *coordinator.Coordinator) {
	t.Helper()
	for _, p := range manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		require.NoError(t, p.ReconcileCommittedHWM(0))
	}
	c.SetGroupSessionCallbacks(func(string) bool { return true }, nil)
	c.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error { return nil })
}

func TestBrokerFSMOwnerCleanupNeverAppendsDuringRaftApply(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	manager := topic.NewTopicManager(cfg, &MockHandlerProvider{}, nil)
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, manager)
	require.NoError(t, err)
	t.Cleanup(func() { c.Stop(); manager.Stop() })
	configureLifecycleCleanupOwner(t, manager, c)
	f := NewBrokerFSM(manager, c)
	registerLifecycleBroker(t, f, "broker-1", TopicLifecycleProtocolVersion)
	create, err := json.Marshal(testTopicCommand("orders", 1, 1))
	require.NoError(t, err)
	require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 1}))
	require.NoError(t, c.RegisterGroup("orders", "workers", 1))
	writes := 0
	c.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error {
		if !f.materializationMu.TryLock() {
			return errors.New("metadata writer is holding the materialization lock")
		}
		f.materializationMu.Unlock()
		writes++
		return nil
	})
	result := f.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`), Index: 2})
	require.True(t, result.(topic.TruncateResult).CleanupPending)
	require.Zero(t, writes, "a new append can require this same Raft apply loop to commit")
	require.NoError(t, f.ReconcileTopicMaterializations())
	require.Equal(t, 1, writes)
	require.Empty(t, f.TopicMaterializationIssues())
}

func TestBrokerFSMFollowerCleanupRequiresCommittedTombstone(t *testing.T) {
	for _, operation := range []string{"delete", "truncate"} {
		t.Run(operation, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			cfg.LogDir = t.TempDir()
			dm := disk.NewDiskManager(cfg)
			manager := topic.NewTopicManager(cfg, dm, nil)
			c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, manager)
			require.NoError(t, err)
			t.Cleanup(func() { c.Stop(); manager.Stop(); dm.CloseAllHandlers() })
			for _, p := range manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
				require.NoError(t, p.ReconcileCommittedHWM(0))
			}
			f := NewBrokerFSM(manager, c)
			registerLifecycleBroker(t, f, "broker-1", TopicLifecycleProtocolVersion)
			create, err := json.Marshal(testTopicCommand("orders", 1, 1))
			require.NoError(t, err)
			require.Nil(t, f.Apply(&raft.Log{Data: []byte("TOPIC:" + string(create)), Index: 1}))
			require.NoError(t, c.RegisterGroup("orders", "workers", 1))
			_, err = c.AddConsumer("workers", "stale-member")
			require.NoError(t, err)
			c.SetOffsetRecordWriter(func(coordinator.ConsumerMetadataRecord) error {
				t.Error("follower attempted a metadata write")
				return nil
			})
			command := `TOPIC_DELETE:{"topic":"orders"}`
			if operation == "truncate" {
				command = `TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`
			}
			f.Apply(&raft.Log{Data: []byte(command), Index: 2})
			require.Error(t, f.TopicMaterializationReadinessError())
			require.NotNil(t, c.GetGroup("workers"))
			require.NotNil(t, f.TopicMaterializationIssues()[0].ConsumerCleanup)
			payload, key, err := coordinator.EncodeConsumerMetadataRecord(coordinator.ConsumerMetadataRecord{
				Version: 1, Type: coordinator.ConsumerMetadataRecordTombstone,
				Group: "workers", Topic: "orders", Epoch: 2,
			})
			require.NoError(t, err)
			p := manager.GetTopic(config.ConsumerOffsetsTopicName).Partitions[0]
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Payload: string(payload), Key: key}}))
			require.Error(t, f.ReconcileTopicMaterializations(), "an uncommitted record is not deletion authority")
			require.NoError(t, p.ApplyReplicaHWM(1))
			require.NoError(t, f.ReconcileTopicMaterializations())
			require.Nil(t, c.GetGroup("workers"))
			require.Empty(t, f.TopicMaterializationIssues())
			if operation == "truncate" {
				require.NotNil(t, manager.GetTopic("orders"))
				require.Equal(t, uint64(2), manager.GetTopic("orders").LifecycleEpoch)
			} else {
				require.Nil(t, manager.GetTopic("orders"))
			}
		})
	}
}
