package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestHistoricalTruncateReplayPreservesRecreatedTopic(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "log-only", true: "snapshot-and-log"}[snapshot], func(t *testing.T) {
			manager, partition := newDurableFSMTopic(t, "orders")
			current := manager.GetTopic("orders").Definition()
			current.InstanceID = "instance-recreated-5678"
			current.ReplicationFactor = 1
			require.NoError(t, manager.ApplyDefinition(current))
			require.NoError(t, partition.EnqueueBatchLeader([]types.Message{{Payload: "fresh"}}))
			require.NoError(t, partition.ApplyReplicaHWM(1))
			partition.FlushDisk()
			old := testTopicCommand("orders", 1, 1)
			old.Definition.InstanceID = "instance-original-1234"
			create, err := json.Marshal(old)
			require.NoError(t, err)
			truncate := []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`)
			replayed := NewBrokerFSM(manager, nil)
			if snapshot {
				source := NewBrokerFSM(nil, nil)
				registerLifecycleBroker(t, source, "broker-1", BrokerProtocolVersionCurrent)
				require.Nil(t, source.Apply(&raft.Log{Data: append([]byte("TOPIC:"), create...), Index: 2}))
				require.True(t, source.Apply(&raft.Log{Data: truncate, Index: 3}).(topic.TruncateResult).Truncated)
				s, err := source.Snapshot()
				require.NoError(t, err)
				var buf bytes.Buffer
				require.NoError(t, s.Persist(&MockSnapshotSink{Writer: &buf}))
				require.NoError(t, replayed.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
			} else {
				replayed.BeginRecoveredPartitionReplay()
				registerLifecycleBroker(t, replayed, "broker-1", BrokerProtocolVersionCurrent)
				require.ErrorContains(t, replayed.Apply(&raft.Log{Data: append([]byte("TOPIC:"), create...), Index: 2}).(error), "instance is immutable")
				require.True(t, replayed.Apply(&raft.Log{Data: truncate, Index: 3}).(topic.TruncateResult).CleanupPending)
			}
			require.Error(t, replayed.TopicMaterializationReadinessError())
			require.False(t, manager.IsTruncationPending("orders"), "old instance cannot install a destructive local intent")
			require.Same(t, partition, manager.GetTopic("orders").Partitions[0])
			require.Equal(t, uint64(1), partition.NextOffset())
			require.Equal(t, current, manager.GetTopic("orders").Definition())
			require.Nil(t, replayed.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 4}))
			create, err = json.Marshal(TopicCommand{Definition: &current})
			require.NoError(t, err)
			require.Nil(t, replayed.Apply(&raft.Log{Data: append([]byte("TOPIC:"), create...), Index: 5}))
			commit, err := json.Marshal(partitionCommitCommand{Topic: "orders", Partition: 0, Leader: "broker-1", LeaderEpoch: 1, HWM: 1, LifecycleEpoch: 1})
			require.NoError(t, err)
			require.Nil(t, replayed.Apply(&raft.Log{Data: append([]byte("PARTITION_COMMIT:"), commit...), Index: 6}))
			require.NoError(t, replayed.FinalizeRecoveredPartitions())
			require.NoError(t, replayed.TopicMaterializationReadinessError())
			require.Same(t, partition, manager.GetTopic("orders").Partitions[0])
			messages, err := partition.ReadCommitted(0, 10)
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.Equal(t, "fresh", messages[0].Payload)
		})
	}
}

func TestColdTruncateReplayPreservesRecreatedTopic(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "log-only", true: "snapshot-and-log"}[snapshot], func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution, cfg.LogDir = true, t.TempDir()
			dm := disk.NewDiskManager(cfg)
			manager := topic.NewTopicManager(cfg, dm, nil)
			current := topic.DefaultDefinition("orders", cfg)
			current.InstanceID, current.Partitions, current.ReplicationFactor = "instance-recreated-5678", 1, 1
			require.NoError(t, manager.ApplyDefinition(current))
			partition, err := manager.GetTopic("orders").GetPartition(0)
			require.NoError(t, err)
			partition.SetHWM(0)
			require.NoError(t, partition.EnqueueBatchLeader([]types.Message{{Payload: "fresh"}}))
			require.NoError(t, partition.ApplyReplicaHWM(1))
			partition.FlushDisk()
			manager.Stop()
			dm.CloseAllHandlers()
			dm = disk.NewDiskManager(cfg)
			manager = topic.NewTopicManager(cfg, dm, nil)
			t.Cleanup(func() { manager.Stop(); dm.CloseAllHandlers() })
			require.NoError(t, manager.RestoreTopics())
			require.Nil(t, manager.GetTopic("orders"), "distributed startup has no standalone registry")
			old := testTopicCommand("orders", 1, 1)
			old.Definition.InstanceID = "instance-original-1234"
			encoded, err := json.Marshal(old)
			require.NoError(t, err)
			prefix := NewBrokerFSM(nil, nil)
			if !snapshot {
				prefix = NewBrokerFSM(manager, nil)
				prefix.BeginRecoveredPartitionReplay()
			}
			registerLifecycleBroker(t, prefix, "broker-1", BrokerProtocolVersionCurrent)
			require.Nil(t, prefix.Apply(&raft.Log{Data: append([]byte("TOPIC:"), encoded...), Index: 2}))
			require.True(t, prefix.Apply(&raft.Log{Data: []byte(`TOPIC_TRUNCATE:{"topic":"orders","expected_revision":1}`), Index: 3}).(topic.TruncateResult).Truncated)
			replayed := prefix
			if snapshot {
				s, err := prefix.Snapshot()
				require.NoError(t, err)
				var buf bytes.Buffer
				require.NoError(t, s.Persist(&MockSnapshotSink{Writer: &buf}))
				replayed = NewBrokerFSM(manager, nil)
				require.NoError(t, replayed.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
			}
			require.Nil(t, manager.GetTopic("orders"), "historical definitions must not open newer files")
			require.Error(t, replayed.TopicMaterializationReadinessError())
			require.Nil(t, replayed.Apply(&raft.Log{Data: []byte(`TOPIC_DELETE:{"topic":"orders"}`), Index: 4}))
			encoded, err = json.Marshal(TopicCommand{Definition: &current})
			require.NoError(t, err)
			require.Nil(t, replayed.Apply(&raft.Log{Data: append([]byte("TOPIC:"), encoded...), Index: 5}))
			commit, err := json.Marshal(partitionCommitCommand{Topic: "orders", Partition: 0, Leader: "broker-1", LeaderEpoch: 1, HWM: 1, LifecycleEpoch: 1})
			require.NoError(t, err)
			require.Nil(t, replayed.Apply(&raft.Log{Data: append([]byte("PARTITION_COMMIT:"), commit...), Index: 6}))
			require.NoError(t, replayed.FinalizeRecoveredPartitions())
			require.NoError(t, replayed.TopicMaterializationReadinessError())
			partition, err = manager.GetTopic("orders").GetPartition(0)
			require.NoError(t, err)
			messages, err := partition.ReadCommitted(0, 10)
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.Equal(t, "fresh", messages[0].Payload)
		})
	}
}
