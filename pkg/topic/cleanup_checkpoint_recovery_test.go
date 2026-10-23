package topic_test

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCleanupCheckpointReopenSkipsCertifiedDiskWork(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	require.NoError(t, tm.CreateTopic(config.ConsumerOffsetsTopicName, 4, false, false))
	definitions := tm.ExportDefinitions()
	for partition, p := range tm.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		batch := make([]types.Message, 2048)
		for i := range batch {
			payload, key, err := coordinator.EncodeConsumerMetadataRecord(coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "unrelated", Topic: "audit", Epoch: 1, PartitionCount: 1})
			require.NoError(t, err)
			batch[i] = types.Message{Offset: uint64(i), Payload: string(payload), Key: key}
			if partition == 0 && i == 0 {
				payload, key, err := coordinator.EncodeConsumerMetadataRecord(coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordTombstone, Group: "workers", Topic: "orders", Epoch: 2})
				require.NoError(t, err)
				batch[i].Payload = string(payload)
				batch[i].Key = key
			}
		}
		require.NoError(t, p.ReplicaAppend(batch))
		require.NoError(t, p.ApplyReplicaHWM(2048))
	}
	reader := &measuredCleanupReader{TopicManager: tm}
	newGroups := func() *coordinator.Coordinator {
		c, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, reader)
		require.NoError(t, err)
		require.NoError(t, c.ImportState(map[string]*coordinator.GroupStateSnapshot{"workers": {TopicName: "orders", Partitions: []int{0}, RegistrationEpoch: 1, LastActivity: time.Now(), Members: map[string][]int{"stale": {0}}}}))
		return c
	}
	c := newGroups()
	for reader.records < 4096 {
		_, err := c.ReconcileTopicGroups("orders")
		require.Error(t, err)
	}
	beforeRecords, beforeBytes := reader.records, reader.bytes
	c.Stop()
	tm.Stop()
	dm.CloseAllHandlers()
	// Reopen real files and new partition objects, retaining only the on-disk
	// checkpoint and the restored authoritative topic/group definitions.
	dm = disk.NewDiskManager(cfg)
	tm = topic.NewTopicManager(cfg, dm, nil)
	require.NoError(t, tm.RestoreDefinitions(definitions))
	for _, p := range tm.GetTopic(config.ConsumerOffsetsTopicName).Partitions {
		require.NoError(t, p.FinalizeSnapshotRecovery(2048))
	}
	reader = &measuredCleanupReader{TopicManager: tm}
	c = newGroups()
	t.Cleanup(func() { c.Stop(); tm.Stop(); dm.CloseAllHandlers() })
	for attempts := 0; c.GetGroup("workers") != nil && attempts < 100; attempts++ {
		_, _ = c.ReconcileTopicGroups("orders")
	}
	require.Nil(t, c.GetGroup("workers"), "persisted tombstone evidence must survive reopening")
	require.Less(t, reader.records, 8192)
	require.LessOrEqual(t, beforeRecords+reader.records-8192, 1024, "only work since the last checkpoint may repeat")
	t.Logf("before_restart_records=%d before_restart_bytes=%d after_restart_records=%d after_restart_bytes=%d repeated_records=%d", beforeRecords, beforeBytes, reader.records, reader.bytes, beforeRecords+reader.records-8192)
}
