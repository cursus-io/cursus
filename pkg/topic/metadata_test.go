package topic

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStandaloneTopicMetadataRestoresDefinitionAndData(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000

	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	policy := Policy{
		CleanupPolicy:     config.CleanupPolicyCompact,
		Partitioner:       PartitionerRoundRobin,
		AuthPolicy:        AuthPolicyACL,
		ReadACL:           []string{"reader"},
		WriteACL:          []string{"writer"},
		RetentionHours:    12,
		RetentionBytes:    4096,
		MinInSyncReplicas: intPtr(1),
	}
	require.NoError(t, manager.CreateTopicWithPolicy("orders", 2, true, false, policy))
	require.NoError(t, manager.CreateTopic("events", 1, false, true))
	require.NoError(t, manager.PublishToPartitionWithAck("orders", 1, &types.Message{Payload: "persisted"}))
	closeTopicManager(manager)
	dm.CloseAllHandlers()

	restartedDM := disk.NewDiskManager(cfg)
	defer restartedDM.CloseAllHandlers()
	restarted := NewTopicManager(cfg, restartedDM, nil)
	require.NoError(t, restarted.RestoreTopics())
	require.Equal(t, 2, restarted.RuntimeSnapshot().MetadataRestoredTopicCount)
	defer closeTopicManager(restarted)

	restored := restarted.GetTopic("orders")
	require.NotNil(t, restored)
	require.Equal(t, 2, len(restored.Partitions))
	require.True(t, restored.IsIdempotent)
	require.False(t, restored.IsEventSourcing)
	require.Equal(t, config.CleanupPolicyCompact, restored.Policy.CleanupPolicy)
	require.Equal(t, PartitionerRoundRobin, restored.Policy.Partitioner)
	require.Equal(t, AuthPolicyACL, restored.Policy.AuthPolicy)
	require.Equal(t, []string{"reader"}, restored.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, restored.Policy.WriteACL)
	require.Equal(t, 12, restored.Policy.RetentionHours)
	require.Equal(t, int64(4096), restored.Policy.RetentionBytes)
	require.NotNil(t, restored.Policy.MinInSyncReplicas)
	require.Equal(t, 1, *restored.Policy.MinInSyncReplicas)
	require.Equal(t, DefaultReplicationFactor, restored.ReplicationFactor)
	require.Equal(t, uint64(1), restored.Revision)
	require.True(t, restarted.GetTopic("events").IsEventSourcing)
	require.Equal(t, config.CleanupPolicyDelete, restarted.GetTopic("events").Policy.CleanupPolicy)

	messages, err := restarted.ReadTopicPartition("orders", 1, 0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "persisted", messages[0].Payload)

	handler, err := restartedDM.GetHandler("orders", 1)
	require.NoError(t, err)
	require.Equal(t, config.CleanupPolicyCompact, handler.(*disk.DiskHandler).CleanupPolicy())
}

func TestStandaloneLegacyManifestWithoutMinInSyncReplicasRestoresBrokerFallback(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.MinInSyncReplicas = 2
	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("legacy", 1, false, false))
	manifest, err := os.ReadFile(filepath.Join(cfg.LogDir, TopicMetadataFileName))
	require.NoError(t, err)
	require.NotContains(t, string(manifest), "min_in_sync_replicas")
	closeTopicManager(manager)
	dm.CloseAllHandlers()

	restartedDM := disk.NewDiskManager(cfg)
	defer restartedDM.CloseAllHandlers()
	restarted := NewTopicManager(cfg, restartedDM, nil)
	require.NoError(t, restarted.RestoreTopics())
	defer closeTopicManager(restarted)
	policy := restarted.GetTopic("legacy").Policy
	require.Nil(t, policy.MinInSyncReplicas)
	require.Equal(t, 2, policy.EffectiveMinInSyncReplicas(cfg.MinInSyncReplicas))
}

func TestStandaloneTopicMetadataPersistsGrowthAndDeletion(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000

	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("sessions", 1, false, false))
	updated := DefaultPolicy()
	updated.RetentionHours = 24
	require.NoError(t, manager.CreateTopicWithPolicy("sessions", 3, false, false, updated))
	require.NoError(t, manager.CreateTopic("deleted", 1, false, false))
	deleted, err := manager.DeleteTopicDurable("deleted")
	require.NoError(t, err)
	require.True(t, deleted)
	closeTopicManager(manager)
	dm.CloseAllHandlers()

	restartedDM := disk.NewDiskManager(cfg)
	defer restartedDM.CloseAllHandlers()
	restarted := NewTopicManager(cfg, restartedDM, nil)
	require.NoError(t, restarted.RestoreTopics())
	defer closeTopicManager(restarted)
	require.Nil(t, restarted.GetTopic("deleted"))
	require.Equal(t, 3, len(restarted.GetTopic("sessions").Partitions))
	require.Equal(t, 24, restarted.GetTopic("sessions").Policy.RetentionHours)
	require.Equal(t, uint64(2), restarted.GetTopic("sessions").Revision)
}

func TestStandaloneTopicMetadataMigratesVersionOneDefinitionDefaults(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000

	dm := disk.NewDiskManager(cfg)
	manager := NewTopicManager(cfg, dm, nil)
	require.NoError(t, manager.CreateTopic("legacy", 1, false, false))
	closeTopicManager(manager)
	dm.CloseAllHandlers()

	manifestPath := filepath.Join(cfg.LogDir, TopicMetadataFileName)
	raw, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	var legacy map[string]any
	require.NoError(t, json.Unmarshal(raw, &legacy))
	legacy["version"] = float64(1)
	topics := legacy["topics"].([]any)
	definition := topics[0].(map[string]any)
	delete(definition, "revision")
	delete(definition, "replication_factor")
	raw, err = json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, raw, 0o600))

	restartedDM := disk.NewDiskManager(cfg)
	defer restartedDM.CloseAllHandlers()
	restarted := NewTopicManager(cfg, restartedDM, nil)
	require.NoError(t, restarted.RestoreTopics())
	defer closeTopicManager(restarted)
	restored := restarted.GetTopic("legacy")
	require.NotNil(t, restored)
	require.Equal(t, DefaultReplicationFactor, restored.ReplicationFactor)
	require.Equal(t, uint64(InitialDefinitionRevision), restored.Revision)

	policy := restored.Policy
	policy.RetentionHours = 24
	require.NoError(t, restarted.CreateTopicWithPolicy("legacy", 1, false, false, policy))
	raw, err = os.ReadFile(manifestPath)
	require.NoError(t, err)
	var migrated topicMetadataManifest
	require.NoError(t, json.Unmarshal(raw, &migrated))
	require.Equal(t, 2, migrated.Version, "first-generation metadata remains readable by the previous release")
	require.Equal(t, uint64(2), migrated.Topics[0].Revision)
	require.Equal(t, DefaultReplicationFactor, migrated.Topics[0].ReplicationFactor)
}

func TestStandaloneTopicMetadataCorruptionFailsClosed(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(cfg.LogDir, TopicMetadataFileName),
		[]byte(`{"version":1,"topics":[],"unexpected":true}`),
		0o600,
	))

	dm := disk.NewDiskManager(cfg)
	defer dm.CloseAllHandlers()
	manager := NewTopicManager(cfg, dm, nil)
	err := manager.RestoreTopics()
	require.ErrorContains(t, err, "unknown field")
	require.Empty(t, manager.ListTopics())
}

func TestStandaloneTopicMetadataVersionTwoRequiresRevisionAndReplicationFactor(t *testing.T) {
	for _, missing := range []string{"revision", "replication_factor"} {
		t.Run(missing, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			definition := map[string]any{
				"name": "orders", "revision": 1, "partitions": 1, "replication_factor": 3,
				"idempotent": false, "event_sourcing": false, "policy": DefaultPolicy(),
			}
			delete(definition, missing)
			manifest, err := json.Marshal(map[string]any{"version": 2, "topics": []any{definition}})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(cfg.LogDir, TopicMetadataFileName), manifest, 0o600))

			dm := disk.NewDiskManager(cfg)
			defer dm.CloseAllHandlers()
			err = NewTopicManager(cfg, dm, nil).RestoreTopics()
			require.ErrorContains(t, err, missing+" must be present")
		})
	}
}

func TestTopicMetadataFailureDoesNotExposePolicyOrPartitionUpdate(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000

	dm := disk.NewDiskManager(cfg)
	defer dm.CloseAllHandlers()
	manager := NewTopicManager(cfg, dm, nil)
	defer closeTopicManager(manager)
	require.NoError(t, manager.CreateTopic("state", 1, false, false))

	blockedPath := filepath.Join(cfg.LogDir, "blocked-metadata-target")
	require.NoError(t, os.Mkdir(blockedPath, 0o750))
	manager.metadataStore.path = blockedPath
	compact := DefaultPolicy()
	compact.CleanupPolicy = config.CleanupPolicyCompact
	err := manager.CreateTopicWithPolicy("state", 2, false, false, compact)
	require.Error(t, err)

	current := manager.GetTopic("state")
	require.Equal(t, 1, len(current.Partitions))
	require.Equal(t, config.CleanupPolicyDelete, current.Policy.CleanupPolicy)
}

func TestValidateNameUsesPortableDiskContract(t *testing.T) {
	for _, name := range []string{"orders", "orders.v2", "orders-v2", "__consumer_offsets", "exactly-once-acks=all_broker_failure"} {
		require.NoError(t, ValidateName(name))
	}
	for _, name := range []string{"", ".", "..", "orders/2026", `orders\\2026`, "orders 2026", "한글"} {
		require.Error(t, ValidateName(name), name)
	}
}

func TestDeleteTopicDurableRejectsInvalidNameBeforeLookup(t *testing.T) {
	manager := NewTopicManager(config.DefaultConfig(), nil, nil)

	deleted, err := manager.DeleteTopicDurable("../escape")
	require.False(t, deleted)
	require.ErrorContains(t, err, "invalid topic name")
}

func closeTopicManager(manager *TopicManager) {
	for _, name := range manager.ListTopics() {
		current := manager.GetTopic(name)
		for _, partition := range current.Partitions {
			partition.Close()
		}
	}
}
