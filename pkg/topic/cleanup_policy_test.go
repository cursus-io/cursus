package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestTopicCleanupPolicyUsesBrokerDefaultAndPropagates(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.CleanupPolicy = config.CleanupPolicyCompact
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000

	diskManager := disk.NewDiskManager(cfg)
	defer diskManager.CloseAllHandlers()
	manager := NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, manager.CreateTopic("state", 1, false, false))

	created := manager.GetTopic("state")
	require.Equal(t, config.CleanupPolicyCompact, created.Policy.CleanupPolicy)
	handlerValue, err := diskManager.GetHandler("state", 0)
	require.NoError(t, err)
	require.Equal(t, config.CleanupPolicyCompact, handlerValue.(*disk.DiskHandler).CleanupPolicy())
	for _, partition := range created.Partitions {
		partition.Close()
	}
}

func TestTopicCleanupPolicyOverridesBrokerDefaultBeforeHandlerStarts(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.CleanupPolicy = config.CleanupPolicyCompact
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000

	diskManager := disk.NewDiskManager(cfg)
	defer diskManager.CloseAllHandlers()
	manager := NewTopicManager(cfg, diskManager, nil)
	policy := DefaultPolicy()
	policy.CleanupPolicy = config.CleanupPolicyDelete
	require.NoError(t, manager.CreateTopicWithPolicy("audit", 1, false, false, policy))

	created := manager.GetTopic("audit")
	require.Equal(t, config.CleanupPolicyDelete, created.Policy.CleanupPolicy)
	handlerValue, err := diskManager.GetHandler("audit", 0)
	require.NoError(t, err)
	require.Equal(t, config.CleanupPolicyDelete, handlerValue.(*disk.DiskHandler).CleanupPolicy())
	for _, partition := range created.Partitions {
		partition.Close()
	}
}

func TestTopicCleanupPolicyUpdateAppliesBeforePartitionGrowth(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000

	diskManager := disk.NewDiskManager(cfg)
	defer diskManager.CloseAllHandlers()
	manager := NewTopicManager(cfg, diskManager, nil)
	deletePolicy := DefaultPolicy()
	deletePolicy.CleanupPolicy = config.CleanupPolicyDelete
	require.NoError(t, manager.CreateTopicWithPolicy("state", 1, false, false, deletePolicy))

	compactPolicy := DefaultPolicy()
	compactPolicy.CleanupPolicy = config.CleanupPolicyCompact
	require.NoError(t, manager.CreateTopicWithPolicy("state", 2, false, false, compactPolicy))

	created := manager.GetTopic("state")
	require.Equal(t, config.CleanupPolicyCompact, created.Policy.CleanupPolicy)
	for partition := 0; partition < 2; partition++ {
		handlerValue, err := diskManager.GetHandler("state", partition)
		require.NoError(t, err)
		require.Equal(t, config.CleanupPolicyCompact, handlerValue.(*disk.DiskHandler).CleanupPolicy())
	}
	for _, partition := range created.Partitions {
		partition.Close()
	}
}

func TestTopicCleanupPolicyRejectsUnsupportedTopicModes(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		distributed   bool
		eventSourcing bool
	}{
		{name: "event sourcing", eventSourcing: true},
		{name: "distributed", distributed: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.EnabledDistribution = testCase.distributed
			diskManager := disk.NewDiskManager(cfg)
			defer diskManager.CloseAllHandlers()
			manager := NewTopicManager(cfg, diskManager, nil)
			policy := DefaultPolicy()
			policy.CleanupPolicy = config.CleanupPolicyCompact
			err := manager.CreateTopicWithPolicy("state", 1, false, testCase.eventSourcing, policy)
			require.ErrorContains(t, err, "cleanup policy compact is not supported")
			require.Nil(t, manager.GetTopic("state"))
		})
	}
}

func TestExistingEventSourcingTopicCannotBeChangedToCompact(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000
	diskManager := disk.NewDiskManager(cfg)
	defer diskManager.CloseAllHandlers()
	manager := NewTopicManager(cfg, diskManager, nil)

	require.NoError(t, manager.CreateTopic("events", 1, false, true))
	compact := DefaultPolicy()
	compact.CleanupPolicy = config.CleanupPolicyCompact
	err := manager.CreateTopicWithPolicy("events", 1, false, false, compact)
	require.ErrorContains(t, err, "cleanup policy compact is not supported")
	require.Equal(t, config.CleanupPolicyDelete, manager.GetTopic("events").Policy.CleanupPolicy)
	for _, partition := range manager.GetTopic("events").Partitions {
		partition.Close()
	}
}
