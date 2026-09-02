package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestInternalConsumerMetadataPolicyCannotBeOverriddenOrDeleted(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	t.Cleanup(dm.CloseAllHandlers)
	manager := NewTopicManager(cfg, dm, nil)

	applicationPolicy := DefaultPolicy()
	applicationPolicy.CleanupPolicy = config.CleanupPolicyDelete
	applicationPolicy.RetentionHours = 1
	applicationPolicy.RetentionBytes = 1
	require.NoError(t, manager.CreateTopicWithPolicy(
		config.ConsumerOffsetsTopicName, 1, true, true, applicationPolicy,
	))
	require.NoError(t, manager.CreateTopicWithPolicy(
		config.ConsumerOffsetsTopicName, 1, false, false, applicationPolicy,
	), "repeated application CREATE cannot weaken internal durability")

	internal := manager.GetTopic(config.ConsumerOffsetsTopicName)
	require.NotNil(t, internal)
	definition := internal.Definition()
	require.False(t, definition.Idempotent)
	require.False(t, definition.EventSourcing)
	require.Equal(t, config.CleanupPolicyCompact, definition.Policy.CleanupPolicy)
	require.Zero(t, definition.Policy.RetentionHours)
	require.Zero(t, definition.Policy.RetentionBytes)

	storage, err := dm.GetHandler(config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	handler, ok := storage.(*disk.DiskHandler)
	require.True(t, ok)
	require.Equal(t, config.CleanupPolicyCompact, handler.CleanupPolicy())
	hours, bytes := handler.RetentionPolicy()
	require.Zero(t, hours)
	require.Zero(t, bytes)

	deleted, err := manager.DeleteTopicDurable(config.ConsumerOffsetsTopicName)
	require.False(t, deleted)
	require.ErrorContains(t, err, "broker-owned internal consumer metadata topic")
}
