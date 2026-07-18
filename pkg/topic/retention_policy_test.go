package topic

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/stretchr/testify/require"
)

func TestTopicPolicyConfiguresPartitionRetention(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.SegmentRollTimeMS = 0

	diskManager := disk.NewDiskManager(cfg)
	defer diskManager.CloseAllHandlers()
	manager := NewTopicManager(cfg, diskManager, nil)

	initial := DefaultPolicy()
	initial.RetentionHours = 24
	initial.RetentionBytes = 4 << 20
	require.NoError(t, manager.CreateTopicWithPolicy("orders", 1, false, false, initial))

	handlerValue, err := diskManager.GetHandler("orders", 0)
	require.NoError(t, err)
	handler := handlerValue.(*disk.DiskHandler)
	hours, bytes := handler.RetentionPolicy()
	require.Equal(t, 24, hours)
	require.Equal(t, int64(4<<20), bytes)

	updated := initial
	updated.RetentionHours = 48
	updated.RetentionBytes = 8 << 20
	require.NoError(t, manager.CreateTopicWithPolicy("orders", 1, false, false, updated))
	hours, bytes = handler.RetentionPolicy()
	require.Equal(t, 48, hours)
	require.Equal(t, int64(8<<20), bytes)

	expanded := updated
	expanded.RetentionHours = 72
	expanded.RetentionBytes = 16 << 20
	require.NoError(t, manager.CreateTopicWithPolicy("orders", 2, false, false, expanded))

	for partition := 0; partition < 2; partition++ {
		value, getErr := diskManager.GetHandler("orders", partition)
		require.NoError(t, getErr)
		partitionHandler := value.(*disk.DiskHandler)
		hours, bytes = partitionHandler.RetentionPolicy()
		require.Equal(t, 72, hours)
		require.Equal(t, int64(16<<20), bytes)
	}

	topic := manager.GetTopic("orders")
	for _, partition := range topic.Partitions {
		partition.Close()
	}
}
