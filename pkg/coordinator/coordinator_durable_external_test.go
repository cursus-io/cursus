package coordinator_test

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
)

func TestCoordinator_DurableOffsetReplayFromDisk(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.IndexSize = 1024

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	cd := coordinator.NewCoordinator(ctx, cfg, tm)

	require.NoError(t, cd.RegisterGroup("topic1", "groupA", 2))
	require.NoError(t, cd.RegisterGroup("topic1", "groupB", 2))
	require.NoError(t, cd.CommitOffset("groupA", "topic1", 0, 7))
	require.NoError(t, cd.CommitOffset("groupA", "topic1", 1, 11))
	require.NoError(t, cd.CommitOffset("groupB", "topic1", 0, 3))
	require.ErrorContains(t, cd.CommitOffset("groupA", "topic1", 0, 6), "offset regression")
	dm.CloseAllHandlers()

	restartedDM := disk.NewDiskManager(cfg)
	restartedTM := topic.NewTopicManager(cfg, restartedDM, nil)
	require.NoError(t, restartedTM.RestoreTopics())
	restarted := coordinator.NewCoordinator(ctx, cfg, restartedTM)
	defer restartedDM.CloseAllHandlers()

	offset, ok := restarted.GetOffset("groupA", "topic1", 0)
	require.True(t, ok)
	require.Equal(t, uint64(7), offset)

	offset, ok = restarted.GetOffset("groupA", "topic1", 1)
	require.True(t, ok)
	require.Equal(t, uint64(11), offset)

	offset, ok = restarted.GetOffset("groupB", "topic1", 0)
	require.True(t, ok)
	require.Equal(t, uint64(3), offset)

	require.ErrorContains(t, restarted.CommitOffset("groupA", "topic1", 0, 6), "offset regression")
	offset, ok = restarted.GetOffset("groupA", "topic1", 0)
	require.True(t, ok)
	require.Equal(t, uint64(7), offset)
}
