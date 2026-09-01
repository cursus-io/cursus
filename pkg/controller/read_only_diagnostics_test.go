package controller_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
)

func TestGroupAndOffsetDiagnosticsAreStrictlyReadOnly(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	dm := disk.NewDiskManager(cfg)
	tm := topic.NewTopicManager(cfg, dm, nil)
	require.NoError(t, tm.RestoreTopics())
	cd, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, tm)
	require.NoError(t, err)
	tm.SetCoordinator(cd)
	require.NoError(t, tm.CreateTopic("events", 2, false, false))
	require.NoError(t, cd.RegisterGroup("events", "workers", 2))
	require.NoError(t, cd.CommitOffset("workers", "events", 0, 7))

	handler := controller.NewCommandHandler(tm, cfg, cd, nil, nil)
	t.Cleanup(func() {
		require.NoError(t, handler.Close())
		cd.Stop()
		for _, name := range tm.ListTopics() {
			for _, partition := range tm.GetTopic(name).Partitions {
				partition.Close()
			}
		}
		dm.CloseAllHandlers()
	})
	beforeState := cd.ExportState()
	beforeDefinitions := tm.ExportDefinitions()
	beforeRecovery := cd.RecoverySnapshot()
	beforeInternalOffsets := internalLogEnds(t, dm, 4)

	ctx := controller.NewClientContext("", 0)
	responses := []string{
		handler.HandleCommand("LIST_GROUPS", ctx),
		handler.HandleCommand("GROUP_STATUS group=workers", ctx),
		handler.HandleCommand("FETCH_OFFSET topic=events partition=0 group=workers", ctx),
		handler.HandleCommand("LIST_OFFSETS topic=events", ctx),
	}
	for index, response := range responses {
		require.True(t, strings.HasPrefix(response, "OK") || strings.Contains(response, `"status":"OK"`), "query %d failed: %s", index, response)
	}
	require.Equal(t, "OK offset=7", responses[2])

	require.Equal(t, beforeState, cd.ExportState())
	require.Equal(t, beforeDefinitions, tm.ExportDefinitions())
	require.Equal(t, beforeRecovery, cd.RecoverySnapshot())
	require.Equal(t, beforeInternalOffsets, internalLogEnds(t, dm, 4), "read-only diagnostics must not append internal records")
}

func internalLogEnds(t *testing.T, dm *disk.DiskManager, partitions int) []uint64 {
	t.Helper()
	result := make([]uint64, partitions)
	for partition := 0; partition < partitions; partition++ {
		handler, err := dm.GetHandler(config.ConsumerOffsetsTopicName, partition)
		require.NoError(t, err)
		result[partition] = handler.GetLatestOffset()
	}
	return result
}
