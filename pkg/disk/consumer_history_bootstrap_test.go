package disk

import (
	"context"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryBootstrapPreservesCompactedCommittedState(t *testing.T) {
	const history = 100
	cfg := consumerHistoryDiskConfig(t, t.TempDir())
	source := openConsumerHistoryDisks(t, cfg, history, true)
	c := historyDiskCoordinator(t, cfg, source)
	for p := range source.handlers {
		_, _, _, err := certifyAndRewriteHistorySegment(t, c, source, p, nil)
		require.NoError(t, err)
	}
	targetCfg := consumerHistoryDiskConfig(t, t.TempDir())
	target := openConsumerHistoryDisks(t, targetCfg, history, false)
	var transferred int64
	for p, d := range source.handlers {
		generation, err := d.CaptureContentGeneration()
		require.NoError(t, err)
		fence := BootstrapFence{Topic: config.ConsumerOffsetsTopicName, Partition: p, ReceiverID: "replacement", SourceID: "source", LeaderID: "source", LeaderEpoch: 3, LifecycleEpoch: 1, CommittedHWM: source.end, RecoveryProtocol: 2}
		export, err := d.ExportRetainedBootstrapContext(context.Background(), generation, 0, 0, source.end, fence)
		require.NoError(t, err)
		require.True(t, export.Manifest().Compacted)
		before, err := target.handlers[p].CaptureContentGeneration()
		require.NoError(t, err)
		receiver := bootstrapTestReceive(t, target.handlers[p], export)
		for _, file := range export.Manifest().Files {
			transferred += file.Size
		}
		require.NoError(t, receiver.Install())
		require.NoError(t, target.handlers[p].CompleteBootstrap(export.Manifest().TransferID))
		require.ErrorIs(t, target.handlers[p].CheckContentGeneration(before), ErrStaleContentGeneration)
		require.NoError(t, export.Close())
		require.Equal(t, source.end, target.handlers[p].GetAbsoluteOffset(), "bootstrap must exclude uncommitted tail")
	}
	t.Logf("local bootstrap logical transferred bytes=%d; network transfer not exercised", transferred)
	closeConsumerHistoryDisks(t, target)
	target = openConsumerHistoryDisks(t, targetCfg, history, false)
	recovered := historyDiskCoordinator(t, targetCfg, target)
	prepared, err := recovered.PrepareFullConsumerMetadataReplay(target)
	require.NoError(t, err)
	require.NoError(t, recovered.ApplyFullConsumerMetadataReplayAndComplete(prepared))
	for p := 0; p < 4; p++ {
		require.Equal(t, uint64(history), recovered.ExportState()[fmt.Sprintf("g%d", p)].Offsets["events"][0])
	}
	require.Equal(t, 8, target.records)
}
