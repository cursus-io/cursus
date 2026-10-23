package disk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerHistoryCancellationAtDurableStages(t *testing.T) {
	for _, stage := range []string{"written", "log_synced", "index_synced", "marker_synced", "generation_invalidated", "marker_published", "log_published", "index_published", "directory_synced"} {
		t.Run(stage, func(t *testing.T) {
			cfg := consumerHistoryDiskConfig(t, t.TempDir())
			r := openConsumerHistoryDisks(t, cfg, 10, true)
			c := historyDiskCoordinator(t, cfg, r)
			d := r.handlers[0]
			generation, err := d.CaptureContentGeneration()
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reached := false
			removed, _, _, err := certifyAndRewriteHistorySegment(t, c, r, 0, func(current string) error {
				if current == stage {
					reached = true
					cancel()
				}
				return consumerHistoryRewriteStage(ctx, current)
			})
			require.True(t, reached)
			switch stage {
			case "written", "log_synced", "index_synced", "marker_synced":
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, removed)
				require.NoError(t, d.CheckContentGeneration(generation))
			default:
				require.NoError(t, err)
				require.Equal(t, 9, removed)
				require.ErrorIs(t, d.CheckContentGeneration(generation), ErrStaleContentGeneration)
			}
			require.NoError(t, d.writeAvailabilityError(), "maintenance cancellation must not fence healthy writes")
			assertHistoryDiskRecovery(t, cfg, r, 10)
			require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: config.ConsumerOffsetsTopicName, Offset: r.end + 1, Payload: "another uncommitted tail"}}))
			closeConsumerHistoryDisks(t, r)
			reopened := openConsumerHistoryDisks(t, cfg, 10, false)
			recovered := historyDiskCoordinator(t, cfg, reopened)
			replay, err := recovered.PrepareFullConsumerMetadataReplay(reopened)
			require.NoError(t, err)
			require.NoError(t, recovered.ApplyFullConsumerMetadataReplayAndComplete(replay))
			for partition := range reopened.handlers {
				require.Equal(t, uint64(10), recovered.ExportState()[fmt.Sprintf("g%d", partition)].Offsets["events"][0])
			}
			require.Equal(t, r.end+2, reopened.handlers[0].GetAbsoluteOffset())
			tail, err := reopened.handlers[0].ReadMessages(r.end, 2)
			require.NoError(t, err)
			require.Len(t, tail, 2)
			require.Equal(t, "uncommitted tail", tail[0].Payload)
			require.Equal(t, "another uncommitted tail", tail[1].Payload)
		})
	}
}

func TestConsumerHistoryPublicationFailureStillFencesWrites(t *testing.T) {
	cfg := consumerHistoryDiskConfig(t, t.TempDir())
	r := openConsumerHistoryDisks(t, cfg, 10, true)
	c := historyDiskCoordinator(t, cfg, r)
	failure := errors.New("injected storage publication failure")
	_, _, _, err := certifyAndRewriteHistorySegment(t, c, r, 0, func(stage string) error {
		if stage == "log_published" {
			return failure
		}
		return nil
	})
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, r.handlers[0].writeAvailabilityError(), failure)
	closeConsumerHistoryDisks(t, r)
	reopened := openConsumerHistoryDisks(t, cfg, 10, false)
	assertHistoryDiskRecovery(t, cfg, reopened, 10)
	require.NoError(t, reopened.handlers[0].writeAvailabilityError())
}

func TestConsumerHistoryInstallerRejectsStaleOrForgedRemoval(t *testing.T) {
	cfg := consumerHistoryDiskConfig(t, t.TempDir())
	r := openConsumerHistoryDisks(t, cfg, 10, true)
	c := historyDiskCoordinator(t, cfg, r)
	d := r.handlers[0]
	segments, err := d.ConsumerHistorySegments(context.Background(), r.end)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 0, r.end)
	require.NoError(t, err)
	removed := plan.RemovedRecords()
	require.NotEmpty(t, removed)
	removed[0].Payload = "forged"
	_, err = d.RewriteConsumerHistory(context.Background(), segments[0], removed)
	require.ErrorContains(t, err, "does not match original")
	assertHistoryDiskRecovery(t, cfg, r, 10)
	_, err = r.handlers[1].RewriteConsumerHistory(context.Background(), segments[0], plan.RemovedRecords())
	require.ErrorIs(t, err, ErrStaleContentGeneration)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = d.RewriteConsumerHistory(ctx, segments[0], plan.RemovedRecords())
	require.ErrorIs(t, err, context.Canceled)
	result, err := d.RewriteConsumerHistory(context.Background(), segments[0], plan.RemovedRecords())
	require.NoError(t, err)
	require.Positive(t, result.RecordsRemoved)
	_, err = d.RewriteConsumerHistory(context.Background(), segments[0], plan.RemovedRecords())
	require.ErrorIs(t, err, ErrStaleContentGeneration)
	assertHistoryDiskRecovery(t, cfg, r, 10)
}

func TestConsumerHistoryPruneCrashStages(t *testing.T) {
	if stage := os.Getenv("CURSUS_HISTORY_PRUNE_CRASH"); stage != "" {
		cfg := consumerHistoryDiskConfig(t, os.Getenv("CURSUS_HISTORY_PRUNE_DIR"))
		d, err := NewDiskHandler(cfg, config.ConsumerOffsetsTopicName, 0)
		require.NoError(t, err)
		d.maintenanceMu.Lock()
		err = d.pruneEmptyConsumerHistorySegment(1, func(current string) error {
			if current == stage {
				os.Exit(77)
			}
			return nil
		})
		require.NoError(t, err)
		t.Fatal("crash stage was not reached")
	}
	for _, stage := range []string{"predecessor_gap_synced", "empty_segment_deleted"} {
		t.Run(stage, func(t *testing.T) {
			cfg := consumerHistoryDiskConfig(t, t.TempDir())
			d, err := NewDiskHandler(cfg, config.ConsumerOffsetsTopicName, 0)
			require.NoError(t, err)
			for i := 0; i < 3; i++ {
				_, err := d.AppendMessageSync(config.ConsumerOffsetsTopicName, 0, &types.Message{Payload: "retained scan fixture"})
				require.NoError(t, err)
				if i < 2 {
					rollCompactionSegment(t, d)
				}
			}
			d.maintenanceMu.Lock()
			_, _, _, err = d.rewriteClosedSegment(1, false, func(types.DiskMessage) bool { return true }, nil)
			d.maintenanceMu.Unlock()
			require.NoError(t, err)
			require.NoError(t, d.Close())
			command := exec.Command(os.Args[0], "-test.run=^TestConsumerHistoryPruneCrashStages$") // #nosec G204 -- reexecutes only this test binary.
			command.Env = append(os.Environ(), "CURSUS_HISTORY_PRUNE_CRASH="+stage, "CURSUS_HISTORY_PRUNE_DIR="+cfg.LogDir)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(output))
			require.Equal(t, 77, exit.ExitCode(), string(output))
			d, err = NewDiskHandler(cfg, config.ConsumerOffsetsTopicName, 0)
			require.NoError(t, err)
			defer func() { _ = d.Close() }()
			var offsets []uint64
			require.NoError(t, d.ScanRetainedRange(context.Background(), 0, 3, func(m types.Message) error {
				offsets = append(offsets, m.Offset)
				return nil
			}))
			require.Equal(t, []uint64{0, 2}, offsets)
		})
	}
}

func TestConsumerHistoryPruneCertifiesPrecedingSegmentGap(t *testing.T) {
	cfg := consumerHistoryDiskConfig(t, t.TempDir())
	d, err := NewDiskHandler(cfg, config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	for i := 0; i < 3; i++ {
		record := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: "g", Topic: "events", Epoch: 1, Revision: uint64(i), Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: uint64(i)}}}
		if i == 0 {
			record.Type, record.PartitionCount, record.Offsets = coordinator.ConsumerMetadataRecordRegistration, 1, nil
		}
		payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
		require.NoError(t, err)
		require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: config.ConsumerOffsetsTopicName, Offset: uint64(i), Key: key, Payload: string(payload), ProducerID: "consumer-offset-coordinator"}}))
		if i < 2 {
			rollCompactionSegment(t, d)
		}
	}
	r := &consumerHistoryDiskReader{handlers: []*DiskHandler{d}, end: 3}
	for p := 1; p < 4; p++ {
		other, err := NewDiskHandler(cfg, config.ConsumerOffsetsTopicName, p)
		require.NoError(t, err)
		t.Cleanup(func() { _ = other.Close() })
		r.handlers = append(r.handlers, other)
	}
	c := historyDiskCoordinator(t, cfg, r)
	segments, err := d.ConsumerHistorySegments(context.Background(), r.end)
	require.NoError(t, err)
	require.Len(t, segments, 2)
	plan, err := c.PrepareConsumerHistoryCompaction(r, 0, 1, 2)
	require.NoError(t, err)
	result, err := d.RewriteConsumerHistory(context.Background(), segments[1], plan.RemovedRecords())
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)
	require.NoError(t, d.Close())
	reopened, err := NewDiskHandler(cfg, config.ConsumerOffsetsTopicName, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	var offsets []uint64
	require.NoError(t, reopened.ScanRetainedRange(context.Background(), 0, 3, func(m types.Message) error {
		offsets = append(offsets, m.Offset)
		return nil
	}))
	require.Equal(t, []uint64{0, 2}, offsets)
}
