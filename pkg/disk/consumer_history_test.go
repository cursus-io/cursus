package disk

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// This adapter is TEST ONLY. Production integration must use fixed partition
// read views, durable activation and FSM fencing; a disk handler alone cannot
// establish those contracts. The fixtures own all logs and commit ends.
type consumerHistoryDiskReader struct {
	handlers []*DiskHandler
	end      uint64
	records  int
	bytes    int
	syncs    int
}

func (r *consumerHistoryDiskReader) Publish(string, *types.Message) error {
	return fmt.Errorf("fixture is read only")
}
func (r *consumerHistoryDiskReader) CreateTopic(string, int, bool, bool) error { return nil }
func (r *consumerHistoryDiskReader) ExistingPartitionCount(string) (int, error) {
	return len(r.handlers), nil
}
func (r *consumerHistoryDiskReader) EarliestTopicOffset(_ string, p int) (uint64, error) {
	return r.handlers[p].GetFirstOffset(), nil
}
func (r *consumerHistoryDiskReader) EndTopicOffset(string, int) (uint64, error) {
	return r.end, nil
}
func (r *consumerHistoryDiskReader) ReadCommittedConsumerMetadata(_ string, p int, next uint64, max int) ([]types.Message, error) {
	messages, err := r.handlers[p].ReadMessages(next, max)
	if err != nil {
		return nil, err
	}
	for i, m := range messages {
		if m.Offset >= r.end {
			messages = messages[:i]
			break
		}
	}
	for _, m := range messages {
		r.records++
		r.bytes += len(m.Key) + len(m.Payload)
	}
	return messages, nil
}
func (r *consumerHistoryDiskReader) ReadRawConsumerMetadataIndex(p int, next uint64, max int) ([]types.Message, error) {
	return r.ReadCommittedConsumerMetadata("", p, next, max)
}
func (r *consumerHistoryDiskReader) ConsumerMetadataIndexIdentity(p int) (string, error) {
	generation, err := r.handlers[p].CleanupGeneration()
	return fmt.Sprintf("fixture-incarnation:1:%d:%s", p, generation), err
}
func (r *consumerHistoryDiskReader) SyncConsumerMetadataIndex() error {
	for _, d := range r.handlers {
		if err := d.SyncCleanupCheckpointContext(context.Background()); err != nil {
			return err
		}
		r.syncs++
	}
	return nil
}

func consumerHistoryDiskConfig(t *testing.T, dir string) *config.Config {
	t.Helper()
	cfg := compactionTestConfig(t)
	cfg.LogDir = dir
	cfg.EnabledDistribution = true
	cfg.DiskMinFreePercent = 0
	cfg.SegmentSize, cfg.IndexSize = 64<<20, 64<<20
	return cfg
}

func openConsumerHistoryDisks(t *testing.T, cfg *config.Config, history int, populate bool) *consumerHistoryDiskReader {
	t.Helper()
	r := &consumerHistoryDiskReader{end: uint64(history + 1)}
	for p := 0; p < 4; p++ {
		d, err := NewDiskHandler(cfg, config.ConsumerOffsetsTopicName, p)
		require.NoError(t, err)
		t.Cleanup(func() { _ = d.Close() })
		r.handlers = append(r.handlers, d)
		if !populate {
			continue
		}
		var batch []types.DiskMessage
		for i := 0; i <= history; i++ {
			record := coordinator.ConsumerMetadataRecord{Version: 1, Type: coordinator.ConsumerMetadataRecordOffsetSnapshot, Group: fmt.Sprintf("g%d", p), Topic: "events", Epoch: 1, Revision: uint64(i), Offsets: []coordinator.OffsetItem{{Partition: 0, Offset: uint64(i)}}}
			if i == 0 {
				record.Type, record.Revision, record.Offsets, record.PartitionCount = coordinator.ConsumerMetadataRecordRegistration, 0, nil, 1
			}
			payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
			require.NoError(t, err)
			batch = append(batch, types.DiskMessage{Topic: config.ConsumerOffsetsTopicName, Partition: int32(p), Offset: uint64(i), Key: key, Payload: string(payload)})
		}
		require.NoError(t, d.WriteBatchSync(batch))
		rollCompactionSegment(t, d)
		// The active physical tail is deliberately invalid metadata, and lies
		// outside the authoritative committed cut. Neither replay nor cleaning
		// may decode, remove or accidentally certify it as committed state.
		require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: config.ConsumerOffsetsTopicName, Partition: int32(p), Offset: r.end, Payload: "uncommitted tail"}}))
	}
	return r
}

func closeConsumerHistoryDisks(t *testing.T, r *consumerHistoryDiskReader) {
	t.Helper()
	for _, d := range r.handlers {
		require.NoError(t, d.Close())
	}
}

func historyDiskCoordinator(t *testing.T, cfg *config.Config, r *consumerHistoryDiskReader) *coordinator.Coordinator {
	t.Helper()
	c, err := coordinator.NewCoordinatorAwaitingDistributedRecovery(context.Background(), cfg, r)
	require.NoError(t, err)
	t.Cleanup(c.Stop)
	release, err := c.BeginConsumerMetadataRecovery(context.Background())
	require.NoError(t, err)
	t.Cleanup(release)
	return c
}

func certifyAndRewriteHistorySegment(t *testing.T, c *coordinator.Coordinator, r *consumerHistoryDiskReader, p int, stage func(string) error) (int, int64, int64, error) {
	t.Helper()
	plan, err := c.PrepareConsumerHistoryCompaction(r, p, 0, r.end)
	require.NoError(t, err)
	require.NoError(t, c.ValidateConsumerHistoryCompaction(r, plan))
	d := r.handlers[p]
	witness, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	remove := make(map[uint64]types.Message)
	for _, m := range plan.RemovedRecords() {
		remove[m.Offset] = m
	}
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	require.NoError(t, d.CheckContentGeneration(witness))
	info, err := os.Stat(d.GetSegmentPath(0))
	require.NoError(t, err)
	return d.rewriteClosedSegment(0, d.segmentAllowsOffsetGaps(0, info.Size()), func(m types.DiskMessage) bool {
		old, ok := remove[m.Offset]
		return ok && old.Payload == m.Payload && old.Key == m.Key && m.ProducerID == "" && !hasTransactionMetadata(m)
	}, stage)
}

func assertHistoryDiskRecovery(t *testing.T, cfg *config.Config, r *consumerHistoryDiskReader, history int) {
	t.Helper()
	c := historyDiskCoordinator(t, cfg, r)
	p, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplayAndComplete(p))
	state := c.ExportState()
	for i, d := range r.handlers {
		group := fmt.Sprintf("g%d", i)
		require.Contains(t, state, group)
		value, ok := state[group].Offsets["events"][0]
		require.True(t, ok)
		require.Equal(t, uint64(history), value)
		require.Equal(t, r.end+1, d.GetAbsoluteOffset())
		tail, err := d.ReadMessages(r.end, 1)
		require.NoError(t, err)
		require.Len(t, tail, 1)
		require.Equal(t, "uncommitted tail", tail[0].Payload)
	}
}

func TestConsumerHistoryDiskReductionAndCacheMiss(t *testing.T) {
	for _, history := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			cfg := consumerHistoryDiskConfig(t, t.TempDir())
			r := openConsumerHistoryDisks(t, cfg, history, true)
			c := historyDiskCoordinator(t, cfg, r)
			start := time.Now()
			assertHistoryDiskRecovery(t, cfg, r, history)
			full := time.Since(start)
			fullRecords, fullBytes := r.records, r.bytes
			r.records, r.bytes = 0, 0
			start = time.Now()
			var removed int
			var before, after int64
			for p := range r.handlers {
				n, b, a, err := certifyAndRewriteHistorySegment(t, c, r, p, nil)
				require.NoError(t, err)
				removed, before, after = removed+n, before+b, after+a
			}
			compaction := time.Since(start)
			workRecords, workBytes := r.records, r.bytes
			require.Equal(t, 4*(history-1), removed)
			require.Less(t, after, before/10)
			closeConsumerHistoryDisks(t, r)
			r = openConsumerHistoryDisks(t, cfg, history, false)
			defer closeConsumerHistoryDisks(t, r)
			start = time.Now()
			assertHistoryDiskRecovery(t, cfg, r, history)
			replay := time.Since(start)
			require.NoFileExists(t, filepath.Join(cfg.LogDir, ".consumer-replay-index.json"))
			require.Equal(t, 8, r.records)
			t.Logf("history=%d full=%s full_records=%d full_key_payload_bytes=%d certification_and_rewrite=%s plan_records=%d plan_key_payload_bytes=%d retained_frame_bytes_before=%d after=%d reopen_replay=%s replay_records=%d replay_key_payload_bytes=%d", history, full, fullRecords, fullBytes, compaction, workRecords, workBytes, before, after, replay, r.records, r.bytes)
			// The production guard remains closed, even though this isolated
			// test has exercised the private storage replacement primitive.
			result, err := r.handlers[0].EnforceCompaction()
			require.NoError(t, err)
			require.Equal(t, "distributed_internal_metadata", result.SkippedReason)
		})
	}
}

func TestConsumerHistoryDiskCrashStages(t *testing.T) {
	const history = 20
	if stage := os.Getenv("CURSUS_HISTORY_CRASH_STAGE"); stage != "" {
		cfg := consumerHistoryDiskConfig(t, os.Getenv("CURSUS_HISTORY_CRASH_DIR"))
		r := openConsumerHistoryDisks(t, cfg, history, false)
		c := historyDiskCoordinator(t, cfg, r)
		_, _, _, err := certifyAndRewriteHistorySegment(t, c, r, 0, func(current string) error {
			if current == stage {
				os.Exit(77)
			}
			return nil
		})
		t.Fatalf("crash stage not reached: %v", err)
	}
	for _, stage := range []string{"written", "log_synced", "index_synced", "marker_synced", "generation_invalidated", "marker_published", "log_published", "index_published", "directory_synced"} {
		t.Run(stage, func(t *testing.T) {
			cfg := consumerHistoryDiskConfig(t, t.TempDir())
			r := openConsumerHistoryDisks(t, cfg, history, true)
			oldGeneration, err := r.handlers[0].CleanupGeneration()
			require.NoError(t, err)
			closeConsumerHistoryDisks(t, r)
			command := exec.Command(os.Args[0], "-test.run=^TestConsumerHistoryDiskCrashStages$") // #nosec G204 -- only reexecutes this test binary.
			command.Env = append(os.Environ(), "CURSUS_HISTORY_CRASH_STAGE="+stage, "CURSUS_HISTORY_CRASH_DIR="+cfg.LogDir)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(output))
			require.Equal(t, 77, exit.ExitCode(), string(output))
			r = openConsumerHistoryDisks(t, cfg, history, false)
			defer closeConsumerHistoryDisks(t, r)
			assertHistoryDiskRecovery(t, cfg, r, history)
			if stage == "generation_invalidated" || stage == "marker_published" || stage == "log_published" || stage == "index_published" || stage == "directory_synced" {
				generation, err := r.handlers[0].CleanupGeneration()
				require.NoError(t, err)
				require.NotEqual(t, oldGeneration, generation)
			}
		})
	}
}

func TestConsumerHistoryDiskConcurrentAppend(t *testing.T) {
	const history = 100
	cfg := consumerHistoryDiskConfig(t, t.TempDir())
	r := openConsumerHistoryDisks(t, cfg, history, true)
	c := historyDiskCoordinator(t, cfg, r)
	d := r.handlers[0]
	appended := false
	_, _, _, err := certifyAndRewriteHistorySegment(t, c, r, 0, func(stage string) error {
		if stage != "written" {
			return nil
		}
		done := make(chan error, 1)
		go func() {
			done <- d.WriteBatchSync([]types.DiskMessage{{Topic: config.ConsumerOffsetsTopicName, Offset: r.end + 1, Payload: "concurrent uncommitted tail"}})
		}()
		select {
		case err := <-done:
			appended = true
			return err
		case <-time.After(10 * time.Second):
			return fmt.Errorf("concurrent append blocked")
		}
	})
	require.NoError(t, err)
	require.True(t, appended)
	closeConsumerHistoryDisks(t, r)
	r = openConsumerHistoryDisks(t, cfg, history, false)
	defer closeConsumerHistoryDisks(t, r)
	require.Equal(t, r.end+2, r.handlers[0].GetAbsoluteOffset())
	tail, err := r.handlers[0].ReadMessages(r.end, 2)
	require.NoError(t, err)
	require.Len(t, tail, 2)
	require.Equal(t, "uncommitted tail", tail[0].Payload)
	require.Equal(t, "concurrent uncommitted tail", tail[1].Payload)
	// Authoritative recovery stops at the original committed end despite both
	// physical appends and reconstruction of the disk's larger physical tail.
	c = historyDiskCoordinator(t, cfg, r)
	prepared, err := c.PrepareFullConsumerMetadataReplay(r)
	require.NoError(t, err)
	require.NoError(t, c.ApplyFullConsumerMetadataReplayAndComplete(prepared))
	require.Equal(t, uint64(history), c.ExportState()["g0"].Offsets["events"][0])
}

func TestConsumerHistoryDiskInvalidatesReplayCache(t *testing.T) {
	const history = 20
	cfg := consumerHistoryDiskConfig(t, t.TempDir())
	cfg.ConsumerRecoveryIndexEnabled = true
	r := openConsumerHistoryDisks(t, cfg, history, true)
	defer closeConsumerHistoryDisks(t, r)
	c := historyDiskCoordinator(t, cfg, r)
	recover := func() {
		p, err := c.PrepareFullConsumerMetadataReplay(r)
		require.NoError(t, err)
		require.NoError(t, c.ApplyFullConsumerMetadataReplayAndComplete(p))
	}
	recover()
	require.Positive(t, c.RecoverySnapshot().IndexWriteBytes)
	recover()
	require.True(t, c.RecoverySnapshot().IndexHit)
	n, _, _, err := certifyAndRewriteHistorySegment(t, c, r, 0, nil)
	require.NoError(t, err)
	require.Equal(t, history-1, n)
	recover()
	require.False(t, c.RecoverySnapshot().IndexHit, "a compacted source must invalidate a previously valid cache")
	require.Equal(t, uint64(history), c.ExportState()["g0"].Offsets["events"][0])
	require.NoError(t, os.Remove(filepath.Join(cfg.LogDir, ".consumer-replay-index.json")))
	recover()
	require.False(t, c.RecoverySnapshot().IndexHit)
	require.Equal(t, uint64(history), c.ExportState()["g0"].Offsets["events"][0])
}
