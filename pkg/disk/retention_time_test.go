package disk

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDurableRetentionCompactionPreservesMtimeAndTime(t *testing.T) {
	cfg := compactionTestConfig(t)
	// Small isolated fixture: test time semantics, not host disk-pressure policy.
	cfg.DiskMinFreeBytes = 0
	cfg.DiskMinFreePercent = 0
	d, err := NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	stamp := time.Unix(1700000000, 0).UnixMilli()
	appendCompactionMessage(t, d, types.Message{Key: "key", Payload: "old", AppendTimeMS: stamp})
	appendCompactionMessage(t, d, types.Message{Key: "key", Payload: "new", AppendTimeMS: stamp + 1})
	rollCompactionSegment(t, d)
	before, err := os.Stat(d.GetSegmentPath(0))
	require.NoError(t, err)
	result, err := d.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)
	after, err := os.Stat(d.GetSegmentPath(0))
	require.NoError(t, err)
	require.Equal(t, before.ModTime(), after.ModTime())
	records := bootstrapRawRecords(t, d.GetSegmentPath(0))
	require.Len(t, records, 1)
	require.Equal(t, stamp+1, records[0].AppendTimeMS)
}

func TestDurableRetentionBootstrapPublicationCrash(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	stamp := time.Unix(1700000000, 0).UnixMilli()
	records := []types.DiskMessage{{Topic: "orders", Partition: 1, AppendTimeMS: stamp, Payload: "kept"}}
	require.NoError(t, source.WriteBatchSync(records))
	export := retainedExportTest(t, source, 0, 0, 1)
	target, cfg := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, target, export)
	bootstrapAfterReplace = func(id string) error {
		if id == "records" {
			return errors.New("injected crash")
		}
		return nil
	}
	err := r.Install()
	bootstrapAfterReplace = nil
	require.ErrorContains(t, err, "injected crash")
	require.NoError(t, target.Close())
	reopened, err := NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.Equal(t, records, bootstrapRawRecords(t, reopened.GetSegmentPath(0)))
	require.NoError(t, reopened.CompleteBootstrap(export.Manifest().TransferID))
	require.NoError(t, reopened.RollSegmentAt(1))
	setRetentionTestClock(reopened, time.UnixMilli(stamp).Add(time.Hour))
	reopened.EnforceRetention(cfg)
	require.FileExists(t, reopened.GetSegmentPath(0))
	setRetentionTestClock(reopened, time.UnixMilli(stamp).Add(time.Hour+time.Millisecond))
	reopened.EnforceRetention(cfg)
	require.NoFileExists(t, reopened.GetSegmentPath(0))
}

func setRetentionTestClock(d *DiskHandler, now time.Time) {
	d.retentionGuardMu.Lock()
	if d.retentionGuard == nil {
		d.retentionGuard = func() (uint64, func(), error) { return ^uint64(0), func() {}, nil }
	}
	d.retentionGuardMu.Unlock()
	d.mu.Lock()
	d.retentionTimeEnabled = true
	d.retentionClock = func() time.Time { return now }
	d.mu.Unlock()
	d.SetRetentionPolicy(1, -1)
}

func TestDurableRetentionRecoveryGranularity(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	source, cfg := bootstrapTestHandler(t, 1)
	for i := uint64(0); i < 2; i++ {
		require.NoError(t, source.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Partition: 1, Offset: i, Payload: "same", AppendTimeMS: t0.Add(time.Duration(i) * time.Hour).UnixMilli()}}))
		require.NoError(t, source.RollSegmentAt(i+1))
	}
	incremental, _ := bootstrapTestHandler(t, 1)
	messages, err := source.ReadMessages(0, 2)
	require.NoError(t, err)
	for i := range messages {
		require.NoError(t, incremental.AppendMessageWithOffset("orders", 1, &messages[i]))
		require.NoError(t, incremental.RollSegmentAt(uint64(i+1)))
	}
	export := retainedExportTest(t, source, 0, 0, 2)
	bootstrap, _ := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, bootstrap, export)
	require.NoError(t, r.Install())
	require.NoError(t, bootstrap.CompleteBootstrap(export.Manifest().TransferID))
	require.NoError(t, bootstrap.RollSegmentAt(2))
	for name, d := range map[string]*DiskHandler{"original": source, "incremental": incremental, "bootstrap": bootstrap} {
		read, err := d.ReadMessages(0, 2)
		require.NoError(t, err)
		require.Equal(t, messages, read)
		setRetentionTestClock(d, t0.Add(90*time.Minute))
		d.EnforceRetention(cfg)
		if name == "bootstrap" {
			require.Equal(t, uint64(0), d.GetFirstOffset())
		} else {
			require.Equal(t, uint64(1), d.GetFirstOffset())
		}
		setRetentionTestClock(d, t0.Add(2*time.Hour))
		d.EnforceRetention(cfg) // equality does not expire the newest record
		require.Less(t, d.GetFirstOffset(), uint64(2))
		before := d.RetentionTimeWorkSnapshot()
		d.EnforceRetention(cfg)
		after := d.RetentionTimeWorkSnapshot()
		require.Equal(t, before.ReadBytes, after.ReadBytes, "warm tick reread log")
		setRetentionTestClock(d, t0.Add(2*time.Hour+time.Millisecond))
		d.EnforceRetention(cfg)
		require.Equal(t, uint64(2), d.GetFirstOffset(), name)
		t.Logf("%s reconstruction=%+v export=%+v", name, d.RetentionTimeWorkSnapshot(), export.RetainedWork())
	}
}

func TestDurableRetentionLegacyFutureAndProtection(t *testing.T) {
	for _, kind := range []string{"legacy", "mixed", "future", "event", "committed"} {
		t.Run(kind, func(t *testing.T) {
			d, cfg := bootstrapTestHandler(t, 1)
			now := time.Unix(1700000000, 0)
			records := []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 0, AppendTimeMS: now.Add(-2 * time.Hour).UnixMilli()}}
			switch kind {
			case "legacy":
				records[0].AppendTimeMS = 0
			case "mixed":
				records = append(records, types.DiskMessage{Topic: "orders", Partition: 1, Offset: 1})
			case "future":
				records[0].AppendTimeMS = now.Add(24 * time.Hour).UnixMilli()
			case "event":
				records[0].EventType = "event"
				records[0].AggregateVersion = 1
			case "committed":
				d.SetRetentionGuard(func() (uint64, func(), error) { return 0, func() {}, nil })
			}
			require.NoError(t, d.WriteBatchSync(records))
			require.NoError(t, d.RollSegmentAt(uint64(len(records))))
			setRetentionTestClock(d, now)
			d.EnforceRetention(cfg)
			require.FileExists(t, d.GetSegmentPath(0))
			d.SetRetentionPolicy(-1, 1)
			d.EnforceRetention(cfg)
			if kind == "event" || kind == "committed" {
				require.FileExists(t, d.GetSegmentPath(0))
			} else {
				require.NoFileExists(t, d.GetSegmentPath(0))
			}
		})
	}
}

func TestDurableRetentionRestartAndCorruptEvidence(t *testing.T) {
	d, cfg := bootstrapTestHandler(t, 1)
	now := time.Unix(1700000000, 0)
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Partition: 1, AppendTimeMS: now.UnixMilli()}}))
	require.NoError(t, d.RollSegmentAt(1))
	setRetentionTestClock(d, now)
	d.EnforceRetention(cfg)
	require.Positive(t, d.RetentionTimeWorkSnapshot().ReadBytes)
	require.NoError(t, d.Close())
	reopened, err := NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	setRetentionTestClock(reopened, now)
	reopened.EnforceRetention(cfg)
	require.Positive(t, reopened.RetentionTimeWorkSnapshot().ReadBytes)
	path := reopened.GetSegmentPath(0)
	require.NoError(t, os.Chmod(path, 0600))
	bytes, err := os.ReadFile(path)
	require.NoError(t, err)
	bytes[len(bytes)-1] ^= 1
	require.NoError(t, os.WriteFile(path, bytes, 0600))
	setRetentionTestClock(reopened, now.Add(3*time.Hour))
	reopened.EnforceRetention(cfg)
	require.FileExists(t, path)
	require.Positive(t, reopened.RetentionTimeWorkSnapshot().InvalidSegments)
}

func TestDurableRetentionBackupRestoreAndMissingIndex(t *testing.T) {
	d, cfg := bootstrapTestHandler(t, 1)
	stamp := time.Unix(1700000000, 0)
	require.NoError(t, d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Partition: 1, AppendTimeMS: stamp.UnixMilli(), Payload: "backup"}}))
	require.NoError(t, d.RollSegmentAt(1))
	setRetentionTestClock(d, stamp)
	d.EnforceRetention(cfg)
	require.NoError(t, d.Close())
	// A cold backup of this isolated, closed fixture. Copying creates new mtimes.
	backupDir := t.TempDir()
	require.NoError(t, filepath.WalkDir(cfg.LogDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(cfg.LogDir, path)
		if err != nil {
			return err
		}
		target := filepath.Join(backupDir, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.Create(target)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(output, input)
		return errors.Join(copyErr, output.Close())
	}))
	copyCfg := *cfg
	copyCfg.LogDir = backupDir
	restored, err := NewDiskHandler(&copyCfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, restored.Close()) }()
	// Time evidence never consults the closed segment's derived offset index.
	index := restored.GetSegmentPath(0)
	index = index[:len(index)-4] + ".index"
	require.NoError(t, os.Chmod(index, 0600))
	require.NoError(t, os.WriteFile(index, []byte("corrupt derived index"), 0600))
	setRetentionTestClock(restored, stamp.Add(time.Hour))
	restored.EnforceRetention(&copyCfg)
	require.FileExists(t, restored.GetSegmentPath(0))
	require.NoError(t, os.Remove(index))
	restored.mu.Lock()
	restored.retentionTimes = nil
	restored.mu.Unlock()
	setRetentionTestClock(restored, stamp.Add(time.Hour+time.Millisecond))
	restored.EnforceRetention(&copyCfg)
	require.NoFileExists(t, restored.GetSegmentPath(0))
	require.Equal(t, uint64(2), restored.RetentionTimeWorkSnapshot().Records)
}

func TestDurableRetentionDoesNotWaitForCompactionUnderPartitionGuard(t *testing.T) {
	d, cfg := bootstrapTestHandler(t, 1)
	released := make(chan struct{})
	d.SetRetentionGuard(func() (uint64, func(), error) {
		return 0, func() { close(released) }, nil
	})
	d.maintenanceMu.Lock()
	defer d.maintenanceMu.Unlock()
	go d.EnforceRetention(cfg)
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("retention held the partition guard while waiting for compaction")
	}
}
