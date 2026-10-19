package topic

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type checkpointCounts struct{ opens, writes, bytes, syncs, closes, renames, dirs int }

type checkpointProbe struct {
	osCheckpointIO
	counts checkpointCounts
	fail   string
	after  func(string)
}

func (p *checkpointProbe) step(stage string) error {
	if p.after != nil {
		p.after(stage)
	}
	if p.fail == stage {
		return errors.New("injected " + stage)
	}
	return nil
}
func (p *checkpointProbe) open(path string) (checkpointFile, error) {
	p.counts.opens++
	if err := p.step("open"); err != nil {
		return nil, err
	}
	f, err := p.osCheckpointIO.open(path)
	return &checkpointProbeFile{checkpointFile: f, probe: p}, err
}
func (p *checkpointProbe) replace(tmp, path string) error {
	p.counts.renames++
	if err := p.step("rename"); err != nil {
		return err
	}
	err := p.osCheckpointIO.replace(tmp, path)
	if err == nil {
		err = p.step("renamed")
	}
	return err
}
func (p *checkpointProbe) syncDir(path string) error {
	p.counts.dirs++
	if err := p.step("dir"); err != nil {
		return err
	}
	return p.osCheckpointIO.syncDir(path)
}

type checkpointProbeFile struct {
	checkpointFile
	probe *checkpointProbe
}

func (f *checkpointProbeFile) Write(data []byte) (int, error) {
	f.probe.counts.writes++
	if err := f.probe.step("write"); err != nil {
		return 0, err
	}
	n, err := f.checkpointFile.Write(data)
	f.probe.counts.bytes += n
	return n, err
}
func (f *checkpointProbeFile) Sync() error {
	f.probe.counts.syncs++
	if err := f.probe.step("sync"); err != nil {
		return err
	}
	return f.checkpointFile.Sync()
}
func (f *checkpointProbeFile) Close() error {
	f.probe.counts.closes++
	err := f.checkpointFile.Close()
	if err != nil {
		return err
	}
	return f.probe.step("close")
}

// No maintenance goroutines: deterministic file-operation counts, with actual
// filesystem writes and syncs rather than a mocked persistence implementation.
func checkpointFixture(t *testing.T) (*Partition, *checkpointProbe) {
	t.Helper()
	dir := t.TempDir()
	probe := &checkpointProbe{}
	p := &Partition{hwmCheckpointPath: filepath.Join(dir, "p.hwm"), hwmAuthoritative: true,
		messageNotifyCh:   make(chan struct{}),
		producerStatePath: filepath.Join(dir, "p.producers"), producerStateIndex: make(map[string]producerStateCheckpointEntry),
		checkpointIO: probe, closeCh: make(chan struct{}), hwmCheckpointCh: make(chan struct{}, 1), producerStateCh: make(chan struct{}, 1)}
	return p, probe
}

type checkpointStorage struct {
	types.StorageHandler
	flushes int
}

func (s *checkpointStorage) Flush()                 { s.flushes++ }
func (s *checkpointStorage) GetFirstOffset() uint64 { return 0 }

func TestCheckpointUnchangedFlushHasZeroFileOperations(t *testing.T) {
	p, probe := checkpointFixture(t)
	storage := &checkpointStorage{}
	p.dh = storage
	p.HWM = 12
	p.producerStateIndex["p"] = producerStateCheckpointEntry{Epoch: 3, Seq: 12, Offset: 11}
	p.FlushDisk()
	before := probe.counts
	require.Equal(t, 2, before.writes)
	require.Equal(t, 2, before.syncs)
	require.Equal(t, 2, before.renames)
	require.Equal(t, 2, before.dirs)
	hwmBytes, err := os.ReadFile(p.hwmCheckpointPath)
	require.NoError(t, err)
	producerBytes, err := os.ReadFile(p.producerStatePath)
	require.NoError(t, err)
	require.Equal(t, len(hwmBytes)+len(producerBytes), before.bytes)
	for i := 0; i < 100; i++ {
		p.FlushDisk()
	}
	require.Equal(t, before, probe.counts, "unchanged state must not open/write/sync/close/rename/syncDir")
	require.Equal(t, 101, storage.flushes, "data durability barrier must still run")
	p.SetHWM(13)
	p.FlushDisk()
	require.Equal(t, before.writes+1, probe.counts.writes, "only HWM changed")
	p.producerStateCoveredOffset = 13
	p.FlushDisk()
	require.Equal(t, before.writes+2, probe.counts.writes, "coverage alone changes the producer checkpoint")
}

func TestCheckpointFailureRetriesEveryPublicationStage(t *testing.T) {
	for _, kind := range []string{"hwm", "producer"} {
		for _, stage := range []string{"open", "write", "sync", "close", "rename", "dir"} {
			t.Run(kind+"/"+stage, func(t *testing.T) {
				p, probe := checkpointFixture(t)
				save := p.persistHWMCheckpoint
				change := func(n uint64) { p.HWM = n }
				path := p.hwmCheckpointPath
				if kind == "producer" {
					save = p.persistProducerStateCheckpoint
					change = func(n uint64) { p.producerStateCoveredOffset = n }
					path = p.producerStatePath
				}
				change(1)
				require.NoError(t, save())
				old, err := os.ReadFile(path)
				require.NoError(t, err)
				change(2)
				probe.fail = stage
				require.ErrorContains(t, save(), "injected "+stage)
				probe.fail = ""
				// Also exercise reverting to the previously saved state after a
				// rename succeeded but directory sync failed.
				change(1)
				require.NoError(t, save())
				got, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, old, got)
				change(2)
				require.NoError(t, save())
				before := probe.counts
				require.NoError(t, save())
				require.Equal(t, before, probe.counts)
			})
		}
	}
}

func TestCheckpointConcurrentProducerUpdateSurvivesPublication(t *testing.T) {
	p, probe := checkpointFixture(t)
	p.producerStateIndex["p"] = producerStateCheckpointEntry{Epoch: 1, Seq: 1}
	entered, resume := make(chan struct{}), make(chan struct{})
	probe.after = func(stage string) {
		if stage == "write" {
			close(entered)
			<-resume
		}
	}
	done := make(chan error, 1)
	go func() { done <- p.persistProducerStateCheckpoint() }()
	<-entered
	p.producerStateMu.Lock()
	p.producerStateIndex["p"] = producerStateCheckpointEntry{Epoch: 2, Seq: 1, Offset: 1}
	p.producerStateCoveredOffset = 2
	p.producerStateMu.Unlock()
	close(resume)
	require.NoError(t, <-done)
	probe.after = nil
	require.NoError(t, p.persistProducerStateCheckpoint())
	data, err := os.ReadFile(p.producerStatePath)
	require.NoError(t, err)
	cp, err := decodeProducerStateCheckpoint(data)
	require.NoError(t, err)
	require.Equal(t, int64(2), cp.Producers["p"].Epoch)
	require.Equal(t, uint64(2), cp.CoveredOffset)
	require.Equal(t, 2, probe.counts.writes)
}

func TestCheckpointConcurrentHWMNeverRegresses(t *testing.T) {
	p, probe := checkpointFixture(t)
	var previous uint64
	probe.after = func(stage string) {
		if stage == "renamed" {
			hwm, ok, _ := loadHWMCheckpoint(p.hwmCheckpointPath)
			if !ok || hwm < previous {
				t.Errorf("HWM regressed: %d -> %d", previous, hwm)
			}
			previous = hwm
		}
	}
	var wg sync.WaitGroup
	for n := 1; n <= 8; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 1; i <= 20; i++ {
				p.SetHWM(uint64(n*20 + i))
				if err := p.persistHWMCheckpoint(); err != nil {
					t.Error(err)
				}
			}
		}(n)
	}
	wg.Wait()
	require.Equal(t, uint64(180), previous)
}

func TestCheckpointHWMUpdateDuringIOIsNotBlockedOrLost(t *testing.T) {
	p, probe := checkpointFixture(t)
	p.SetHWM(1)
	entered, resume := make(chan struct{}), make(chan struct{})
	probe.after = func(stage string) {
		if stage == "write" {
			close(entered)
			<-resume
		}
	}
	done := make(chan error, 1)
	go func() { done <- p.persistHWMCheckpoint() }()
	<-entered
	updated := make(chan struct{})
	go func() { p.SetHWM(2); close(updated) }()
	select {
	case <-updated:
	case <-time.After(time.Second):
		close(resume)
		<-done
		<-updated
		t.Fatal("partition updates blocked behind checkpoint I/O")
	}
	close(resume)
	require.NoError(t, <-done)
	probe.after = nil
	require.NoError(t, p.persistHWMCheckpoint())
	hwm, ok, _ := loadHWMCheckpoint(p.hwmCheckpointPath)
	require.True(t, ok)
	require.Equal(t, uint64(2), hwm)
	require.Equal(t, 2, probe.counts.writes)
}

func TestCheckpointSnapshotDoesNotPublishTemporaryVisibility(t *testing.T) {
	p, probe := checkpointFixture(t)
	p.HWM = 10
	p.LEO.Store(10)
	require.NoError(t, p.persistHWMCheckpoint())
	require.NoError(t, p.ReconcileSnapshotHWM(4))
	require.NoError(t, p.persistHWMCheckpoint())
	require.Equal(t, 1, probe.counts.writes)
	hwm, ok, _ := loadHWMCheckpoint(p.hwmCheckpointPath)
	require.True(t, ok)
	require.Equal(t, uint64(10), hwm)
	require.NoError(t, p.FinalizeSnapshotRecovery(10))
	require.NoError(t, p.persistHWMCheckpoint())
	require.Equal(t, 1, probe.counts.writes)
}

func TestCheckpointSnapshotFinalizationReschedulesDeferredSave(t *testing.T) {
	p, probe := checkpointFixture(t)
	p.HWM = 10
	p.LEO.Store(10)
	probe.fail = "open"
	require.Error(t, p.persistHWMCheckpoint())
	require.NoError(t, p.ReconcileSnapshotHWM(4))
	require.NoError(t, p.persistHWMCheckpoint(), "temporary visibility cannot be published")
	probe.fail = ""
	require.NoError(t, p.FinalizeSnapshotRecovery(10))
	require.Len(t, p.hwmCheckpointCh, 1, "same final HWM must reschedule the pre-snapshot failed save")
	require.NoError(t, p.persistHWMCheckpoint())
	hwm, ok, _ := loadHWMCheckpoint(p.hwmCheckpointPath)
	require.True(t, ok)
	require.Equal(t, uint64(10), hwm)
}

func TestCheckpointShutdownRestartPreservesChangedProducerState(t *testing.T) {
	cfg := checkpointDiskConfig(t.TempDir())
	dh, err := disk.NewDiskHandler(cfg, "shutdown", 0)
	require.NoError(t, err)
	p := NewPartition(0, "shutdown", dh, nil, cfg)
	p.StartProducerStateMaintenance()
	require.NoError(t, p.EnqueueSyncIdempotent(types.Message{ProducerID: "p", Epoch: 2, SeqNum: 1, Payload: "saved"}))
	p.FlushDisk()
	require.NoError(t, p.EnqueueSyncIdempotent(types.Message{ProducerID: "p", Epoch: 2, SeqNum: 2, Payload: "shutdown"}))
	p.Close()
	require.NoError(t, dh.Close())
	dh, err = disk.NewDiskHandler(cfg, "shutdown", 0)
	require.NoError(t, err)
	p = NewPartition(0, "shutdown", dh, nil, cfg)
	p.StartProducerStateMaintenance()
	t.Cleanup(func() { p.Close(); require.NoError(t, dh.Close()) })
	require.Equal(t, uint64(2), p.GetHWM())
	entry, ok := p.lookupProducerState("p")
	require.True(t, ok)
	require.Equal(t, uint64(2), entry.Seq)
	stamp := time.Unix(1000000000, 0)
	for _, path := range []string{p.hwmCheckpointPath, p.producerStatePath} {
		require.NoError(t, os.Chtimes(path, stamp, stamp))
	}
	p.FlushDisk()
	p.Close()
	for _, path := range []string{p.hwmCheckpointPath, p.producerStatePath} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, stamp, info.ModTime(), "validated loaded state should not be rewritten")
	}
}

type checkpointRecoveryStorage struct {
	types.StorageHandler
	calls, records, payloadBytes int
}

func TestCheckpointRollbackFailurePreservesRetainedProducerState(t *testing.T) {
	cfg := checkpointDiskConfig(t.TempDir())
	dh, err := disk.NewDiskHandler(cfg, "rollback", 0)
	require.NoError(t, err)
	p := NewPartition(0, "rollback", dh, nil, cfg)
	t.Cleanup(func() { p.Close(); require.NoError(t, dh.Close()) })
	require.NoError(t, p.EnqueueBatchLeaderWithMode([]types.Message{
		{ProducerID: "p", Epoch: 1, SeqNum: 1, Payload: "retained"},
		{ProducerID: "p", Epoch: 1, SeqNum: 2, Payload: "divergent"},
	}, true))
	require.NoError(t, p.ApplyReplicaHWM(2))
	p.FlushDisk()
	ops := &retryCheckpointIO{}
	ops.fail.Store(true)
	p.hwmCheckpointMu.Lock()
	p.producerCheckpointMu.Lock()
	p.checkpointIO = ops
	p.producerCheckpointMu.Unlock()
	p.hwmCheckpointMu.Unlock()
	require.ErrorContains(t, p.ApplyReplicaRollback(1), "temporary open failure")
	require.Equal(t, uint64(1), p.NextOffset())
	require.Equal(t, uint64(1), p.GetHWM())
	entry, ok := p.lookupProducerState("p")
	require.True(t, ok)
	require.Equal(t, uint64(1), entry.Seq)
	ops.fail.Store(false)
	require.NoError(t, p.ApplyReplicaRollback(1), "retry at the already-truncated LEO must publish the recovered checkpoint")
	data, err := os.ReadFile(p.producerStatePath)
	require.NoError(t, err)
	cp, err := decodeProducerStateCheckpoint(data)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cp.Producers["p"].Seq)
	require.NoError(t, p.EnqueueSyncIdempotent(types.Message{ProducerID: "p", Epoch: 1, SeqNum: 1, Payload: "retained"}))
	require.Equal(t, uint64(1), p.NextOffset(), "retained sequence must remain a duplicate")
	require.NoError(t, p.EnqueueSyncIdempotent(types.Message{ProducerID: "p", Epoch: 1, SeqNum: 2, Payload: "replacement"}))
	require.Equal(t, uint64(2), p.NextOffset())
	p.FlushDisk()
}

func (s *checkpointRecoveryStorage) ReadMessages(offset uint64, count int) ([]types.Message, error) {
	msgs, err := s.StorageHandler.ReadMessages(offset, count)
	s.calls++
	s.records += len(msgs)
	for _, msg := range msgs {
		s.payloadBytes += len(msg.Payload)
	}
	return msgs, err
}

type retryCheckpointIO struct {
	osCheckpointIO
	attempts atomic.Int32
	fail     atomic.Bool
}

func (r *retryCheckpointIO) open(path string) (checkpointFile, error) {
	r.attempts.Add(1)
	if r.fail.Load() {
		return nil, errors.New("temporary open failure")
	}
	return r.osCheckpointIO.open(path)
}

type checkpointLogCounter struct {
	writer   io.Writer
	path     []byte
	warnings atomic.Int32
}

func (c *checkpointLogCounter) Write(data []byte) (int, error) {
	if bytes.Contains(data, c.path) {
		c.warnings.Add(1)
	}
	return c.writer.Write(data)
}

func TestCheckpointWarningReportsChangedErrorAndPeriodicReminder(t *testing.T) {
	var warning checkpointWarning
	now := time.Now()
	require.True(t, warning.allow("disk full", now))
	require.False(t, warning.allow("disk full", now.Add(time.Second)))
	require.True(t, warning.allow("read only", now.Add(2*time.Second)))
	require.False(t, warning.allow("read only", now.Add(31*time.Second)))
	require.True(t, warning.allow("read only", now.Add(32*time.Second)))
}

func TestCheckpointBackgroundRetriesWithoutNewMutation(t *testing.T) {
	p, _ := checkpointFixture(t)
	counter := &checkpointLogCounter{writer: log.Writer(), path: []byte(p.hwmCheckpointPath)}
	log.SetOutput(counter)
	t.Cleanup(func() { log.SetOutput(counter.writer) })
	p.dh = &checkpointStorage{}
	p.producerStatePath = ""
	ops := &retryCheckpointIO{}
	ops.fail.Store(true)
	p.checkpointIO = ops
	p.hwmCheckpointWG.Add(1)
	go p.runHWMCheckpointLoop()
	t.Cleanup(p.Close)
	p.SetHWM(1)
	require.Eventually(t, func() bool { return ops.attempts.Load() >= 2 }, 2*time.Second, 10*time.Millisecond)
	p.FlushDisk()
	require.Equal(t, int32(1), counter.warnings.Load(), "explicit flush and repeated background failures share the warning limit")
	ops.fail.Store(false)
	require.Eventually(t, func() bool { hwm, ok, _ := loadHWMCheckpoint(p.hwmCheckpointPath); return ok && hwm == 1 }, 2*time.Second, 10*time.Millisecond)
	ops.fail.Store(true)
	p.SetHWM(2)
	require.Eventually(t, func() bool { return counter.warnings.Load() == 2 }, 2*time.Second, 10*time.Millisecond, "success resets the warning budget for a later failure")
	ops.fail.Store(false)
}

func checkpointDiskConfig(dir string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.LogDir = dir
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	return cfg
}

// A separate process exits without Partition.Close or DiskHandler.Close, at
// real checkpoint publication boundaries. This is process-crash recovery, not
// a power-loss simulation or a claim about the host's volatile device cache.
func TestCheckpointCrashRestart(t *testing.T) {
	if dir := os.Getenv("CURSUS_CHECKPOINT_CRASH_DIR"); dir != "" {
		cfg := checkpointDiskConfig(dir)
		dh, err := disk.NewDiskHandler(cfg, "crash", 0)
		require.NoError(t, err)
		p := NewPartition(0, "crash", dh, nil, cfg)
		require.NoError(t, p.EnqueueSyncIdempotent(types.Message{ProducerID: "p", Epoch: 7, SeqNum: 1, Payload: "first"}))
		p.FlushDisk()
		p.hwmCheckpointMu.Lock()
		p.producerCheckpointMu.Lock()
		require.NoError(t, p.EnqueueSyncIdempotent(types.Message{ProducerID: "p", Epoch: 7, SeqNum: 2, Payload: "second"}))
		p.checkpointIO = &checkpointProbe{after: func(stage string) {
			if stage == os.Getenv("CURSUS_CHECKPOINT_CRASH_STAGE") {
				os.Exit(0)
			}
		}}
		p.producerCheckpointMu.Unlock()
		if os.Getenv("CURSUS_CHECKPOINT_CRASH_KIND") == "hwm" {
			p.hwmCheckpointMu.Unlock()
			require.NoError(t, p.persistHWMCheckpoint())
		} else {
			// Keep the HWM timer out of this producer-publication crash.
			require.NoError(t, p.persistProducerStateCheckpoint())
		}
		t.Fatal("crash stage was not reached")
	}
	for _, kind := range []string{"hwm", "producer"} {
		for _, stage := range []string{"sync", "rename", "renamed", "dir"} {
			t.Run(kind+"/"+stage, func(t *testing.T) {
				dir := t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointCrashRestart$")
				cmd.Env = append(os.Environ(), "CURSUS_CHECKPOINT_CRASH_DIR="+dir, "CURSUS_CHECKPOINT_CRASH_STAGE="+stage, "CURSUS_CHECKPOINT_CRASH_KIND="+kind)
				out, err := cmd.CombinedOutput()
				require.NoError(t, err, string(out))
				dh, err := disk.NewDiskHandler(checkpointDiskConfig(dir), "crash", 0)
				require.NoError(t, err)
				p := NewPartition(0, "crash", dh, nil, checkpointDiskConfig(dir))
				defer func() { p.Close(); require.NoError(t, dh.Close()) }()
				require.NoError(t, p.RecoveryError())
				require.Equal(t, uint64(2), p.NextOffset())
				wantHWM, wantSequence := uint64(1), uint64(1)
				if stage == "renamed" || stage == "dir" {
					if kind == "hwm" {
						wantHWM = 2
					} else {
						wantSequence = 2
					}
				}
				require.Equal(t, wantHWM, p.GetHWM())
				loaded, ok := p.lookupProducerState("p")
				require.True(t, ok)
				require.Equal(t, wantSequence, loaded.Seq, "publication must leave precisely the old or new complete image")
				work := &checkpointRecoveryStorage{StorageHandler: dh}
				p.dh = work
				require.NoError(t, p.RecoverProducerStateFromLog())
				require.Equal(t, 1, work.calls)
				require.Equal(t, 2, work.records)
				require.Equal(t, len("firstsecond"), work.payloadBytes, "measure reconstruction work, not only request count")
				require.NoError(t, p.EnqueueSyncIdempotent(types.Message{ProducerID: "p", Epoch: 7, SeqNum: 2, Payload: "second"}))
				require.Equal(t, uint64(2), p.NextOffset(), "restart lost producer dedup state")
				msgs, err := p.ReadMessages(0, 10)
				require.NoError(t, err)
				require.Len(t, msgs, 2)
				require.Equal(t, "first", msgs[0].Payload)
				require.Equal(t, "second", msgs[1].Payload)
				p.FlushDisk()
			})
		}
	}
}
