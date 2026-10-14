package topic

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func bootstrapPartitionDisk(t *testing.T, partition int) (*disk.DiskHandler, *config.Config) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 0
	cfg.DiskMinFreePercent = 0
	cfg.SegmentSize = 1 << 20
	cfg.IndexSize = 4096
	cfg.IndexIntervalBytes = 64
	cfg.SegmentRollTimeMS = 0
	cfg.RetentionCheckIntervalMS = 60000
	cfg.CompactionCheckIntervalMS = 60000
	d, err := disk.NewDiskHandler(cfg, "orders", partition)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d, cfg
}
func partitionTransfer(t *testing.T, target *disk.DiskHandler, partition int, end uint64, records []types.DiskMessage) *disk.BootstrapReceiver {
	t.Helper()
	source, _ := bootstrapPartitionDisk(t, partition)
	if end > 0 {
		require.NoError(t, source.WriteCompactedReplicaRange(0, end, records))
	}
	fence := disk.BootstrapFence{Topic: "orders", Partition: partition, ReceiverID: "receiver", SourceID: "source", LeaderID: "source", LeaderEpoch: 3, LifecycleEpoch: 7, CommittedHWM: end, RecoveryProtocol: 3}
	export, err := source.ExportBootstrap(0, end, fence, []byte(`{"snapshots":[]}`))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, export.Close()) })
	receiver, err := target.StageBootstrap(export.Manifest())
	require.NoError(t, err)
	for _, f := range export.Manifest().Files {
		for offset := int64(0); offset < f.Size; {
			chunk, err := export.ReadChunk(f.ID, offset, disk.BootstrapChunkBytes)
			require.NoError(t, err)
			require.NoError(t, receiver.WriteChunk(f.ID, offset, chunk))
			offset += int64(len(chunk))
		}
	}
	return receiver
}

func TestBootstrapSessionMissingManifestCannotEraseExistingPending(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	p.bootstrapPending.Store(true)
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	s.Close()
	require.True(t, p.BootstrapPending())
	require.Equal(t, BootstrapOutcomeUncertain, s.ClosedOutcome())
}
func TestBootstrapPendingConstructorAndOfflineRecovery(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	receiver := partitionTransfer(t, d, 0, 5, []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 2, ProducerID: "new", SeqNum: 1, Payload: "new"}})
	require.NoError(t, receiver.Install())
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	require.True(t, p.BootstrapPending())
	require.False(t, p.HWMKnown())
	require.NoError(t, p.permanentRecoveryError())
	require.ErrorIs(t, p.RecoveryError(), ErrBootstrapPending)
	_, err := p.ReadMessages(0, 10)
	require.ErrorIs(t, err, ErrBootstrapPending)
	_, _, err = p.ReadMessagesBounded(0, 10, 1000, true)
	require.ErrorIs(t, err, ErrBootstrapPending)
	_, err = p.ReadCommitted(0, 10)
	require.ErrorIs(t, err, ErrBootstrapPending)
	_, err = p.ReadCommittedRange(0, 5, 10)
	require.ErrorIs(t, err, ErrBootstrapPending)
	_, err = p.readCommittedMetadata(0, 10)
	require.ErrorIs(t, err, ErrBootstrapPending)
	_, _, err = p.ProducerSequenceOffset("new", 0, 1)
	require.ErrorIs(t, err, ErrBootstrapPending)
	_, err = p.ReadOffsetRange()
	require.ErrorIs(t, err, ErrBootstrapPending)
	require.Zero(t, p.LastStableOffset())
	p.SetHWM(99)
	require.NoError(t, p.ApplyReplicaHWM(99))
	require.NoError(t, p.ReconcileSnapshotHWM(99))
	require.NoError(t, p.FinalizeSnapshotRecovery(99))
	p.FlushDisk()
	require.Zero(t, p.GetHWM())
	require.False(t, p.HWMKnown())
	p.StartProducerStateMaintenance()
	p.StartProducerStateMaintenance()
	session, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	var escaped *PartitionReadView
	require.NoError(t, session.Complete(func(v *PartitionReadView, catalog []byte) error {
		escaped = v
		require.JSONEq(t, `{"snapshots":[]}`, string(catalog))
		require.Equal(t, uint64(5), v.CommittedEnd())
		require.False(t, p.HWMKnown())
		raw, err := v.ReadMessages(0, 10)
		require.NoError(t, err)
		require.Len(t, raw, 1)
		committed, _, _, err := v.ReadCommittedPage(0, 10, 0, true)
		require.NoError(t, err)
		require.Len(t, committed, 1)
		return nil
	}))
	require.NoError(t, session.Complete(nil))
	session.Close()
	_, err = escaped.ReadMessages(0, 1)
	require.Error(t, err)
	require.False(t, p.BootstrapPending())
	require.True(t, p.HWMKnown())
	require.Equal(t, uint64(5), p.GetHWM())
	offset, found, err := p.ProducerSequenceOffset("new", 0, 1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(2), offset)
	checkpoint, ok, _ := loadHWMCheckpoint(p.hwmCheckpointPath)
	require.True(t, ok)
	require.Equal(t, uint64(5), checkpoint)
}

func TestBootstrapCompletedReopenRebuildsCompactedHoles(t *testing.T) {
	for _, tc := range []struct {
		name    string
		end     uint64
		records []types.DiskMessage
	}{
		{name: "empty"}, {name: "all holes", end: 9}, {name: "trailing holes", end: 9, records: []types.DiskMessage{{Topic: "orders", Partition: 0, Offset: 2, ProducerID: "p", SeqNum: 4}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, cfg := bootstrapPartitionDisk(t, 0)
			p := NewPartition(0, "orders", d, nil, cfg)
			p.producerState.Store("stale", &producerEntry{lastSeq: 99})
			p.producerStateIndex["stale"] = producerStateCheckpointEntry{Seq: 99}
			receiver := partitionTransfer(t, d, 0, tc.end, tc.records)
			session, err := p.BeginBootstrapSession()
			require.NoError(t, err)
			detached := false
			require.NoError(t, session.Install(receiver, func() error { detached = true; require.Equal(t, uint64(0), d.GetAbsoluteOffset()); return nil }))
			require.True(t, detached)
			require.NoError(t, session.Complete(nil))
			session.Close()
			_, stale := p.producerState.Load("stale")
			require.False(t, stale)
			require.NotContains(t, p.producerStateIndex, "stale")
			require.Equal(t, tc.end, p.producerStateCoveredOffset)
			p.Close()
			require.NoError(t, d.Close())
			reopened, err := disk.NewDiskHandler(cfg, "orders", 0)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			pending, err := reopened.PendingBootstrap()
			require.NoError(t, err)
			require.Nil(t, pending)
			recovered := NewPartition(0, "orders", reopened, nil, cfg)
			t.Cleanup(recovered.Close)
			require.NoError(t, recovered.RecoveryError())
			require.NoError(t, recovered.RecoverProducerStateFromLog())
			require.Equal(t, tc.end, recovered.producerStateCoveredOffset)
			require.Equal(t, tc.end, recovered.GetHWM())
		})
	}
}

func TestBootstrapReadViewDrainsWithoutRecursiveReadLock(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	require.NoError(t, p.EnqueueSync(types.Message{Payload: "old"}))
	entered, readAgain, readDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		readDone <- p.WithReadView(func(v *PartitionReadView) error {
			close(entered)
			<-readAgain
			_, _, _, err := v.ReadCommittedPage(0, 10, 0, true)
			return err
		})
	}()
	<-entered
	sessionReady := make(chan *BootstrapSession, 1)
	sessionErr := make(chan error, 1)
	go func() {
		s, err := p.BeginBootstrapSession()
		if err != nil {
			sessionErr <- err
			return
		}
		sessionReady <- s
	}()
	select {
	case s := <-sessionReady:
		s.Close()
		t.Fatal("writer did not drain existing read")
	case err := <-sessionErr:
		t.Fatal(err)
	case <-time.After(30 * time.Millisecond):
	}
	close(readAgain)
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("nested read deadlocked behind waiting writer")
	}
	select {
	case s := <-sessionReady:
		s.Close()
	case err := <-sessionErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("writer did not acquire")
	}
	require.False(t, p.BootstrapPending())
	records, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
}

func TestBootstrapCheckpointFailureRetryPreservesUnrelatedError(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	receiver := partitionTransfer(t, d, 0, 3, nil)
	unrelated := errors.New("unrelated component")
	p.setRecoveryError(unrelated)
	p.txnRecoveryErr = errors.New("old transaction index")
	p.producerRecoveryErr = errors.New("old producer index")
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	require.NoError(t, s.Install(receiver, nil))
	original := p.hwmCheckpointPath
	p.hwmCheckpointPath = filepath.Join(t.TempDir(), "missing", "hwm")
	require.Error(t, s.Complete(nil))
	s.Close()
	require.True(t, p.BootstrapPending())
	pending, err := d.PendingBootstrap()
	require.NoError(t, err)
	require.NotNil(t, pending)
	p.hwmCheckpointPath = original
	s, err = p.BeginBootstrapSession()
	require.NoError(t, err)
	require.NoError(t, s.Complete(nil))
	s.Close()
	require.ErrorIs(t, p.RecoveryError(), unrelated)
	require.Nil(t, p.txnRecoveryErr)
	require.Nil(t, p.producerRecoveryErr)
	require.False(t, p.BootstrapPending())
}

func TestHWMCheckpointCapturesOnlyAfterSerialization(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	p.SetHWM(1)
	p.hwmCheckpointMu.Lock()
	done := make(chan struct{})
	go func() { p.persistHWMCheckpoint(); close(done) }()
	time.Sleep(30 * time.Millisecond)
	p.mu.Lock()
	p.HWM = 8
	p.mu.Unlock()
	p.hwmCheckpointMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("checkpoint blocked")
	}
	hwm, ok, _ := loadHWMCheckpoint(p.hwmCheckpointPath)
	require.True(t, ok)
	require.Equal(t, uint64(8), hwm)
}

func TestBootstrapRawRecoveryRetainsTransactionAndProducerFences(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	receiver := partitionTransfer(t, d, 0, 8, []types.DiskMessage{
		{Topic: "orders", Offset: 0, ProducerID: "open", SeqNum: 4, Epoch: 2, TransactionalID: "open", TransactionState: types.TransactionStateOpen},
		{Topic: "orders", Offset: 1, ProducerID: "abort", SeqNum: 5, Epoch: 3, TransactionalID: "aborted", TransactionState: types.TransactionStateOpen},
		{Topic: "orders", Offset: 2, ProducerID: "abort", SeqNum: 6, Epoch: 3, TransactionalID: "aborted", TransactionMarker: types.TransactionMarkerAbort, ControlBatchCoordinatorEpoch: 4},
		{Topic: "orders", Offset: 3, ProducerID: "control", SeqNum: 1, ControlBatchType: types.ControlBatchTransaction},
	})
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	require.NoError(t, s.Install(receiver, nil))
	require.NoError(t, s.Complete(nil))
	s.Close()
	require.Equal(t, uint64(4), p.producerStateIndex["open"].Seq)
	require.Equal(t, uint64(6), p.producerStateIndex["abort"].Seq)
	require.Equal(t, uint64(1), p.producerStateIndex["control"].Seq)
	require.Equal(t, uint64(0), p.LastStableOffset())
	require.Len(t, p.txnMarkers, 1)
	require.Len(t, p.txnOpenOffsets, 1)
}

func TestBootstrapPendingTopicConstructorAndAddedPartition(t *testing.T) {
	for _, added := range []bool{false, true} {
		t.Run(fmt.Sprint(added), func(t *testing.T) {
			id := 0
			if added {
				id = 1
			}
			d, cfg := bootstrapPartitionDisk(t, id)
			receiver := partitionTransfer(t, d, id, 4, nil)
			require.NoError(t, receiver.Install())
			hp := new(MockHandlerProvider)
			hp.On("GetHandler", "orders", id).Return(d, nil)
			def := Definition{Name: "orders", Partitions: id + 1, LifecycleEpoch: 9, ReplicationFactor: 1}
			var topic *Topic
			var err error
			if added {
				empty := &visibilityBenchmarkStorage{}
				hp.On("GetHandler", "orders", 0).Return(empty, nil)
				topic, err = newTopicWithDefinition(Definition{Name: "orders", Partitions: 1, LifecycleEpoch: 9, ReplicationFactor: 1}, hp, cfg, nil)
				require.NoError(t, err)
				err = topic.applyFullDefinition(def, hp, nil)
			} else {
				topic, err = newTopicWithDefinition(def, hp, cfg, nil)
			}
			require.NoError(t, err)
			for _, p := range topic.Partitions {
				t.Cleanup(p.Close)
			}
			p := topic.Partitions[id]
			require.True(t, p.BootstrapPending())
			require.False(t, p.HWMKnown())
			_, attempt := p.BootstrapStatus()
			require.ErrorContains(t, attempt, "incompatible")
			s, err := p.BeginBootstrapSession()
			require.NoError(t, err)
			require.ErrorContains(t, s.Complete(nil), "lifecycle")
			s.Close()
		})
	}
}

func TestBootstrapStrictDirectorySyncFailureRetriesAfterHWMPersist(t *testing.T) {
	for _, newSession := range []bool{false, true} {
		t.Run(fmt.Sprint(newSession), func(t *testing.T) {
			d, cfg := bootstrapPartitionDisk(t, 0)
			p := NewPartition(0, "orders", d, nil, cfg)
			t.Cleanup(p.Close)
			receiver := partitionTransfer(t, d, 0, 4, nil)
			calls := 0
			failure := errors.New("directory sync failure")
			p.checkpointDirectorySync = func(path string) error {
				calls++
				if calls == 2 {
					return failure
				}
				return syncPartitionDirectory(path)
			}
			s, err := p.BeginBootstrapSession()
			require.NoError(t, err)
			require.NoError(t, s.Install(receiver, nil))
			require.ErrorIs(t, s.Complete(nil), failure)
			require.True(t, p.BootstrapPending())
			require.False(t, p.HWMKnown())
			hwm, ok, _ := loadHWMCheckpoint(p.hwmCheckpointPath)
			require.True(t, ok)
			require.Equal(t, uint64(4), hwm)
			pending, err := d.PendingBootstrap()
			require.NoError(t, err)
			require.NotNil(t, pending)
			if newSession {
				s.Close()
				s, err = p.BeginBootstrapSession()
				require.NoError(t, err)
			}
			require.NoError(t, s.Complete(nil))
			s.Close()
			require.True(t, p.HWMKnown())
		})
	}
}

func TestBootstrapFailureBeforeIntentRestoresVisibleGeneration(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	require.NoError(t, p.EnqueueSync(types.Message{Payload: "old"}))
	receiver := partitionTransfer(t, d, 0, 3, nil)
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	require.ErrorContains(t, s.Install(receiver, func() error { return errors.New("detach failed") }), "detach failed")
	s.Close()
	require.NoError(t, receiver.Abort())
	require.False(t, p.BootstrapPending())
	require.NoError(t, p.RecoveryError())
	records, err := p.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "old", records[0].Payload)
}

func TestBootstrapRebuildPreservesCoordinatorEpochResolver(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	resolver := &testCoordinatorEpochDecisionResolver{state: types.TransactionStateCommitted, coordinatorEpoch: 8, known: true}
	p.SetTransactionDecisionResolver(resolver)
	receiver := partitionTransfer(t, d, 0, 4, []types.DiskMessage{
		{Topic: "orders", Offset: 0, TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen},
		{Topic: "orders", Offset: 2, TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit, ControlBatchCoordinatorEpoch: 7},
	})
	s, err := p.BeginBootstrapSession()
	require.NoError(t, err)
	require.NoError(t, s.Install(receiver, nil))
	require.NoError(t, s.Complete(nil))
	s.Close()
	require.Same(t, resolver, p.txnResolver)
	require.Zero(t, p.LastStableOffset())
	require.Len(t, p.txnOpenOffsets, 1)
	records, err := p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Empty(t, records)
	matching := &testCoordinatorEpochDecisionResolver{state: types.TransactionStateCommitted, coordinatorEpoch: 7, known: true}
	p.SetTransactionDecisionResolver(matching)
	require.Equal(t, uint64(4), p.LastStableOffset())
	records, err = p.ReadCommitted(0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, uint64(0), records[0].Offset)
}

func TestBootstrapSourceCapturesOneAvailableGeneration(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	require.NoError(t, p.EnqueueSync(types.Message{Payload: "source"}))
	var exported *disk.BootstrapExport
	var escaped *PartitionReadView
	require.NoError(t, p.WithBootstrapSource(func(source *BootstrapSource) error {
		escaped = source.View()
		require.False(t, p.BootstrapPending())
		records, err := source.View().ReadMessages(0, 10)
		require.NoError(t, err)
		require.Len(t, records, 1)
		fence := disk.BootstrapFence{Topic: "orders", Partition: 0, ReceiverID: "receiver", SourceID: "source", LeaderID: "source", LeaderEpoch: 1, LifecycleEpoch: 7, CommittedHWM: 1, RecoveryProtocol: 3}
		exported, err = source.Export(context.Background(), fence, []byte(`{"source":true}`))
		return err
	}))
	defer func() { require.NoError(t, exported.Close()) }()
	_, err := escaped.ReadMessages(0, 1)
	require.Error(t, err)
	require.Equal(t, uint64(1), exported.Manifest().EndOffset)
	for _, f := range exported.Manifest().Files {
		if f.Size > 0 {
			chunk, err := exported.ReadChunk(f.ID, 0, disk.BootstrapChunkBytes)
			require.NoError(t, err)
			require.NotEmpty(t, chunk)
		}
	}
}
