package topic

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func rollbackPartition(t *testing.T) (*Partition, *disk.DiskHandler, *config.Config) {
	return rollbackPartitionRecords(t, []types.Message{{Payload: "common"}, {Payload: "divergent"}, {Payload: "tail"}})
}

func rollbackPartitionRecords(t *testing.T, records []types.Message) (*Partition, *disk.DiskHandler, *config.Config) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	d, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(func() { p.Close(); require.NoError(t, d.Close()) })
	p.SetHWM(0)
	for i := range records {
		records[i].Offset = uint64(i)
	}
	require.NoError(t, p.ReplicaAppend(records))
	require.NoError(t, p.ApplyReplicaHWM(uint64(len(records))))
	p.FlushDisk()
	return p, d, cfg
}

type rollbackScanStorage struct {
	*disk.DiskHandler
	afterScan func() error
}

func (s *rollbackScanStorage) ScanRetainedRange(ctx context.Context, start, end uint64, visit func(types.Message) error) error {
	if err := s.DiskHandler.ScanRetainedRange(ctx, start, end, visit); err != nil {
		return err
	}
	return s.afterScan()
}

func TestReplicaRollbackRejectsRewrittenGenerationAfterRawScan(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	p.dh = &rollbackScanStorage{d, func() error { return d.TruncateTo(0) }}
	require.ErrorIs(t, p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, nil), disk.ErrStaleContentGeneration)
	require.ErrorIs(t, p.RecoveryError(), disk.ErrStaleContentGeneration)
	require.Error(t, p.Enqueue(types.Message{Payload: "must stay unavailable"}))
}

func TestReplicaRollbackRejectsRetentionDuringRawRebuild(t *testing.T) {
	p, d, cfg := rollbackPartition(t)
	require.NoError(t, d.RollSegmentAt(3))
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 3, Payload: "keep"}, {Offset: 4, Payload: "discard"}}))
	require.NoError(t, p.ApplyReplicaHWM(5))
	p.dh = &rollbackScanStorage{d, func() error {
		d.SetRetentionPolicy(0, 1)
		d.EnforceRetention(cfg)
		return nil
	}}
	require.ErrorIs(t, p.ApplyReplicaRollbackChecked(context.Background(), 4, 5, nil, 5, nil), disk.ErrStaleContentGeneration)
	require.Equal(t, uint64(3), d.GetFirstOffset())
	require.ErrorIs(t, p.RecoveryError(), ErrReplicaRollbackFailed)
	require.Equal(t, uint64(4), p.NextOffset())
	require.Equal(t, uint64(4), p.GetHWM())
}

func TestReplicaRollbackPartialDiskFailureRemainsUnavailable(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	err := p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, func() error {
		// Truncation closes writers and advances generation before removing this
		// target index. A nonempty directory makes that destructive phase fail.
		path := d.GetIndexPath(0)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Mkdir(path, 0700); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(path, "occupied"), []byte("x"), 0600)
	})
	require.ErrorContains(t, err, "remove target index")
	require.Error(t, p.RecoveryError())
	_, err = d.CaptureContentGeneration()
	require.ErrorContains(t, err, "unavailable")
	require.Equal(t, p.NextOffset(), p.GetHWM())
}

func TestReplicaRollbackVerifiesPrefixInsideExclusion(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	proofs, err := d.LineageCandidates(1)
	require.NoError(t, err)
	prefix := proofs[0]
	prefix.Digest = strings.Repeat("f", 64)
	intent := false
	err = p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, &prefix, 3, func() error { intent = true; return nil })
	require.ErrorContains(t, err, "prefix changed")
	require.False(t, intent)
	require.Equal(t, uint64(3), d.GetAbsoluteOffset())
	require.NoError(t, p.RecoveryError())
	prefix = proofs[0]
	require.NoError(t, p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, &prefix, 3, nil))
	require.Equal(t, uint64(1), p.NextOffset())
}

func TestReplicaRollbackRebuildsRawTransactionAndProducerStateAcrossRestart(t *testing.T) {
	p, d, cfg := rollbackPartitionRecords(t, []types.Message{
		{Payload: "retained-open", ProducerID: "producer", Epoch: 2, SeqNum: 1, TransactionalID: "transaction", TransactionState: types.TransactionStateOpen},
		{Payload: "discarded-marker", TransactionalID: "transaction", Epoch: 2, TransactionMarker: types.TransactionMarkerAbort, TransactionState: types.TransactionStateAborted},
		{Payload: "discarded-producer", ProducerID: "producer", Epoch: 2, SeqNum: 2},
	})
	p.SetTransactionDecisionResolver(&testTransactionDecisionResolver{state: types.TransactionStateAborted, known: true})
	require.NoError(t, p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, nil))
	require.Empty(t, p.txnMarkers)
	require.Len(t, p.txnOpenOffsets, 1)
	require.Equal(t, uint64(0), p.LastStableOffset())
	require.Equal(t, uint64(1), p.producerStateIndex["producer"].Seq)
	require.Equal(t, uint64(1), p.producerStateCoveredOffset)
	p.Close()
	require.NoError(t, d.Close())
	reopened, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	restarted := NewPartition(0, "orders", reopened, nil, cfg)
	t.Cleanup(func() { restarted.Close(); require.NoError(t, reopened.Close()) })
	require.NoError(t, restarted.RecoveryError())
	require.Equal(t, uint64(1), restarted.GetHWM())
	require.Equal(t, uint64(1), restarted.NextOffset())
	require.Empty(t, restarted.txnMarkers)
	require.Len(t, restarted.txnOpenOffsets, 1)
	require.Equal(t, uint64(1), restarted.producerStateIndex["producer"].Seq)
}

func TestReplicaRollbackRejectsUnknownStorageAuthority(t *testing.T) {
	cfg := config.DefaultConfig()
	p := NewPartition(0, "orders", &visibilityBenchmarkStorage{messages: make([]types.Message, 3)}, nil, cfg)
	defer p.Close()
	require.ErrorIs(t, p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, nil), ErrReplicaRollbackUnsupported)
	require.Equal(t, uint64(3), p.NextOffset())
}

func TestReplicaRollbackPreservesIndependentReconciliationFailure(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	failure := errors.New("awaiting independent committed HWM reconciliation")
	p.MarkReconciliationPending(failure)
	require.NoError(t, p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, nil))
	require.ErrorIs(t, p.RecoveryError(), failure)
	require.NoError(t, p.rollbackError())
	require.NoError(t, p.ReconcileCommittedHWM(1), "the original reconciliation owner can resolve its marker")
	require.NoError(t, p.RecoveryError())
}

func TestReplicaRollbackCancellationBeforeMutationRemainsRetryable(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	ctx, cancel := context.WithCancel(context.Background())
	err := p.ApplyReplicaRollbackChecked(ctx, 1, 3, nil, 3, func() error { cancel(); return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrReplicaRollbackFailed)
	require.NoError(t, p.RecoveryError())
	require.Equal(t, uint64(3), d.GetAbsoluteOffset())
	require.NoError(t, p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, nil))
}

func TestReplicaRollbackCloseDuringRebuildStaysUnavailable(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	closed := make(chan struct{})
	p.dh = &rollbackScanStorage{d, func() error {
		go func() { p.Close(); close(closed) }()
		<-p.closeCh
		return nil
	}}
	require.ErrorIs(t, p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, nil), ErrReplicaRollbackFailed)
	<-closed
	require.Error(t, p.RecoveryError())
	require.Equal(t, uint64(1), p.GetHWM())
	require.Equal(t, uint64(1), p.NextOffset())
}

func TestReplicaRollbackExcludesDirectMutations(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mutate       func(*Partition) error
		returnsError bool
	}{
		{"enqueue", func(p *Partition) error { return p.Enqueue(types.Message{Payload: "x"}) }, true},
		{"enqueue_sync", func(p *Partition) error { return p.EnqueueSync(types.Message{Payload: "x"}) }, true},
		{"enqueue_idempotent", func(p *Partition) error { return p.EnqueueSyncIdempotent(types.Message{Payload: "x"}) }, true},
		{"enqueue_batch", func(p *Partition) error { return p.EnqueueBatch([]types.Message{{Payload: "x"}}) }, true},
		{"enqueue_batch_sync", func(p *Partition) error { return p.EnqueueBatchSync([]types.Message{{Payload: "x"}}) }, true},
		{"enqueue_leader", func(p *Partition) error { return p.EnqueueBatchLeader([]types.Message{{Payload: "x"}}) }, true},
		{"replica_append", func(p *Partition) error { return p.ReplicaAppend([]types.Message{{Offset: 3, Payload: "x"}}) }, true},
		{"compacted_append", func(p *Partition) error {
			return p.ReplicaAppendCompactedRange([]types.Message{{Offset: 3, Payload: "x"}}, 4)
		}, true},
		{"reserve", func(p *Partition) error { _, err := p.ReserveOffsets(1); return err }, true},
		{"update_leo", func(p *Partition) error { p.UpdateLEO(4); return nil }, false},
		{"apply_hwm", func(p *Partition) error { return p.ApplyReplicaHWM(4) }, true},
		{"set_hwm", func(p *Partition) error { p.SetHWM(4); return nil }, false},
		{"mark_verified", func(p *Partition) error { p.MarkReplicaCatchupVerified(1, 1, 3); return nil }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _, _ := rollbackPartition(t)
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, func() error { close(entered); <-release; return nil })
			}()
			<-entered
			err := tc.mutate(p)
			leo, hwm, verified := p.NextOffset(), p.GetHWM(), p.ReplicaCatchupVerified(1, 1, 3)
			pending := p.RecoveryError()
			close(release)
			rollbackErr := <-done
			if tc.returnsError {
				require.ErrorIs(t, err, ErrReplicaRollbackPending)
			}
			require.Equal(t, uint64(3), leo)
			require.Equal(t, uint64(3), hwm)
			require.False(t, verified)
			require.ErrorIs(t, pending, ErrReplicaRollbackPending)
			require.NoError(t, rollbackErr)
		})
	}
}

func TestReplicaRollbackStaleGenerationDoesNotTruncate(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	err := p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, func() error {
		// An independent disk rewrite invalidates the token after validation.
		return d.TruncateTo(2)
	})
	require.ErrorIs(t, err, disk.ErrStaleContentGeneration)
	require.Equal(t, uint64(2), d.GetAbsoluteOffset(), "checked rollback must not truncate to one")
	require.Equal(t, uint64(3), p.NextOffset())
	require.NoError(t, p.RecoveryError(), "clean rejection releases only its transient state")
}

func TestReplicaRollbackCheckpointFailureRemainsUnavailable(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	failure := errors.New("injected checkpoint sync failure")
	p.hwmCheckpointMu.Lock()
	p.producerCheckpointMu.Lock()
	p.checkpointDirectorySync = func(string) error { return failure }
	p.producerCheckpointMu.Unlock()
	p.hwmCheckpointMu.Unlock()
	require.ErrorIs(t, p.ApplyReplicaRollbackChecked(context.Background(), 1, 3, nil, 3, nil), failure)
	require.Equal(t, uint64(1), d.GetAbsoluteOffset())
	require.Equal(t, uint64(1), p.NextOffset())
	require.Equal(t, uint64(1), p.GetHWM())
	require.ErrorIs(t, p.RecoveryError(), failure)
	require.ErrorIs(t, p.ApplyReplicaHWM(2), failure)
	p.SetHWM(2)
	require.Equal(t, uint64(1), p.GetHWM())
	require.ErrorIs(t, p.ApplyReplicaRollbackChecked(context.Background(), 0, 1, nil, 3, nil), failure)
}

func TestReplicaRollbackDoesNotDeadlockCompactionHWMGate(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.CleanupPolicy = config.CleanupPolicyCompact
	cfg.CompactionCheckIntervalMS = 60000
	d, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	p := NewPartition(0, "orders", d, nil, cfg)
	p.SetHWM(0)
	require.NoError(t, p.EnqueueBatchLeader([]types.Message{{Payload: "common"}, {Payload: "divergent"}}))
	p.FlushDisk()
	require.NoError(t, p.ApplyReplicaHWM(2))
	entered, release := make(chan struct{}), make(chan struct{})
	d.SetDistributedCompactionGate(func() (bool, string) {
		close(entered)
		<-release
		_ = p.GetHWM()
		return false, "test_gate"
	})
	compacted := make(chan error, 1)
	go func() { _, err := d.EnforceCompaction(); compacted <- err }()
	<-entered
	rolled := make(chan error, 1)
	go func() { rolled <- p.ApplyReplicaRollback(1) }()
	require.Eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		return strings.Contains(string(buf[:n]), "(*DiskHandler).truncateTo(")
	}, 3*time.Second, time.Millisecond)
	close(release)
	select {
	case err := <-compacted:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("rollback holds partition mu while compaction needs GetHWM")
	}
	select {
	case err := <-rolled:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("rollback did not complete after compaction")
	}
	require.Equal(t, uint64(1), p.GetHWM())
	require.Equal(t, uint64(1), p.NextOffset())
	p.Close()
	require.NoError(t, d.Close())
}
