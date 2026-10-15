package topic

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type tailRepairStorage struct {
	*disk.DiskHandler
	truncate func()
}

func (s *tailRepairStorage) TruncateTo(offset uint64) error {
	s.truncate()
	return s.DiskHandler.TruncateTo(offset)
}

func (s *tailRepairStorage) TruncateToIfGenerationContext(ctx context.Context, offset uint64, generation disk.ContentGeneration) error {
	s.truncate()
	return s.DiskHandler.TruncateToIfGenerationContext(ctx, offset, generation)
}

func TestTailRepairDoesNotDeadlockSampledCompactionGate(t *testing.T) {
	for _, reconcile := range []bool{false, true} {
		name := "speculative"
		if reconcile {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.EnabledDistribution = true
			cfg.CleanupPolicy = config.CleanupPolicyCompact
			cfg.CompactionCheckIntervalMS = 60000
			d, err := disk.NewDiskHandler(cfg, "orders", 0)
			require.NoError(t, err)
			reached := make(chan struct{})
			p := NewPartition(0, "orders", &tailRepairStorage{d, func() { close(reached) }}, nil, cfg)
			p.SetHWM(0)
			require.NoError(t, p.EnqueueBatchLeader([]types.Message{{Payload: "committed"}}))
			require.NoError(t, p.ApplyReplicaHWM(1))
			p.FlushDisk()
			sampled, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			d.SetDistributedCompactionGate(func() (bool, string) {
				// The real distributed gate reaches this expression after full-ISR and
				// policy checks. Pause between its two independently sampled values.
				if p.NextOffset() != 1 {
					return false, "replica_not_caught_up"
				}
				close(sampled)
				<-release
				if p.GetHWM() != 1 {
					return false, "replica_not_caught_up"
				}
				return true, ""
			})
			compacted := make(chan error, 1)
			go func() { _, err := d.EnforceCompaction(); compacted <- err }()
			select {
			case <-sampled:
			case <-time.After(time.Second):
				t.Fatal("compaction did not sample the caught-up LEO")
			}
			require.NoError(t, p.EnqueueBatchLeader([]types.Message{{Payload: "failed-replication-tail"}}))
			repaired := make(chan error, 1)
			go func() {
				if reconcile {
					repaired <- p.ReconcileCommittedHWM(1)
				} else {
					repaired <- p.TruncateReplicaTail(1)
				}
			}()
			select {
			case <-reached:
			case <-time.After(time.Second):
				t.Fatal("tail repair did not reach disk truncation")
			}
			unblock()
			select {
			case err := <-compacted:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("tail repair holds partition mu while compaction waits for GetHWM")
			}
			select {
			case err := <-repaired:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("tail repair did not finish after compaction")
			}
			require.Equal(t, uint64(1), p.NextOffset())
			require.Equal(t, uint64(1), p.GetHWM())
			p.Close()
			require.NoError(t, d.Close())
		})
	}
}

func TestTailRepairPreservesSpeculativeAuthorityAndRawState(t *testing.T) {
	for _, authoritative := range []bool{false, true} {
		name := "unknown"
		if authoritative {
			name = "authoritative"
		}
		t.Run(name, func(t *testing.T) {
			p, d, cfg := rollbackPartitionRecords(t, []types.Message{
				{Payload: "committed"},
				{Payload: "retained-open", ProducerID: "producer", Epoch: 2, SeqNum: 1, TransactionalID: "transaction", TransactionState: types.TransactionStateOpen},
				{Payload: "discarded-marker", ProducerID: "producer", Epoch: 2, SeqNum: 2, TransactionalID: "transaction", TransactionMarker: types.TransactionMarkerAbort},
			})
			p.hwmCheckpointMu.Lock()
			p.mu.Lock()
			p.HWM, p.hwmAuthoritative = 1, authoritative
			p.mu.Unlock()
			err := os.Remove(p.hwmCheckpointPath)
			require.True(t, err == nil || os.IsNotExist(err))
			p.hwmCheckpointMu.Unlock()
			require.ErrorContains(t, p.TruncateReplicaTail(0), "crosses committed HWM")
			require.ErrorContains(t, p.TruncateReplicaTail(4), "behind repair boundary")
			require.NoError(t, p.TruncateReplicaTail(2))
			require.Equal(t, uint64(1), p.GetHWM())
			require.Equal(t, authoritative, p.HWMKnown())
			require.Equal(t, uint64(2), p.NextOffset())
			require.Empty(t, p.txnMarkers)
			require.Len(t, p.txnOpenOffsets, 1)
			require.Equal(t, uint64(1), p.producerStateIndex["producer"].Seq)
			checkpointHWM, exists, _ := loadHWMCheckpoint(p.hwmCheckpointPath)
			require.Equal(t, authoritative, exists)
			if authoritative {
				require.Equal(t, uint64(1), checkpointHWM)
			}
			p.Close()
			require.NoError(t, d.Close())
			reopened, err := disk.NewDiskHandler(cfg, "orders", 0)
			require.NoError(t, err)
			restarted := NewPartition(0, "orders", reopened, nil, cfg)
			t.Cleanup(func() { restarted.Close(); require.NoError(t, reopened.Close()) })
			require.Equal(t, authoritative, restarted.HWMKnown(), "tail repair must not certify an unknown HWM on restart")
			require.Equal(t, uint64(1), restarted.producerStateIndex["producer"].Seq)
		})
	}
}

func TestTailRepairReconcilePublishesAfterRebuildAndPreservesNewMarker(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	p.mu.Lock()
	p.HWM, p.hwmAuthoritative = 1, false
	p.mu.Unlock()
	oldMarker, newMarker := errors.New("old reconciliation"), errors.New("new reconciliation")
	p.MarkReconciliationPending(oldMarker)
	p.dh = &rollbackScanStorage{d, func() error {
		require.Equal(t, uint64(1), p.GetHWM(), "new HWM must remain unpublished during rebuild")
		require.False(t, p.HWMKnown())
		p.MarkReconciliationPending(newMarker)
		return nil
	}}
	require.NoError(t, p.ReconcileCommittedHWM(2))
	require.Equal(t, uint64(2), p.GetHWM())
	require.True(t, p.HWMKnown())
	require.ErrorIs(t, p.RecoveryError(), newMarker)
	require.NotErrorIs(t, p.RecoveryError(), oldMarker)
	checkpoint, exists, _ := loadHWMCheckpoint(p.hwmCheckpointPath)
	require.True(t, exists)
	require.Equal(t, uint64(2), checkpoint)
}

func TestTailRepairFailuresKeepOldHWMAndRemainUnavailable(t *testing.T) {
	for _, failureAt := range []string{"scan", "checkpoint", "partial_truncate"} {
		t.Run(failureAt, func(t *testing.T) {
			p, d, _ := rollbackPartition(t)
			p.mu.Lock()
			p.HWM, p.hwmAuthoritative = 1, false
			p.mu.Unlock()
			failure := errors.New("injected repair failure")
			switch failureAt {
			case "scan":
				p.dh = &rollbackScanStorage{d, func() error { return failure }}
			case "checkpoint":
				p.hwmCheckpointMu.Lock()
				p.producerCheckpointMu.Lock()
				p.checkpointDirectorySync = func(string) error { return failure }
				p.producerCheckpointMu.Unlock()
				p.hwmCheckpointMu.Unlock()
			case "partial_truncate":
				p.dh = &tailRepairFailureStorage{DiskHandler: d, failure: failure}
			}
			require.ErrorIs(t, p.ReconcileCommittedHWM(2), failure)
			require.ErrorIs(t, p.RecoveryError(), ErrReplicaRollbackFailed)
			require.Equal(t, uint64(1), p.GetHWM())
			require.False(t, p.HWMKnown())
			require.LessOrEqual(t, p.GetHWM(), p.NextOffset())
			require.Error(t, p.EnqueueBatchLeader([]types.Message{{Payload: "blocked"}}))
		})
	}
}

type tailRepairFailureStorage struct {
	*disk.DiskHandler
	failure error
}

func (s *tailRepairFailureStorage) TruncateToIfGenerationContext(ctx context.Context, offset uint64, generation disk.ContentGeneration) error {
	if err := s.DiskHandler.TruncateToIfGenerationContext(ctx, offset, generation); err != nil {
		return err
	}
	return s.failure
}

func TestTailRepairNoOpDoesNotTruncateOrScan(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	p.dh = &tailRepairStorage{d, func() { t.Fatal("no-op must not truncate") }}
	generation, err := d.CaptureContentGeneration()
	require.NoError(t, err)
	require.NoError(t, p.TruncateReplicaTail(3))
	p.MarkReconciliationPending(errors.New("owned marker"))
	require.NoError(t, p.ReconcileCommittedHWM(3))
	require.NoError(t, p.RecoveryError())
	require.NoError(t, d.CheckContentGenerationContext(context.Background(), generation))
}

func TestTailRepairLegacyNonDurableStorage(t *testing.T) {
	for _, reconcile := range []bool{false, true} {
		name := "speculative"
		if reconcile {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			storage := new(MockStorageHandler)
			storage.On("GetLatestOffset").Return(uint64(3)).Once()
			storage.On("TruncateTo", uint64(2)).Return(nil).Once()
			storage.On("GetFirstOffset").Return(uint64(0)).Once()
			storage.On("GetAbsoluteOffset").Return(uint64(2)).Once()
			storage.On("ReadMessages", uint64(0), 1024).Return([]types.Message{{Offset: 0, Payload: "committed"}, {Offset: 1, Payload: "retained", ProducerID: "producer", SeqNum: 1}}, nil).Once()
			cfg := config.DefaultConfig()
			cfg.EnabledDistribution = true
			p := NewPartition(0, "orders", storage, nil, cfg)
			p.mu.Lock()
			p.HWM, p.hwmAuthoritative = 1, true
			p.mu.Unlock()
			if reconcile {
				require.NoError(t, p.ReconcileCommittedHWM(2))
				require.Equal(t, uint64(2), p.GetHWM())
			} else {
				require.NoError(t, p.TruncateReplicaTail(2))
				require.Equal(t, uint64(1), p.GetHWM())
			}
			require.NoError(t, p.RecoveryError())
			require.Equal(t, uint64(2), p.NextOffset())
			require.Equal(t, uint64(1), p.producerStateIndex["producer"].Seq)
			storage.AssertExpectations(t)
		})
	}
}
