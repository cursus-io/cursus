package topic

import (
	"context"
	"math"
	"testing"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func snapshotEvidencePartition(t *testing.T) (*Partition, *disk.DiskHandler) {
	t.Helper()
	p, d, _ := rollbackPartitionRecords(t, []types.Message{
		{TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen},
		{TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit},
	})
	return p, d
}

func appendSnapshotEvidenceMarker(t *testing.T, p *Partition) {
	t.Helper()
	require.NoError(t, p.EnqueueSync(types.Message{TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit}))
}

func TestSnapshotEvidenceRejectsLiveDuplicateMarker(t *testing.T) {
	for _, timing := range []string{"before-page", "during-page", "after-page", "batch"} {
		t.Run(timing, func(t *testing.T) {
			p, d := snapshotEvidencePartition(t)
			require.Nil(t, p.txnResolver)
			require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
				require.Equal(t, uint64(2), c.CommittedEnd())
				require.Equal(t, uint64(2), c.LastStableOffset())
				messages, _, _, err := c.ReadVisiblePage(context.Background(), 0, 2, 0, true)
				require.NoError(t, err)
				require.Len(t, messages, 1)
				switch timing {
				case "before-page":
					appendSnapshotEvidenceMarker(t, p)
				case "during-page":
					p.dh = &snapshotCutStorage{DiskHandler: d, afterPage: func() { appendSnapshotEvidenceMarker(t, p) }}
				case "batch":
					require.NoError(t, p.EnqueueBatchSync([]types.Message{{TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit}}))
				}
				messages, _, _, err = c.ReadVisiblePage(context.Background(), 0, 2, 0, true)
				require.NoError(t, err)
				if timing == "after-page" {
					require.Len(t, messages, 1)
					appendSnapshotEvidenceMarker(t, p)
				} else {
					require.Empty(t, messages, "the later live marker hides the fixed-cut record")
				}
				require.NoError(t, c.ValidateRaw(context.Background()))
				require.Error(t, c.ValidateTransactional(context.Background()), "changed evidence must never certify this visibility result")
				return nil
			}))
		})
	}
}

type snapshotEvidenceBoundsStorage struct {
	*disk.DiskHandler
	afterFlushed func()
}

func (s *snapshotEvidenceBoundsStorage) GetFlushedOffset() uint64 {
	end := s.DiskHandler.GetFlushedOffset()
	if s.afterFlushed != nil {
		hook := s.afterFlushed
		s.afterFlushed = nil
		hook()
	}
	return end
}

func TestSnapshotEvidenceCapturesBeforeBounds(t *testing.T) {
	for _, timing := range []string{"anchor", "fixed-end"} {
		t.Run(timing, func(t *testing.T) {
			p, d := snapshotEvidencePartition(t)
			hook := func() { appendSnapshotEvidenceMarker(t, p) }
			if timing == "anchor" {
				p.dh = &snapshotCutStorage{DiskHandler: d, afterAnchor: hook}
			} else {
				p.dh = &snapshotEvidenceBoundsStorage{DiskHandler: d, afterFlushed: hook}
			}
			require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
				if timing == "fixed-end" {
					require.Equal(t, uint64(2), c.CommittedEnd())
				}
				require.NoError(t, c.ValidateRaw(context.Background()))
				require.Error(t, c.ValidateTransactional(context.Background()), "capturing evidence after bounds would silently refresh this token")
				return nil
			}))
		})
	}
}

func TestSnapshotEvidenceRawPrefixIndependentOfAppendedTransactions(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		require.NoError(t, p.EnqueueSync(types.Message{Payload: "ordinary append"}))
		require.NoError(t, c.ValidateTransactional(context.Background()), "ordinary append does not change transaction evidence")
		require.NoError(t, p.EnqueueSync(types.Message{TransactionalID: "later", TransactionState: types.TransactionStateOpen}))
		page, err := c.ReadRawPage(context.Background(), 0, 10, 0, true)
		require.NoError(t, err)
		require.Len(t, page.Messages, 3)
		require.True(t, page.Exhausted)
		require.NoError(t, c.ValidateRaw(context.Background()), "the entire fixed raw prefix remains nontransactional")
		require.Error(t, c.ValidateTransactional(context.Background()))
		return nil
	}))
}

func TestSnapshotEvidenceAuthoritativeMutationPaths(t *testing.T) {
	for _, mutation := range []string{"open", "prune", "install", "resolver"} {
		t.Run(mutation, func(t *testing.T) {
			p, _ := snapshotEvidencePartition(t)
			require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
				p.txnMarkerMu.RLock()
				before := p.txnEvidenceRevision
				p.txnMarkerMu.RUnlock()
				switch mutation {
				case "open":
					p.indexTransactionMessage(types.Message{TransactionalID: "new", TransactionState: types.TransactionStateOpen, Offset: 2})
				case "prune":
					p.pruneTransactionIndex(2)
				case "install":
					p.installTransactionState(&rawRecoveryState{markers: make(map[transactionMarkerKey]transactionMarkerInfo), open: make(map[transactionMarkerKey]uint64)})
				case "resolver":
					p.SetTransactionDecisionResolver(nil)
				}
				p.txnMarkerMu.RLock()
				after := p.txnEvidenceRevision
				p.txnMarkerMu.RUnlock()
				require.Greater(t, after, before)
				require.Error(t, c.ValidateTransactional(context.Background()))
				return c.ValidateRaw(context.Background())
			}))
		})
	}
}

func TestSnapshotEvidenceResolvedOpenCacheDeletionIsEquivalent(t *testing.T) {
	p, _ := snapshotEvidencePartition(t)
	m := transaction.NewManager()
	importSnapshotCutOpen(t, m)
	p.SetTransactionDecisionResolver(m)
	require.Len(t, p.txnOpenOffsets, 1)
	state := m.ExportState()
	state["tx"].State = transaction.StateCommitted
	require.NoError(t, m.ImportState(state))
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		require.Equal(t, uint64(2), c.LastStableOffset())
		before := p.txnEvidenceRevision
		require.Equal(t, uint64(2), p.lastStableOffset())
		require.Empty(t, p.txnOpenOffsets)
		require.Equal(t, before, p.txnEvidenceRevision)
		require.True(t, p.txnMarkers[transactionMarkerKey{transactionalID: "tx", epoch: 1}].hasOpenOffset)
		return c.ValidateTransactional(context.Background())
	}))
}

func TestSnapshotEvidenceSaturationPreservesRawExport(t *testing.T) {
	for _, exhaustedAtEntry := range []bool{false, true} {
		p, _ := snapshotEvidencePartition(t)
		p.txnMarkerMu.Lock()
		p.txnEvidenceRevision = math.MaxUint64 - 1
		if exhaustedAtEntry {
			p.txnEvidenceRevision++
		}
		p.txnMarkerMu.Unlock()
		require.NoError(t, p.WithBootstrapSource(func(source *BootstrapSource) error {
			c, err := source.SnapshotReadCut()
			require.NoError(t, err)
			if !exhaustedAtEntry {
				require.NoError(t, c.ValidateTransactional(context.Background()))
				appendSnapshotEvidenceMarker(t, p)
			}
			require.Error(t, c.ValidateTransactional(context.Background()), "evidence exhaustion alone must fail before any resolver rebind")
			p.SetTransactionDecisionResolver(nil)
			require.Equal(t, uint64(math.MaxUint64), p.txnEvidenceRevision)
			require.Error(t, c.ValidateTransactional(context.Background()))
			require.NoError(t, c.ValidateRaw(context.Background()))
			fence := disk.BootstrapFence{Topic: "orders", Partition: 0, ReceiverID: "receiver", SourceID: "source", LeaderID: "source", LeaderEpoch: 1, LifecycleEpoch: 1, CommittedHWM: 2, RecoveryProtocol: 3}
			export, err := source.Export(context.Background(), fence, nil)
			require.NoError(t, err)
			return export.Close()
		}))
	}
}
