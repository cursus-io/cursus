package topic

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type snapshotCutStorage struct {
	*disk.DiskHandler
	afterAnchor func()
	afterPage   func()
}

func importSnapshotCutOpen(t *testing.T, m *transaction.Manager) {
	t.Helper()
	require.NoError(t, m.ImportState(map[string]*transaction.Snapshot{"tx": {
		ID: "tx", Producer: "producer", Epoch: 1, State: transaction.StateOpen,
		Revision: 1, CreatedAt: time.Unix(100, 0), UpdatedAt: time.Unix(100, 0),
	}}))
}

func (s *snapshotCutStorage) GetFirstOffsetContext(ctx context.Context) (uint64, error) {
	first, err := s.DiskHandler.GetFirstOffsetContext(ctx)
	if s.afterAnchor != nil {
		s.afterAnchor()
	}
	return first, err
}

func (s *snapshotCutStorage) ReadRetainedPage(ctx context.Context, start, end uint64, records, bytes int) (disk.RetainedPage, error) {
	page, err := s.DiskHandler.ReadRetainedPage(ctx, start, end, records, bytes)
	if s.afterPage != nil {
		s.afterPage()
	}
	return page, err
}

func TestSnapshotCutOriginalGenerationAndOptionalStorage(t *testing.T) {
	for _, change := range []string{"none", "bounds", "page", "final"} {
		t.Run(change, func(t *testing.T) {
			p, d, _ := rollbackPartition(t)
			wrapped := &snapshotCutStorage{DiskHandler: d}
			p.dh = wrapped
			rewrite := func() { rewriteSameTail(t, d) }
			if change == "bounds" {
				wrapped.afterAnchor = rewrite
			}
			if change == "page" {
				wrapped.afterPage = rewrite
			}
			called := false
			err := p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
				called = true
				page, err := c.ReadRawPage(context.Background(), 0, 2, 0, true)
				if err != nil {
					return err
				}
				require.Len(t, page.Messages, 2)
				require.Equal(t, uint64(2), page.EndOffset)
				if change == "final" {
					rewrite()
				}
				return nil
			})
			if change == "none" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, disk.ErrStaleContentGeneration)
			}
			require.Equal(t, change != "bounds", called)
		})
	}
	p, d, _ := rollbackPartition(t)
	p.dh = struct{ types.StorageHandler }{d}
	require.ErrorIs(t, p.WithSnapshotCertification(context.Background(), func(*SnapshotReadCut) error { t.Fatal("unsupported backend entered callback"); return nil }), ErrReplicaSourceUnsupported)
}

func TestSnapshotCutFixedBoundsAndFilteredProgress(t *testing.T) {
	p, _, _ := rollbackPartitionRecords(t, []types.Message{
		{TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateCommitted},
		{Payload: "ordinary"},
		{TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit},
	})
	p.mu.Lock()
	p.HWM = 2
	p.mu.Unlock()
	var escaped *SnapshotReadCut
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		escaped = c
		require.Equal(t, uint64(2), c.CommittedEnd())
		require.Equal(t, uint64(2), c.LastStableOffset())
		p.SetHWM(3)
		msgs, bytes, next, err := c.ReadVisiblePage(context.Background(), 0, 1, 0, true)
		require.NoError(t, err)
		require.Empty(t, msgs)
		require.Positive(t, bytes)
		require.Equal(t, uint64(1), next)
		msgs, _, next, err = c.ReadVisiblePage(context.Background(), next, 10, 0, true)
		require.NoError(t, err)
		require.Len(t, msgs, 1)
		require.Equal(t, uint64(2), next)
		raw, err := c.ReadRawPage(context.Background(), 0, 10, 0, true)
		require.NoError(t, err)
		require.Len(t, raw.Messages, 2)
		require.True(t, raw.Exhausted)
		v := c.View()
		require.Equal(t, c.ContentGeneration(), raw.ContentGeneration)
		require.Equal(t, uint64(2), v.CommittedEnd())
		require.NoError(t, v.ValidateIdentity("orders", 0))
		require.Error(t, v.ValidateBootstrapSource("orders", 0, 1))
		require.Error(t, v.ValidateBootstrapRecovery("orders", 0, 1))
		_, err = v.ReplicaCatchupIndexGeneration("orders", 0, 2)
		require.Error(t, err)
		_, err = v.ReadCommittedMetadataThrough(0, 2, 10)
		require.ErrorContains(t, err, "do not support committed metadata")
		_, err = v.ReadCommittedMetadata(0, 10)
		require.Error(t, err)
		return c.ValidateTransactional(context.Background())
	}))
	require.Error(t, escaped.ValidateRaw(context.Background()))
	require.Error(t, escaped.View().ValidateIdentity("orders", 0))
	_, err := escaped.ReadRawPage(context.Background(), 0, 1, 0, true)
	require.Error(t, err)
}

func TestSnapshotCutScratchLSORestoresResolvedEvidence(t *testing.T) {
	p, _, _ := rollbackPartitionRecords(t, []types.Message{
		{TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen},
		{Payload: "ordinary"},
		{TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit},
	})
	m := transaction.NewManager()
	p.SetTransactionDecisionResolver(m)
	require.Equal(t, uint64(3), p.LastStableOffset()) // Unknown compatibility removes the live open.
	require.Empty(t, p.txnOpenOffsets)
	importSnapshotCutOpen(t, m)
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		require.Zero(t, c.LastStableOffset())
		require.Empty(t, p.txnOpenOffsets, "certification must not mutate live state")
		messages, _, next, err := c.ReadVisiblePage(context.Background(), 0, 10, 0, true)
		require.NoError(t, err)
		require.Empty(t, messages)
		require.Zero(t, next)
		return c.ValidateTransactional(context.Background())
	}))
}

func TestSnapshotCutResolverProvenance(t *testing.T) {
	for _, change := range []string{"nil", "unsupported", "unrelated", "import", "same", "nil-rebind", "overflow", "overflow-during"} {
		t.Run(change, func(t *testing.T) {
			p, _, _ := rollbackPartition(t)
			m := transaction.NewManager()
			if change != "nil" && change != "nil-rebind" {
				p.SetTransactionDecisionResolver(m)
			}
			if change == "unsupported" {
				p.SetTransactionDecisionResolver(&testTransactionDecisionResolver{known: true, state: types.TransactionStateCommitted})
			}
			if change == "overflow" {
				p.txnResolverRevision = math.MaxUint64 - 1
				p.SetTransactionDecisionResolver(m)
			}
			if change == "overflow-during" {
				p.txnResolverRevision = math.MaxUint64 - 1
			}
			require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
				switch change {
				case "unrelated":
					_, _, err := m.InitProducer("other")
					require.NoError(t, err)
				case "import":
					importSnapshotCutOpen(t, m)
				case "same":
					p.SetTransactionDecisionResolver(m)
				case "nil-rebind":
					p.SetTransactionDecisionResolver(nil)
				case "overflow", "overflow-during":
					p.SetTransactionDecisionResolver(nil)
					require.Equal(t, uint64(math.MaxUint64), p.txnResolverRevision)
				}
				require.NoError(t, c.ValidateRaw(context.Background()), "raw nontransactional provenance must ignore all resolver changes")
				err := c.ValidateTransactional(context.Background())
				if change == "nil" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				return nil
			}))
		})
	}
}

func TestSnapshotCutDecisionChangesDuringPage(t *testing.T) {
	p, d, _ := rollbackPartitionRecords(t, []types.Message{
		{TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen},
		{TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit},
	})
	m := transaction.NewManager()
	p.SetTransactionDecisionResolver(m)
	p.dh = &snapshotCutStorage{DiskHandler: d, afterPage: func() {
		importSnapshotCutOpen(t, m)
	}}
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		require.Equal(t, uint64(2), c.LastStableOffset())
		messages, _, next, err := c.ReadVisiblePage(context.Background(), 0, 10, 0, true)
		require.NoError(t, err)
		require.Empty(t, messages)
		require.Equal(t, uint64(2), next)
		require.Error(t, c.ValidateTransactional(context.Background()))
		return nil
	}))
}

func TestSnapshotCutRawLimitsCancellationAndState(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		_, err := c.ReadRawPage(context.Background(), 0, 10, 1, false)
		require.ErrorIs(t, err, disk.ErrRetainedPageLimit)
		page, err := c.ReadRawPage(context.Background(), 0, 10, 1, true)
		require.NoError(t, err)
		require.Len(t, page.Messages, 1)
		require.Greater(t, page.DecodedBytes, 1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = c.ReadRawPage(ctx, 0, 10, 0, true)
		require.ErrorIs(t, err, context.Canceled)
		p.mu.Lock()
		p.HWM = 2
		p.mu.Unlock()
		require.Error(t, c.ValidateRaw(context.Background()))
		p.SetHWM(3)
		return nil
	}))
	p.validateBootstrapDefinition(Definition{Name: "orders", LifecycleEpoch: 1})
	require.Error(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		p.validateBootstrapDefinition(Definition{Name: "orders", LifecycleEpoch: 2})
		return nil
	}))
}

func TestReplicaSnapshotCutWitnessFences(t *testing.T) {
	for _, change := range []string{"none", "owner", "generation", "attempt", "marker", "leader", "epoch", "lifecycle", "local-lifecycle", "hwm", "bootstrap", "recovery", "after"} {
		t.Run(change, func(t *testing.T) {
			p, d, _ := rollbackPartition(t)
			pending := errors.New("owned reconciliation pending")
			p.MarkReconciliationPending(pending)
			_, witness := replicaWitness(t, p, 0)
			leader, epoch, lifecycle, hwm := "leader", 1, uint64(1), uint64(3)
			switch change {
			case "owner":
				p, _, _ = rollbackPartition(t)
			case "generation":
				rewriteSameTail(t, d)
			case "attempt":
				_, _ = replicaWitness(t, p, 0)
			case "marker":
				p.MarkReconciliationPending(pending)
			case "leader":
				leader = "other"
			case "epoch":
				epoch++
			case "lifecycle":
				lifecycle++
			case "local-lifecycle":
				p.validateBootstrapDefinition(Definition{Name: "orders", LifecycleEpoch: 2})
			case "hwm":
				hwm--
			case "bootstrap":
				p.bootstrapPending.Store(true)
				defer p.bootstrapPending.Store(false)
			case "recovery":
				p.setRecoveryError(errors.New("independent failure"))
			}
			called := false
			err := p.WithReplicaSnapshotCertification(context.Background(), witness, leader, epoch, lifecycle, hwm, func(c *SnapshotReadCut) error {
				called = true
				require.Equal(t, uint64(3), c.CommittedEnd())
				require.Error(t, c.View().ValidateBootstrapSource("orders", 0, 1))
				_, err := c.View().ReplicaCatchupIndexGeneration("orders", 0, 3)
				require.Error(t, err)
				if change == "after" {
					p.MarkReconciliationPending(pending)
				}
				return nil
			})
			if change == "none" {
				require.NoError(t, err)
				require.ErrorIs(t, p.ReconciliationError(), pending)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, change == "none" || change == "after", called)
		})
	}
}

func TestBootstrapSnapshotCutLegacyResolverAndLifetime(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	p.SetTransactionDecisionResolver(&testTransactionDecisionResolver{known: true, state: types.TransactionStateCommitted})
	var source *BootstrapSource
	require.NoError(t, p.WithBootstrapSource(func(s *BootstrapSource) error {
		source = s
		c, err := s.SnapshotReadCut()
		require.NoError(t, err)
		require.NoError(t, s.View().ValidateBootstrapSource("orders", 0, 1))
		require.Error(t, c.View().ValidateBootstrapSource("orders", 0, 1))
		require.Error(t, c.ValidateTransactional(context.Background()))
		fence := disk.BootstrapFence{Topic: "orders", Partition: 0, ReceiverID: "receiver", SourceID: "source", LeaderID: "source", LeaderEpoch: 1, LifecycleEpoch: 1, CommittedHWM: 3, RecoveryProtocol: 3}
		exported, err := s.Export(context.Background(), fence, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(3), exported.Manifest().EndOffset)
		return exported.Close()
	}))
	_, err := source.SnapshotReadCut()
	require.Error(t, err)
}

type snapshotGenerationResolver struct {
	p         *Partition
	t         *testing.T
	available bool
	onCheck   func()
}

func (r *snapshotGenerationResolver) TransactionDecision(string, int64) (string, bool) {
	return types.TransactionStateCommitted, true
}

func (r *snapshotGenerationResolver) CaptureTransactionDecisionGeneration() (uint64, bool) {
	require.True(r.t, r.p.txnMarkerMu.TryLock(), "capture must release transaction marker lock")
	r.p.txnMarkerMu.Unlock()
	return 7, r.available
}

func (r *snapshotGenerationResolver) CheckTransactionDecisionGeneration(uint64) bool {
	require.True(r.t, r.p.txnMarkerMu.TryLock(), "check must release transaction marker lock")
	r.p.txnMarkerMu.Unlock()
	if r.onCheck != nil {
		r.onCheck()
	}
	return true
}

func TestSnapshotCutGenerationLockOrderAndBindingRecheck(t *testing.T) {
	for _, available := range []bool{false, true} {
		p, _, _ := rollbackPartition(t)
		r := &snapshotGenerationResolver{p: p, t: t, available: available}
		p.SetTransactionDecisionResolver(r)
		require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
			if available {
				require.NoError(t, c.ValidateTransactional(context.Background()))
				r.onCheck = func() { p.SetTransactionDecisionResolver(r) }
			}
			require.Error(t, c.ValidateTransactional(context.Background()))
			return c.ValidateRaw(context.Background())
		}))
	}
}

func TestSnapshotCutMarkerCompatibilityAndRetentionFloor(t *testing.T) {
	for _, test := range []struct {
		name     string
		resolver TransactionDecisionResolver
		stable   uint64
	}{
		{"nil", nil, 3},
		{"unknown", &testTransactionDecisionResolver{known: false}, 3},
		{"terminal", &testTransactionDecisionResolver{known: true, state: types.TransactionStateCommitted}, 3},
		{"mismatch", &testCoordinatorEpochDecisionResolver{known: true, state: types.TransactionStateCommitted, coordinatorEpoch: 8}, 1},
		{"matching", &testCoordinatorEpochDecisionResolver{known: true, state: types.TransactionStateCommitted, coordinatorEpoch: 7}, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, _, _ := rollbackPartitionRecords(t, []types.Message{
				{TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen},
				{Payload: "ordinary"},
				{TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit, ControlBatchCoordinatorEpoch: 7},
			})
			p.SetTransactionDecisionResolver(test.resolver)
			p.txnMarkerMu.Lock()
			p.txnRetentionFloor = 1
			p.txnMarkerMu.Unlock()
			require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
				require.Equal(t, test.stable, c.LastStableOffset())
				return nil
			}))
		})
	}
}

func TestSnapshotCutMarkedGapProgress(t *testing.T) {
	d, cfg := bootstrapPartitionDisk(t, 0)
	require.NoError(t, d.WriteCompactedReplicaRange(0, 10, []types.DiskMessage{
		{Topic: "orders", Offset: 2, TransactionalID: "tx", TransactionState: types.TransactionStateAborted},
		{Topic: "orders", Offset: 5, TransactionalID: "tx", TransactionMarker: types.TransactionMarkerAbort},
	}))
	p := NewPartition(0, "orders", d, nil, cfg)
	t.Cleanup(p.Close)
	p.SetHWM(10)
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		messages, bytes, next, err := c.ReadVisiblePage(context.Background(), 0, 1, 0, true)
		require.NoError(t, err)
		require.Empty(t, messages)
		require.Positive(t, bytes)
		require.Equal(t, uint64(5), next)
		messages, bytes, next, err = c.ReadVisiblePage(context.Background(), next, 1, 0, true)
		require.NoError(t, err)
		require.Empty(t, messages)
		require.Positive(t, bytes)
		require.Equal(t, uint64(10), next)
		return c.ValidateTransactional(context.Background())
	}))
}

func TestBootstrapSnapshotCutFinalGenerationCheck(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	require.ErrorIs(t, p.WithBootstrapSource(func(s *BootstrapSource) error {
		_, err := s.SnapshotReadCut()
		require.NoError(t, err)
		rewriteSameTail(t, d)
		return nil
	}), disk.ErrStaleContentGeneration)
}

func TestSnapshotCutRebuildsOpenEvidenceBeforeFixedEnd(t *testing.T) {
	p, _, _ := rollbackPartitionRecords(t, []types.Message{
		{TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen},
		{Payload: "ordinary"},
		{TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit},
	})
	require.Equal(t, uint64(3), p.LastStableOffset())
	require.Empty(t, p.txnOpenOffsets)
	p.mu.Lock()
	p.HWM = 2
	p.mu.Unlock()
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		require.Zero(t, c.LastStableOffset())
		p.SetHWM(3)
		require.Zero(t, c.LastStableOffset())
		require.Empty(t, p.txnOpenOffsets)
		return c.ValidateTransactional(context.Background())
	}))
}

func TestSnapshotCutPageCancellationAndOriginalResolver(t *testing.T) {
	p, d, _ := rollbackPartitionRecords(t, []types.Message{
		{TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen},
		{TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit},
	})
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, p.WithSnapshotCertification(context.Background(), func(c *SnapshotReadCut) error {
		// Captured nil compatibility remains the read resolver even when binding changes.
		p.SetTransactionDecisionResolver(&testTransactionDecisionResolver{known: true, state: types.TransactionStateOpen})
		messages, _, _, err := c.ReadVisiblePage(ctx, 0, 10, 0, true)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		require.Error(t, c.ValidateTransactional(ctx))
		p.dh = &snapshotCutStorage{DiskHandler: d, afterPage: cancel}
		messages, _, _, err = c.ReadVisiblePage(ctx, 0, 10, 0, true)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, messages)
		return nil
	}))
}

func TestBootstrapSnapshotCutExhaustedResolverPreservesExport(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	p.SetTransactionDecisionResolver(&snapshotGenerationResolver{p: p, t: t, available: false})
	require.NoError(t, p.WithBootstrapSource(func(s *BootstrapSource) error {
		cut, err := s.SnapshotReadCut()
		require.NoError(t, err)
		require.Error(t, cut.ValidateTransactional(context.Background()))
		fence := disk.BootstrapFence{Topic: "orders", Partition: 0, ReceiverID: "receiver", SourceID: "source", LeaderID: "source", LeaderEpoch: 1, LifecycleEpoch: 1, CommittedHWM: 3, RecoveryProtocol: 3}
		exported, err := s.Export(context.Background(), fence, nil)
		require.NoError(t, err)
		return exported.Close()
	}))
}

func TestSnapshotCutViewCannotAuthorizeVerifiedReplicaRepair(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	_, witness := replicaWitness(t, p, 7)
	require.NoError(t, p.AcceptReplicaCatchupWitness(witness, "leader", 1, 1, 3))
	// Establish that both helpers work on the ordinary read view, so snapshot
	// rejection below is specifically a purpose check, not missing verification.
	require.NoError(t, p.WithReadView(func(v *PartitionReadView) error {
		generation, err := v.ReplicaRepairGeneration(1, 1, 3, 7)
		require.NoError(t, err)
		release, err := v.AcquireReplicaRepairPublication(1, 1, 3, 7, generation)
		require.NoError(t, err)
		release()
		return nil
	}))
	check := func(c *SnapshotReadCut) error {
		_, err := c.View().ReplicaRepairGeneration(1, 1, 3, 7)
		require.ErrorContains(t, err, "cannot authorize replica repair publication")
		release, err := c.View().AcquireReplicaRepairPublication(1, 1, 3, 7, c.ContentGeneration())
		require.ErrorContains(t, err, "cannot authorize replica repair publication")
		require.Nil(t, release)
		return nil
	}
	require.NoError(t, p.WithSnapshotCertification(context.Background(), check))
	require.NoError(t, p.WithReplicaSnapshotCertification(context.Background(), witness, "leader", 1, 1, 3, check))
	require.NoError(t, p.WithBootstrapSource(func(source *BootstrapSource) error {
		cut, err := source.SnapshotReadCut()
		require.NoError(t, err)
		return check(cut)
	}))
}
