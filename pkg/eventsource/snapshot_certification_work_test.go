package eventsource

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type snapshotWorkStorage struct {
	*disk.DiskHandler
	records, bytes atomic.Int64
	afterPage      func()
	anchor         *uint64
}

func (s *snapshotWorkStorage) GetFirstOffsetContext(ctx context.Context) (uint64, error) {
	if s.anchor != nil {
		return *s.anchor, ctx.Err()
	}
	return s.DiskHandler.GetFirstOffsetContext(ctx)
}

func (s *snapshotWorkStorage) ReadRetainedPage(ctx context.Context, start, end uint64, records, bytes int) (disk.RetainedPage, error) {
	page, err := s.DiskHandler.ReadRetainedPage(ctx, start, end, records, bytes)
	s.records.Add(int64(len(page.Messages)))
	s.bytes.Add(int64(page.DecodedBytes))
	if s.afterPage != nil {
		s.afterPage()
	}
	return page, err
}

type snapshotWorkProvider struct{ storage *snapshotWorkStorage }

func (p *snapshotWorkProvider) GetHandler(string, int) (types.StorageHandler, error) {
	return p.storage, nil
}
func newSnapshotWorkHandler(t *testing.T) (*Handler, *topic.Partition, *snapshotWorkStorage) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes, cfg.DiskMinFreePercent = 1, 0.1
	d, err := disk.NewDiskHandler(cfg, "orders", 0)
	require.NoError(t, err)
	storage := &snapshotWorkStorage{DiskHandler: d}
	tm := topic.NewTopicManager(cfg, &snapshotWorkProvider{storage}, nil)
	require.NoError(t, tm.CreateTopic("orders", 1, false, true))
	h := NewHandler(tm)
	t.Cleanup(func() { require.NoError(t, h.Close()); tm.Stop(); require.NoError(t, d.Close()) })
	p, err := tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	return h, p, storage
}
func certifyTestCut(t *testing.T, h *Handler, p *topic.Partition) (*certifiedSnapshotIndex, error) {
	t.Helper()
	var proof *certifiedSnapshotIndex
	err := p.WithSnapshotCertification(context.Background(), func(cut *topic.SnapshotReadCut) error {
		h.indexSyncMu.Lock()
		defer h.indexSyncMu.Unlock()
		var err error
		proof, err = h.certifySnapshotIndex(context.Background(), "orders", 0, cut)
		return err
	})
	return proof, err
}

func TestSnapshotCertificationActualRawWork(t *testing.T) {
	h, p, s := newSnapshotWorkHandler(t)
	records := make([]types.Message, 128)
	for i := range records {
		records[i] = types.Message{Offset: uint64(i), Key: "a", AggregateVersion: uint64(i + 1), Payload: strings.Repeat("x", 1024)}
	}
	require.NoError(t, p.ReplicaAppend(records))
	require.NoError(t, p.ApplyReplicaHWM(128))
	_, err := certifyTestCut(t, h, p)
	require.NoError(t, err)
	initialRecords, initialBytes := s.records.Load(), s.bytes.Load()
	require.Equal(t, int64(256), initialRecords)
	s.records.Store(0)
	s.bytes.Store(0)
	manager := transaction.NewManager()
	p.SetTransactionDecisionResolver(manager)
	_, _, err = manager.InitProducer("unrelated")
	require.NoError(t, err)
	_, err = certifyTestCut(t, h, p)
	require.NoError(t, err)
	require.Zero(t, s.records.Load())
	require.Zero(t, s.bytes.Load())
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 128, Key: "a", AggregateVersion: 129, Payload: strings.Repeat("y", 1024)}}))
	require.NoError(t, p.ApplyReplicaHWM(129))
	_, err = certifyTestCut(t, h, p)
	require.NoError(t, err)
	require.Equal(t, int64(2), s.records.Load())
	require.Less(t, s.bytes.Load(), initialBytes)
	t.Logf("cold classification+replay %d records/%d bytes; same cut 0; suffix classification+replay %d records/%d bytes", initialRecords, initialBytes, s.records.Load(), s.bytes.Load())
}

func TestSnapshotCertificationTransactionalReplayAndDecisionFailure(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "full-repeat", true: "mid-page-decision"}[failure], func(t *testing.T) {
			h, p, s := newSnapshotWorkHandler(t)
			records := []types.Message{{Offset: 0, Key: "a", AggregateVersion: 1, TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen}, {Offset: 1, TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit}}
			require.NoError(t, p.ReplicaAppend(records))
			require.NoError(t, p.ApplyReplicaHWM(2))
			manager := transaction.NewManager()
			p.SetTransactionDecisionResolver(manager)
			proof, err := certifyTestCut(t, h, p)
			require.NoError(t, err)
			require.True(t, proof.transactional)
			s.records.Store(0)
			s.bytes.Store(0)
			if failure {
				s.afterPage = func() {
					s.afterPage = nil
					require.NoError(t, manager.ImportState(map[string]*transaction.Snapshot{"tx": {ID: "tx", Producer: "p", Epoch: 1, State: transaction.StateOpen, Revision: 1, CreatedAt: time.Unix(100, 0), UpdatedAt: time.Unix(100, 0)}}))
				}
			}
			_, err = certifyTestCut(t, h, p)
			if failure {
				require.Error(t, err)
				require.ErrorIs(t, proof.idx.checkForTest(), ErrStreamIndexInvalid)
				require.Nil(t, h.indexes["orders:0"])
			} else {
				require.NoError(t, err)
				require.Equal(t, int64(4), s.records.Load())
				require.Positive(t, s.bytes.Load())
			}
		})
	}
}
func (si *StreamIndex) checkForTest() error { _, err := si.ResetRevision(); return err }

func TestSnapshotCertificationRetentionNeverSelfSeeds(t *testing.T) {
	for _, checkpoint := range []uint64{0, 1, 3} {
		t.Run(map[uint64]string{0: "candidate-only", 1: "accepted-checkpoint", 3: "disagrees"}[checkpoint], func(t *testing.T) {
			h, p, s := newSnapshotWorkHandler(t)
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1}, {Offset: 1, Key: "a", AggregateVersion: 2}}))
			require.NoError(t, p.ApplyReplicaHWM(2))
			if checkpoint == 1 {
				require.Empty(t, h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "accepted"}))
			}
			if checkpoint == 3 {
				require.NoError(t, seedStaleSnapshotFixture(h, SnapshotResult{Topic: "orders", Key: "a", Version: 3, Payload: "stale"}))
			}
			anchor := uint64(1)
			s.anchor = &anchor
			response := h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 2, Payload: "candidate"})
			if checkpoint == 1 {
				require.Empty(t, response)
			} else {
				require.Contains(t, response, "checkpoint")
				require.Nil(t, h.indexes["orders:0"])
			}
		})
	}
}

func TestSnapshotCertificationRebuildsResetGenerationAndReplacement(t *testing.T) {
	for _, change := range []string{"reset", "generation", "replacement", "seed"} {
		t.Run(change, func(t *testing.T) {
			h, p, s := newSnapshotWorkHandler(t)
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1}}))
			require.NoError(t, p.ApplyReplicaHWM(1))
			proof, err := certifyTestCut(t, h, p)
			require.NoError(t, err)
			s.records.Store(0)
			switch change {
			case "reset":
				require.NoError(t, proof.idx.Reset())
			case "generation":
				require.NoError(t, s.TruncateTo(0))
				require.NoError(t, s.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 0, Key: "a", AggregateVersion: 1, Payload: "rewritten"}}))
			case "replacement":
				require.NoError(t, h.DetachPartitionForBootstrap("orders", 0))
			case "seed":
				require.NoError(t, proof.idx.SeedVersion("a", 99))
			}
			current, err := certifyTestCut(t, h, p)
			require.NoError(t, err)
			require.Equal(t, int64(2), s.records.Load())
			v, err := current.idx.VersionBefore("a", 1)
			require.NoError(t, err)
			require.Equal(t, uint64(1), v)
		})
	}
}

func TestSnapshotCertificationReevaluatesUnknownTransactionDecision(t *testing.T) {
	h, p, _ := newSnapshotWorkHandler(t)
	manager := transaction.NewManager()
	p.SetTransactionDecisionResolver(manager)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1, TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen}, {Offset: 1, TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit}}))
	require.NoError(t, p.ApplyReplicaHWM(2))
	// Unknown live state uses the retained terminal marker, but is not a reusable
	// visibility authorization after the resolver learns a live nonterminal state.
	proof, err := certifyTestCut(t, h, p)
	require.NoError(t, err)
	require.NoError(t, manager.ImportState(map[string]*transaction.Snapshot{"tx": {ID: "tx", Producer: "p", Epoch: 1, State: transaction.StateOpen, Revision: 1, CreatedAt: time.Unix(100, 0), UpdatedAt: time.Unix(100, 0)}}))
	response := h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "bad"})
	require.Contains(t, response, "snapshot_version_exceeds_stream")
	require.Same(t, proof.idx, h.indexes["orders:0"], "candidate rejection alone does not retire the valid cache")
}

type unsupportedSnapshotResolver struct{}

func (*unsupportedSnapshotResolver) TransactionDecision(string, int64) (string, bool) {
	return "", false
}

func TestSnapshotCertificationClassifiesHiddenRawEvidence(t *testing.T) {
	for _, kind := range []string{"ordinary", "aborted", "hidden-control"} {
		t.Run(kind, func(t *testing.T) {
			h, p, _ := newSnapshotWorkHandler(t)
			records := []types.Message{{Offset: 0, Key: "a", AggregateVersion: 1}}
			version := uint64(1)
			switch kind {
			case "aborted":
				records = []types.Message{{Offset: 0, Key: "a", AggregateVersion: 1, TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen}, {Offset: 1, TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerAbort}}
			case "hidden-control":
				records = []types.Message{{Offset: 0, ControlBatchType: types.ControlBatchTransaction}}
				version = 0
			}
			require.NoError(t, p.ReplicaAppend(records))
			require.NoError(t, p.ApplyReplicaHWM(uint64(len(records))))
			if kind != "aborted" {
				p.SetTransactionDecisionResolver(&unsupportedSnapshotResolver{})
			}
			response := h.SaveSnapshotReplica(SnapshotResult{Topic: "orders", Key: "a", Version: version, Payload: "snapshot"})
			switch kind {
			case "ordinary":
				require.Empty(t, response)
			case "aborted":
				require.Contains(t, response, "snapshot_version_exceeds_stream")
			case "hidden-control":
				require.Contains(t, response, "provenance unavailable")
			}
		})
	}
}

func TestSnapshotCertificationPartialReplayRetiresThenColdAppend(t *testing.T) {
	h, p, _ := newSnapshotWorkHandler(t)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1}}))
	require.NoError(t, p.ApplyReplicaHWM(1))
	proof, err := certifyTestCut(t, h, p)
	require.NoError(t, err)
	require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 1, Key: "a", AggregateVersion: 3}}))
	require.NoError(t, p.ApplyReplicaHWM(2))
	_, err = certifyTestCut(t, h, p)
	require.Error(t, err)
	require.ErrorIs(t, proof.idx.checkForTest(), ErrStreamIndexInvalid)
	require.Nil(t, h.indexes["orders:0"])
	_, response := h.AppendStream("APPEND_STREAM topic=orders key=a version=2 message=bad", AppendOptions{})
	require.Contains(t, response, "gap")
}

func TestPreparedReplicaSnapshotUsesActualWitnessAndRejectsStoreReplacement(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "actual", true: "store"}[replace], func(t *testing.T) {
			h, p, _ := newSnapshotWorkHandler(t)
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1}}))
			require.NoError(t, p.ApplyReplicaHWM(1))
			p.MarkReconciliationPending(errors.New("pending"))
			release := p.BeginReplicationMutation()
			request, err := p.CaptureReplicaRequestWitness(context.Background(), 1)
			require.NoError(t, err)
			witness, err := p.CompleteReplicaCatchupVerification(context.Background(), request, "leader", 1, 1, 1, 0)
			require.NoError(t, err)
			release()
			candidate := SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "snapshot"}
			require.Contains(t, h.SaveSnapshotReplica(candidate), "pending")
			var prepared *PreparedReplicaSnapshot
			require.NoError(t, p.WithReplicaSnapshotCertification(context.Background(), witness, "leader", 1, 1, 1, func(cut *topic.SnapshotReadCut) error {
				var err error
				prepared, err = h.PrepareReplicaSnapshot(context.Background(), candidate, cut)
				return err
			}))
			candidate.Payload = "mutated caller copy"
			if replace {
				h.mu.Lock()
				h.snapshots["orders:0"] = nil
				h.mu.Unlock()
				defer func() { h.mu.Lock(); h.snapshots["orders:0"] = prepared.store; h.mu.Unlock() }()
			}
			err = h.PublishPreparedReplicaSnapshot(context.Background(), p, prepared, witness, "leader", 1, 1, 1)
			if replace {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				record, err := prepared.store.Read("a")
				require.NoError(t, err)
				require.Equal(t, "snapshot", record.Payload)
			}
			require.Error(t, h.PublishPreparedReplicaSnapshot(context.Background(), p, prepared, witness, "leader", 1, 1, 1))
		})
	}
}

type publicationDecisionResolver struct {
	*transaction.Manager
	once       sync.Once
	afterCheck func()
}

func (r *publicationDecisionResolver) CheckTransactionDecisionGeneration(generation uint64) bool {
	ok := r.Manager.CheckTransactionDecisionGeneration(generation)
	if r.afterCheck != nil {
		r.once.Do(r.afterCheck)
	}
	return ok
}

func TestPreparedSnapshotRechecksOriginalDecisionAfterStoreWait(t *testing.T) {
	for _, phase := range []string{"before-write", "during-sync"} {
		t.Run(phase, func(t *testing.T) {
			h, p, _ := newSnapshotWorkHandler(t)
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1, TransactionalID: "tx", Epoch: 1, TransactionState: types.TransactionStateOpen}, {Offset: 1, TransactionalID: "tx", Epoch: 1, TransactionMarker: types.TransactionMarkerCommit}}))
			require.NoError(t, p.ApplyReplicaHWM(2))
			resolver := &publicationDecisionResolver{Manager: transaction.NewManager()}
			p.SetTransactionDecisionResolver(resolver)
			release := p.BeginReplicationMutation()
			request, err := p.CaptureReplicaRequestWitness(context.Background(), 2)
			require.NoError(t, err)
			witness, err := p.CompleteReplicaCatchupVerification(context.Background(), request, "leader", 1, 1, 2, 0)
			require.NoError(t, err)
			release()
			var prepared *PreparedReplicaSnapshot
			require.NoError(t, p.WithReplicaSnapshotCertification(context.Background(), witness, "leader", 1, 1, 2, func(cut *topic.SnapshotReadCut) error {
				var err error
				prepared, err = h.PrepareReplicaSnapshot(context.Background(), SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "snapshot"}, cut)
				return err
			}))
			checked := make(chan struct{})
			resolver.afterCheck = func() { close(checked) }
			var writes atomic.Int64
			prepared.store.syncFileFn = func() error {
				writes.Add(1)
				if err := prepared.store.file.Sync(); err != nil {
					return err
				}
				if phase == "during-sync" {
					_, _, err := resolver.InitProducer("changed-during-sync")
					return err
				}
				return nil
			}
			prepared.store.mu.Lock()
			done := make(chan error, 1)
			go func() {
				done <- h.PublishPreparedReplicaSnapshot(context.Background(), p, prepared, witness, "leader", 1, 1, 2)
			}()
			<-checked
			if phase == "before-write" {
				_, _, err = resolver.InitProducer("changed-while-waiting")
				require.NoError(t, err)
			}
			prepared.store.mu.Unlock()
			require.Error(t, <-done)
			record, err := prepared.store.Read("a")
			require.NoError(t, err)
			if phase == "before-write" {
				require.Zero(t, writes.Load())
				require.Nil(t, record)
			} else {
				require.Equal(t, int64(1), writes.Load())
				require.NotNil(t, record)
			}
			require.ErrorIs(t, prepared.index.idx.checkForTest(), ErrStreamIndexInvalid)
		})
	}
}

func TestPreparedSnapshotOuterCutFailureRetiresExactIndex(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "current", true: "replacement"}[replace], func(t *testing.T) {
			h, p, _ := newSnapshotWorkHandler(t)
			require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 0, Key: "a", AggregateVersion: 1}}))
			require.NoError(t, p.ApplyReplicaHWM(1))
			release := p.BeginReplicationMutation()
			request, err := p.CaptureReplicaRequestWitness(context.Background(), 1)
			require.NoError(t, err)
			witness, err := p.CompleteReplicaCatchupVerification(context.Background(), request, "leader", 1, 1, 1, 0)
			require.NoError(t, err)
			release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var prepared *PreparedReplicaSnapshot
			err = p.WithReplicaSnapshotCertification(ctx, witness, "leader", 1, 1, 1, func(cut *topic.SnapshotReadCut) error {
				var prepareErr error
				prepared, prepareErr = h.PrepareReplicaSnapshot(ctx, SnapshotResult{Topic: "orders", Key: "a", Version: 1, Payload: "snapshot"}, cut)
				if prepareErr != nil {
					return prepareErr
				}
				cancel() // The owning cut's final validation, after successful preparation.
				return nil
			})
			require.ErrorIs(t, err, context.Canceled)
			require.NotNil(t, prepared)
			var replacement *StreamIndex
			if replace {
				replacement, err = NewStreamIndex(t.TempDir(), 0)
				require.NoError(t, err)
				h.mu.Lock()
				h.indexes["orders:0"] = replacement
				h.mu.Unlock()
			}
			h.RetirePreparedReplicaSnapshot(prepared, context.Canceled)
			require.ErrorIs(t, prepared.index.idx.checkForTest(), ErrStreamIndexInvalid)
			require.Same(t, replacement, h.indexes["orders:0"])
			if replacement != nil {
				require.NoError(t, replacement.checkForTest())
			}
			record, readErr := prepared.store.Read("a")
			require.NoError(t, readErr)
			require.Nil(t, record)
			require.Error(t, h.PublishPreparedReplicaSnapshot(context.Background(), p, prepared, witness, "leader", 1, 1, 1))
		})
	}
}
