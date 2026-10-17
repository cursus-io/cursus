package fsm

import (
	"bytes"
	"context"
	"errors"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSourceCertifiedBytePageDoesNotInferHWM(t *testing.T) {
	payload := strings.Repeat("x", 24<<20)
	f, _ := indexedRecoveryFSM(t, []types.Message{{Offset: 0, Payload: payload}, {Offset: 1, Payload: payload}, {Offset: 2, Payload: payload}})
	request := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4, LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, CommittedHWM: 3, MaxRecords: MaxReplicaCatchupRecords}
	batch, err := f.FetchReplicaCatchup(request)
	require.NoError(t, err)
	require.Equal(t, 2, len(batch.Messages))
	require.Equal(t, uint64(2), batch.EndOffset)
	require.False(t, batch.Verified)
	require.False(t, batch.Compacted)
	require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
}

func TestSourceRetainedCursorWorkWithPreviousRecordDigest(t *testing.T) {
	const count = 1024
	messages := make([]types.Message, count)
	for i := range messages {
		messages[i] = types.Message{Offset: uint64(i), Payload: strings.Repeat("x", 4096)}
	}
	var provider *sourceMutationProvider
	f, _ := indexedRecoveryFSMWithProvider(t, messages, func(dm *disk.DiskManager) topic.HandlerProvider {
		provider = &sourceMutationProvider{DiskManager: dm}
		return provider
	})
	r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4, LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, CommittedHWM: count, MaxRecords: 7}
	for r.NextOffset < count {
		batch, err := f.FetchReplicaCatchup(r)
		require.NoError(t, err)
		require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
		require.Equal(t, r.NextOffset, batch.Messages[0].Offset)
		r.PreviousRecordDigest, err = replicaRecordDigest(batch.Messages[len(batch.Messages)-1])
		require.NoError(t, err)
		r.NextOffset = batch.EndOffset
	}
	work := provider.storage.RetainedPageWorkSnapshot()
	t.Logf("source catch-up including previous-record digests: %+v", work)
	info, err := os.Stat(provider.storage.GetSegmentPath(0))
	require.NoError(t, err)
	require.Greater(t, work.BytesRead, uint64(info.Size()))
	require.Less(t, work.BytesRead, uint64(info.Size()*2), "source previous-record proofs must reuse the certified position: %+v", work)
	require.Less(t, work.DecodedFrames, uint64(count*2))
	require.Greater(t, work.CacheHits, uint64(0))
	require.LessOrEqual(t, work.PeakCheckpoints, uint64(128))
}

func TestSourceContextCancelsFenceWaits(t *testing.T) {
	f, _ := indexedRecoveryFSM(t, nil)
	api, ok := any(f).(interface {
		FetchReplicaCatchupContext(context.Context, ReplicaCatchupRequest) (ReplicaCatchupBatch, error)
	})
	require.True(t, ok, "source must expose cancellable proof requests")
	for _, materialization := range []bool{false, true} {
		if materialization {
			f.materializationMu.Lock()
		} else {
			f.transitionMu.Lock()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := api.FetchReplicaCatchupContext(ctx, ReplicaCatchupRequest{})
		cancel()
		if materialization {
			f.materializationMu.Unlock()
		} else {
			f.transitionMu.Unlock()
		}
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

type sourceMutationStorage struct {
	*disk.DiskHandler
	afterMatch func()
	afterPage  func()
	matchErr   error
	pageErr    error
	offsetWait <-chan struct{}
}

func (s *sourceMutationStorage) MatchLineageCandidates(c []disk.LineagePrefix, h uint64) (*disk.LineagePrefix, error) {
	p, err := s.DiskHandler.MatchLineageCandidates(c, h)
	if s.afterMatch != nil {
		s.afterMatch()
	}
	if s.matchErr != nil {
		return nil, s.matchErr
	}
	return p, err
}
func (s *sourceMutationStorage) MatchLineageCandidatesContext(ctx context.Context, c []disk.LineagePrefix, h uint64) (*disk.LineagePrefix, error) {
	api := any(s.DiskHandler).(interface {
		MatchLineageCandidatesContext(context.Context, []disk.LineagePrefix, uint64) (*disk.LineagePrefix, error)
	})
	p, err := api.MatchLineageCandidatesContext(ctx, c, h)
	if s.afterMatch != nil {
		s.afterMatch()
	}
	if s.matchErr != nil {
		return nil, s.matchErr
	}
	return p, err
}
func (s *sourceMutationStorage) ReadRetainedPage(ctx context.Context, start, end uint64, n, b int) (disk.RetainedPage, error) {
	if s.pageErr != nil {
		return disk.RetainedPage{}, s.pageErr
	}
	p, err := s.DiskHandler.ReadRetainedPage(ctx, start, end, n, b)
	if s.afterPage != nil {
		s.afterPage()
	}
	return p, err
}

type sourceMutationProvider struct {
	*disk.DiskManager
	storage  *sourceMutationStorage
	hide     bool
	hidePage bool
}

func (p *sourceMutationProvider) GetHandlerWithPolicy(name string, id int, policy string, hours int, bytes int64) (types.StorageHandler, error) {
	h, err := p.DiskManager.GetHandlerWithPolicy(name, id, policy, hours, bytes)
	if err != nil {
		return nil, err
	}
	p.storage = &sourceMutationStorage{DiskHandler: h.(*disk.DiskHandler)}
	if p.hidePage {
		return &generationOnlyStorage{StorageHandler: h, disk: h.(*disk.DiskHandler)}, nil
	}
	if p.hide {
		return struct{ types.StorageHandler }{h}, nil
	}
	return p.storage, nil
}
func TestSourceRejectsChangedGenerationAndProofFailures(t *testing.T) {
	for _, stage := range []string{"normal proof", "bootstrap proof", "truncate proof", "normal page", "proof I/O"} {
		t.Run(stage, func(t *testing.T) {
			var provider *sourceMutationProvider
			f, p := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "one"}, {Offset: 1, Payload: "two"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &sourceMutationProvider{DiskManager: dm}
				return provider
			})
			for _, b := range f.brokers {
				b.RecoveryProtocol = 1
			}
			r := f.BuildReplicaCatchupRequests("node-2")[0]
			r.PreviousRecordDigest = ""
			r.NextOffset = 0
			var err error
			r.PrefixCandidates, err = p.ReplicaLineageCandidates(0)
			require.NoError(t, err)
			if stage == "bootstrap proof" {
				r.NextOffset = 2
				r.PrefixCandidates = []disk.LineagePrefix{{StartOffset: 1, EndOffset: 2, Digest: strings.Repeat("a", 64)}}
			}
			if stage == "truncate proof" {
				r.NextOffset = 2
				r.PrefixCandidates = append([]disk.LineagePrefix{{EndOffset: 2, Digest: strings.Repeat("a", 64)}}, r.PrefixCandidates...)
			}
			mutate := func() {
				require.NoError(t, provider.storage.TruncateTo(0))
				require.NoError(t, provider.storage.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 0, Payload: "replacement"}, {Topic: "orders", Offset: 1, Payload: "two"}}))
			}
			if stage == "normal page" {
				provider.storage.afterPage = mutate
			} else {
				provider.storage.afterMatch = mutate
			}
			if stage == "proof I/O" {
				provider.storage.afterMatch = nil
				provider.storage.matchErr = errors.New("proof I/O failed")
			}
			batch, err := f.FetchReplicaCatchup(r)
			require.Error(t, err)
			if stage != "proof I/O" {
				require.ErrorIs(t, err, disk.ErrStaleContentGeneration)
			}
			require.False(t, batch.BootstrapRequired)
			require.Empty(t, batch.Digest)
		})
	}
}
func TestSourceMissingOptionalAuthorityFailsClosed(t *testing.T) {
	f, _ := indexedRecoveryFSMWithProvider(t, nil, func(dm *disk.DiskManager) topic.HandlerProvider {
		return &sourceMutationProvider{DiskManager: dm, hide: true}
	})
	require.Empty(t, f.BuildReplicaCatchupRequests("node-2"), "unsupported local storage cannot capture receiver authority")
	meta := f.GetPartitionMetadata("orders-0")
	// Source-side validation receives only the remote wire request; its local
	// handler deliberately lacks authority, independently of receiver capture.
	r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: meta.Leader, SourceBroker: "node-1", LeaderEpoch: meta.LeaderEpoch, LifecycleEpoch: meta.LifecycleEpoch, CommittedHWM: meta.CommittedHWM, MaxRecords: MaxReplicaCatchupRecords}
	batch, err := f.FetchReplicaCatchup(r)
	require.Error(t, err)
	require.Empty(t, batch.Digest)
}

func TestSourceCertifiedSparseRangesAndRawMarkers(t *testing.T) {
	for _, tc := range []struct {
		name     string
		start    uint64
		max      int
		offsets  []uint64
		end      uint64
		verified bool
	}{
		{"sparse cutoff", 0, 1, []uint64{2}, 5, false},
		{"trailing holes", 5, 4, []uint64{5, 6}, 10, true},
		{"all holes", 7, 4, nil, 10, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var provider *sourceMutationProvider
			f, p := indexedRecoveryFSMWithProvider(t, nil, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &sourceMutationProvider{DiskManager: dm}
				return provider
			})
			records := []types.DiskMessage{
				{Topic: "orders", Offset: 2, Payload: "aborted", TransactionalID: "tx", TransactionState: types.TransactionStateAborted},
				{Topic: "orders", Offset: 5, TransactionalID: "tx", TransactionMarker: types.TransactionMarkerAbort},
				{Topic: "orders", Offset: 6, Payload: "open", TransactionalID: "open", TransactionState: types.TransactionStateOpen},
			}
			require.NoError(t, provider.storage.WriteCompactedReplicaRange(0, 10, records))
			p.UpdateLEO(10)
			require.NoError(t, p.ApplyReplicaHWM(10))
			f.partitionMetadata["orders-0"].CommittedHWM = 10
			f.topicState["orders"].Policy.CleanupPolicy = config.CleanupPolicyCompact
			r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, NextOffset: tc.start, CommittedHWM: 10, MaxRecords: tc.max}
			batch, err := f.FetchReplicaCatchup(r)
			require.NoError(t, err)
			var offsets []uint64
			for _, m := range batch.Messages {
				offsets = append(offsets, m.Offset)
			}
			require.Equal(t, tc.offsets, offsets)
			require.Equal(t, tc.end, batch.EndOffset)
			require.Equal(t, tc.verified, batch.Verified)
			require.True(t, batch.Compacted)
			require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
			if tc.start == 0 {
				require.Equal(t, types.TransactionStateAborted, batch.Messages[0].TransactionState)
			}
			if tc.start == 5 {
				require.Equal(t, types.TransactionMarkerAbort, batch.Messages[0].TransactionMarker)
				require.Equal(t, types.TransactionStateOpen, batch.Messages[1].TransactionState)
			}
			f.topicState["orders"].Policy.CleanupPolicy = config.CleanupPolicyDelete
			_, err = f.FetchReplicaCatchup(r)
			require.ErrorContains(t, err, "uncompacted")
		})
	}
}

func TestSourceCorruptHistoryDoesNotAuthorizeBootstrap(t *testing.T) {
	var provider *sourceMutationProvider
	f, p := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "one"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
		provider = &sourceMutationProvider{DiskManager: dm}
		return provider
	})
	for _, b := range f.brokers {
		b.RecoveryProtocol = 1
	}
	r := f.BuildReplicaCatchupRequests("node-2")[0]
	r.PreviousRecordDigest = ""
	r.PrefixCandidates = []disk.LineagePrefix{{StartOffset: 1, EndOffset: 1, Digest: strings.Repeat("a", 64)}}
	// Force a fresh proof scan of truncated physical bytes, not a derived cache.
	require.NoError(t, os.Remove(provider.storage.BaseName+".lineage"))
	require.NoError(t, os.Truncate(provider.storage.GetSegmentPath(0), 2))
	batch, err := f.FetchReplicaCatchup(r)
	require.Error(t, err)
	require.False(t, batch.BootstrapRequired)
	require.Empty(t, batch.Digest)
	require.Equal(t, uint64(1), p.NextOffset())
}

func TestSourceRestoreWaitsForSealedPageInBothModes(t *testing.T) {
	for _, mode := range []int{0, 1} {
		t.Run(string(rune('0'+mode)), func(t *testing.T) {
			var provider *sourceMutationProvider
			f, p := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "before restore"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &sourceMutationProvider{DiskManager: dm}
				return provider
			})
			for _, b := range f.brokers {
				b.RecoveryProtocol = 1
				b.LifecycleProtocol = BrokerProtocolVersionCurrent
			}
			r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, CommittedHWM: 1, MaxRecords: 1, RecoveryProtocol: mode}
			var err error
			if mode == 1 {
				r.PrefixCandidates, err = p.ReplicaLineageCandidates(0)
				require.NoError(t, err)
			}
			snapshot, err := f.Snapshot()
			require.NoError(t, err)
			defer snapshot.Release()
			buf := new(bytes.Buffer)
			require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: buf}))
			entered, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			provider.storage.afterPage = func() { close(entered); <-release }
			fetched := make(chan error, 1)
			go func() {
				batch, err := f.FetchReplicaCatchup(r)
				if err == nil {
					err = ValidateReplicaCatchupBatchDigest(batch)
				}
				fetched <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("source did not reach page")
			}
			restored := make(chan error, 1)
			go func() { restored <- f.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))) }()
			select {
			case err := <-restored:
				t.Fatalf("restore crossed source fence: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			close(release)
			require.NoError(t, <-fetched)
			require.NoError(t, <-restored)
		})
	}
}

type generationOnlyStorage struct {
	types.StorageHandler
	disk *disk.DiskHandler
}

func (s *generationOnlyStorage) CaptureContentGenerationContext(ctx context.Context) (disk.ContentGeneration, error) {
	return s.disk.CaptureContentGenerationContext(ctx)
}
func (s *generationOnlyStorage) CheckContentGenerationContext(ctx context.Context, g disk.ContentGeneration) error {
	return s.disk.CheckContentGenerationContext(ctx, g)
}
func TestSourceMissingPageAuthorityFailsClosed(t *testing.T) {
	f, _ := indexedRecoveryFSMWithProvider(t, nil, func(dm *disk.DiskManager) topic.HandlerProvider {
		return &sourceMutationProvider{DiskManager: dm, hidePage: true}
	})
	require.Empty(t, f.BuildReplicaCatchupRequests("node-2"), "incomplete local storage authority cannot capture receiver proof")
	meta := f.GetPartitionMetadata("orders-0")
	// Model the remote wire request independently of unsupported local receiver storage.
	r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: meta.Leader, SourceBroker: "node-1", LeaderEpoch: meta.LeaderEpoch, LifecycleEpoch: meta.LifecycleEpoch, CommittedHWM: meta.CommittedHWM, MaxRecords: MaxReplicaCatchupRecords}
	batch, err := f.FetchReplicaCatchup(r)
	require.ErrorIs(t, err, topic.ErrReplicaSourceUnsupported)
	require.Empty(t, batch.Digest)
}

func TestSourceCertifiedPagePreservesLegacyRecordRepresentation(t *testing.T) {
	f, p := indexedRecoveryFSM(t, []types.Message{{Offset: 0, Payload: "one", LeaderEpoch: 4}, {Offset: 1, Payload: "two", LeaderEpoch: 4}})
	legacy, err := p.ReadMessages(0, 2)
	require.NoError(t, err)
	r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, CommittedHWM: 2, MaxRecords: 2}
	batch, err := f.FetchReplicaCatchup(r)
	require.NoError(t, err)
	require.Equal(t, legacy, batch.Messages, "every legacy record field participates in the digest")
	r.NextOffset = 1
	r.PreviousRecordDigest, err = replicaRecordDigest(legacy[0])
	require.NoError(t, err)
	batch, err = f.FetchReplicaCatchup(r)
	require.NoError(t, err)
	require.Nil(t, batch.TruncateTo)
	require.Equal(t, legacy[1:], batch.Messages)
}

func TestSourcePreviousRecordReadPreservesCancellation(t *testing.T) {
	var provider *sourceMutationProvider
	f, p := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "one"}}, func(dm *disk.DiskManager) topic.HandlerProvider {
		provider = &sourceMutationProvider{DiskManager: dm}
		return provider
	})
	previous, err := p.ReadMessages(0, 1)
	require.NoError(t, err)
	digest, err := replicaRecordDigest(previous[0])
	require.NoError(t, err)
	provider.storage.pageErr = context.Canceled
	r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, NextOffset: 1, CommittedHWM: 1, MaxRecords: 1, PreviousRecordDigest: digest}
	batch, err := f.FetchReplicaCatchup(r)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, batch.Digest)
}

func (s *sourceMutationStorage) GetFirstOffset() uint64 {
	if s.offsetWait != nil {
		<-s.offsetWait
	}
	return s.DiskHandler.GetFirstOffset()
}
func (s *sourceMutationStorage) GetFirstOffsetContext(ctx context.Context) (uint64, error) {
	if s.offsetWait != nil {
		select {
		case <-s.offsetWait:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return any(s.DiskHandler).(interface {
		GetFirstOffsetContext(context.Context) (uint64, error)
	}).GetFirstOffsetContext(ctx)
}
func TestSourceLegacyEpochScanCancelsRetainedStartWait(t *testing.T) {
	var provider *sourceMutationProvider
	f, _ := indexedRecoveryFSMWithProvider(t, []types.Message{{Offset: 0, Payload: "one", LeaderEpoch: 4}}, func(dm *disk.DiskManager) topic.HandlerProvider {
		provider = &sourceMutationProvider{DiskManager: dm}
		return provider
	})
	release := make(chan struct{})
	provider.storage.offsetWait = release
	r := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, NextOffset: 1, CommittedHWM: 1, MaxRecords: 1, PreviousRecordDigest: strings.Repeat("a", 64), PreviousLeaderEpoch: 4}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := f.FetchReplicaCatchupContext(ctx, r); result <- err }()
	select {
	case err := <-result:
		close(release)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(200 * time.Millisecond):
		close(release)
		<-result
		t.Fatal("legacy epoch scan waited on non-contextual retained-start accessor")
	}
}

type sourceBlockingWriteStorage struct {
	*disk.DiskHandler
	entered chan struct{}
	release chan struct{}
}

func (s *sourceBlockingWriteStorage) WriteBatchSync(records []types.DiskMessage) error {
	close(s.entered)
	<-s.release
	return s.DiskHandler.WriteBatchSync(records)
}

type sourceBlockingWriteProvider struct {
	*disk.DiskManager
	entered chan struct{}
	release chan struct{}
}

func (p *sourceBlockingWriteProvider) GetHandlerWithPolicy(name string, id int, policy string, hours int, bytes int64) (types.StorageHandler, error) {
	handler, err := p.DiskManager.GetHandlerWithPolicy(name, id, policy, hours, bytes)
	if err != nil {
		return nil, err
	}
	return &sourceBlockingWriteStorage{DiskHandler: handler.(*disk.DiskHandler), entered: p.entered, release: p.release}, nil
}

func TestSourceDeadlineReleasesBothFencesBehindLeaderWrite(t *testing.T) {
	for _, mode := range []int{0, 1} {
		t.Run(string(rune('0'+mode)), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			f, p := indexedRecoveryFSMWithProvider(t, nil, func(dm *disk.DiskManager) topic.HandlerProvider {
				return &sourceBlockingWriteProvider{DiskManager: dm, entered: entered, release: release}
			})
			for _, b := range f.brokers {
				b.RecoveryProtocol = 1
			}
			request := ReplicaCatchupRequest{Topic: "orders", BrokerID: "node-2", Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: f.partitionMetadata["orders-0"].LifecycleEpoch, MaxRecords: 1, RecoveryProtocol: mode}
			if mode == 1 {
				var err error
				request.PrefixCandidates, err = p.ReplicaLineageCandidates(0)
				require.NoError(t, err)
			}
			written := make(chan error, 1)
			fetched := make(chan error, 1)
			fetchStarted, fetchFinished := false, false
			go func() {
				written <- p.EnqueueBatchLeaderWithMode([]types.Message{{Payload: "in-flight", LeaderEpoch: 4}}, false)
			}()
			defer func() {
				close(release)
				require.NoError(t, <-written)
				if fetchStarted && !fetchFinished {
					<-fetched
				}
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("leader did not enter durable storage write")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			fetchStarted = true
			go func() { _, err := f.FetchReplicaCatchupContext(ctx, request); fetched <- err }()
			// Only the source fetch can hold these fences in this fixture. Observe both
			// before the deadline so an already-expired request cannot satisfy the test.
			require.Eventually(t, func() bool {
				transitionFree := f.transitionMu.TryLock()
				if transitionFree {
					f.transitionMu.Unlock()
				}
				materializationFree := f.materializationMu.TryLock()
				if materializationFree {
					f.materializationMu.Unlock()
				}
				return !transitionFree && !materializationFree
			}, 100*time.Millisecond, time.Millisecond)
			select {
			case err := <-fetched:
				fetchFinished = true
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(time.Second):
				t.Error("expired source request waited for blocked leader write")
			}
			// The write remains blocked until the deferred release, including here.
			transitionFree := f.transitionMu.TryLock()
			if transitionFree {
				defer f.transitionMu.Unlock()
			}
			materializationFree := f.materializationMu.TryLock()
			if materializationFree {
				defer f.materializationMu.Unlock()
			}
			if !transitionFree {
				t.Error("deadline must release transition fence before leader write finishes")
			}
			if !materializationFree {
				t.Error("deadline must release materialization fence before leader write finishes")
			}
		})
	}
}
