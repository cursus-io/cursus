package topic

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func metadataRoot(t *testing.T) *disk.DetachedBootstrapRoot {
	t.Helper()
	root, err := disk.NewDetachedBootstrapRoot(t.TempDir(), disk.DefaultBootstrapLimits(), 0, 0, 32)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	return root
}
func metadataPrepared(t *testing.T, root *disk.DetachedBootstrapRoot, start, end uint64, records []types.DiskMessage) *disk.DetachedRawStore {
	t.Helper()
	var raw []byte
	for _, record := range records {
		payload, err := util.SerializeDiskMessage(record)
		require.NoError(t, err)
		raw = binary.BigEndian.AppendUint32(raw, uint32(len(payload)))
		raw = append(raw, payload...)
	}
	var id [16]byte
	_, err := rand.Read(id[:])
	require.NoError(t, err)
	sum, empty := sha256.Sum256(raw), sha256.Sum256(nil)
	m := disk.BootstrapMetadata{Version: disk.BootstrapVersion, TransferID: hex.EncodeToString(id[:]), StartOffset: start, EndOffset: end, Compacted: true,
		Fence: disk.BootstrapFence{Topic: "orders", Partition: 1, ReceiverID: "receiver", SourceID: "source", LeaderID: "source", LeaderEpoch: 3, LifecycleEpoch: 7, CommittedHWM: end, RecoveryProtocol: 3},
		Files: []disk.BootstrapFile{{ID: "records", Role: "canonical-log", Size: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])}, {ID: "catalog", Role: "snapshot-catalog", SHA256: hex.EncodeToString(empty[:])}}}
	stage, err := root.Stage("orders", 1, m)
	require.NoError(t, err)
	for position := 0; position < len(raw); {
		end := min(position+disk.BootstrapChunkBytes, len(raw))
		require.NoError(t, stage.WriteChunk("records", int64(position), raw[position:end]))
		position = end
	}
	prepared, err := stage.Prepare(context.Background())
	require.NoError(t, err)
	return prepared
}
func metadataStore(t *testing.T, root *disk.DetachedBootstrapRoot, budget *DetachedMetadataBudget, end uint64, records []types.DiskMessage) *DetachedMetadataStore {
	t.Helper()
	raw, err := root.Adopt(metadataPrepared(t, root, 0, end, records))
	require.NoError(t, err)
	store, err := NewDetachedMetadataStore(context.Background(), raw, budget)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}
func metadataRecord(offset uint64, txn string, epoch int64, marker string) types.DiskMessage {
	return types.DiskMessage{Topic: "orders", Partition: 1, Offset: offset, TransactionalID: txn, Epoch: epoch, TransactionMarker: marker, ControlBatchCoordinatorEpoch: 7}
}
func metadataLease(t *testing.T, s *DetachedMetadataStore) *DetachedMetadataLease {
	t.Helper()
	l, err := s.Acquire()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, l.Close()) })
	return l
}
func metadataOffsets(records []types.Message) []uint64 {
	offsets := make([]uint64, len(records))
	for i, m := range records {
		offsets[i] = m.Offset
	}
	return offsets
}

func TestDetachedMetadataFixedLeaseHistoryAndReceiverDecisions(t *testing.T) {
	ctx := context.Background()
	root := metadataRoot(t)
	budget := NewDetachedMetadataBudget()
	s := metadataStore(t, root, budget, 3, []types.DiskMessage{metadataRecord(0, "txn", 1, ""), metadataRecord(1, "txn", 1, types.TransactionMarkerCommit), metadataRecord(2, "txn", 2, "")})
	old := metadataLease(t, s)
	suffix := metadataPrepared(t, root, 3, 6, []types.DiskMessage{metadataRecord(3, "txn", 1, types.TransactionMarkerAbort), metadataRecord(4, "txn", 2, types.TransactionMarkerCommit), metadataRecord(5, "", 0, "")})
	result, err := s.AppendPrepared(ctx, old.Bounds(), suffix)
	require.NoError(t, err)
	require.NoError(t, result.CleanupError)
	fresh := metadataLease(t, s)
	records, err := old.ReadCommittedMetadata(ctx, 0, 100, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, metadataOffsets(records))
	records, err = fresh.ReadCommittedMetadata(ctx, 0, 100, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 5}, metadataOffsets(records))
	resolver := &testCoordinatorEpochDecisionResolver{state: types.TransactionStateCommitted, coordinatorEpoch: 7, known: true}
	records, err = fresh.ReadCommittedMetadata(ctx, 0, 100, resolver)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 5}, metadataOffsets(records))
	resolver.coordinatorEpoch = 8
	records, err = fresh.ReadCommittedMetadata(ctx, 0, 100, resolver)
	require.NoError(t, err)
	require.Equal(t, []uint64{5}, metadataOffsets(records))
	resolver.known = false
	records, err = fresh.ReadCommittedMetadata(ctx, 0, 100, resolver)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 5}, metadataOffsets(records))
	resolver.known = true
	resolver.coordinatorEpoch = 7
	resolver.state = types.TransactionStateOpen
	records, err = fresh.ReadCommittedMetadata(ctx, 0, 100, resolver)
	require.NoError(t, err)
	require.Equal(t, []uint64{5}, metadataOffsets(records))
	// A marker exactly at the older exclusive end cannot resolve that lease.
	require.Equal(t, uint64(3), old.Bounds().EndOffset)
	require.Equal(t, uint64(3), s.Work().Initial.DecodedRecords)
	require.Equal(t, uint64(3), s.Work().Suffix.DecodedRecords)
	require.Equal(t, uint64(result.Bounds.RawBytes-old.Bounds().RawBytes), s.Work().Append.RawReadBytes)
}

func TestDetachedMetadataExclusiveEndAndRepeatedMarkers(t *testing.T) {
	ctx := context.Background()
	root := metadataRoot(t)
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 1, []types.DiskMessage{metadataRecord(0, "txn", 1, "")})
	old := metadataLease(t, s)
	_, err := s.AppendPrepared(ctx, old.Bounds(), metadataPrepared(t, root, 1, 2, []types.DiskMessage{metadataRecord(1, "txn", 1, types.TransactionMarkerCommit)}))
	require.NoError(t, err)
	fresh := metadataLease(t, s)
	records, err := old.ReadCommittedMetadata(ctx, 0, 10, nil)
	require.NoError(t, err)
	require.Empty(t, records)
	records, err = fresh.ReadCommittedMetadata(ctx, 0, 10, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, metadataOffsets(records))
	for end := uint64(2); end < 18; end++ {
		marker := types.TransactionMarkerCommit
		if end%2 == 0 {
			marker = types.TransactionMarkerAbort
		}
		bounds, err := s.raw.Bounds()
		require.NoError(t, err)
		_, err = s.AppendPrepared(ctx, bounds, metadataPrepared(t, root, end, end+1, []types.DiskMessage{metadataRecord(end, "txn", 1, marker)}))
		require.NoError(t, err)
	}
	records, err = fresh.ReadCommittedMetadata(ctx, 0, 10, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, metadataOffsets(records))
	require.Len(t, s.history.entries[transactionMarkerKey{"txn", 1}].entries, 17)
	require.Equal(t, uint64(17), s.Work().Suffix.DecodedRecords)
}

func TestDetachedMetadataFilteredPrefixHolesAndPageLimits(t *testing.T) {
	ctx := context.Background()
	root := metadataRoot(t)
	var records []types.DiskMessage
	for i := uint64(0); i < 1100; i++ {
		records = append(records, metadataRecord(i, "unresolved", 1, ""))
	}
	for _, offset := range []uint64{2000, 3000, 4000} {
		m := metadataRecord(offset, "", 0, "")
		m.Payload = strings.Repeat("x", 600000)
		records = append(records, m)
	}
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 5000, records)
	l := metadataLease(t, s)
	page, err := l.ReadCommittedMetadata(ctx, 0, 10000, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{2000}, metadataOffsets(page))
	require.GreaterOrEqual(t, s.Work().Reads.DecodedRecords, uint64(1102))
	page, err = l.ReadCommittedMetadata(ctx, 2001, 10000, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{3000}, metadataOffsets(page))
	require.Equal(t, records[1101].Payload, page[0].Payload)
	page, err = l.ReadCommittedMetadata(ctx, 3001, 1, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{4000}, metadataOffsets(page))
	page, err = l.ReadCommittedMetadata(ctx, 4001, 10, nil)
	require.NoError(t, err)
	require.Empty(t, page)
}
func TestDetachedMetadataOversizedFirstAndRecordCap(t *testing.T) {
	ctx := context.Background()
	root := metadataRoot(t)
	big := metadataRecord(0, "", 0, "")
	big.Payload = strings.Repeat("z", 2<<20)
	records := []types.DiskMessage{big}
	for i := uint64(1); i < 1100; i++ {
		records = append(records, metadataRecord(i, "", 0, ""))
	}
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 1100, records)
	l := metadataLease(t, s)
	page, err := l.ReadCommittedMetadata(ctx, 0, 2000, nil)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, big.Payload, page[0].Payload)
	require.Equal(t, uint64(2), s.Work().Reads.DecodedRecords, "oversized attempt and retry both counted")
	page, err = l.ReadCommittedMetadata(ctx, 1, 2000, nil)
	require.NoError(t, err)
	require.Len(t, page, 1024)
	page, err = l.ReadCommittedMetadata(ctx, 1025, 3, nil)
	require.NoError(t, err)
	require.Len(t, page, 3)
	require.Equal(t, 0, len(s.budget.scratch))
}
func TestDetachedMetadataConversionEveryDiskField(t *testing.T) {
	raw := types.DiskMessage{Topic: "orders", Partition: 1, Offset: 4, ProducerID: "p", SeqNum: 7, Epoch: 8, LeaderEpoch: 9, Payload: "data", Key: "key", EventType: "event", SchemaVersion: 2, AggregateVersion: 11, Metadata: "meta", EventID: "id", PayloadDigest: "digest", TransactionalID: "txn", TransactionState: types.TransactionStateAborted, TransactionMarker: types.TransactionMarkerAbort, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 17, ControlBatchKey: []byte{1, 2}, ControlBatchValue: []byte{3, 4}}
	actual := reflect.ValueOf(detachedMetadataMessage(raw))
	expected := reflect.ValueOf(raw)
	for i := 0; i < expected.NumField(); i++ {
		name := expected.Type().Field(i).Name
		a := actual.FieldByName(name)
		if name == "Partition" {
			require.Equal(t, expected.Field(i).Int(), a.Int())
			continue
		}
		require.Equal(t, expected.Field(i).Interface(), a.Interface(), name)
	}
	require.True(t, actual.FieldByName("Timestamp").IsZero())
	require.True(t, actual.FieldByName("RetryCount").IsZero())
	require.True(t, actual.FieldByName("Retry").IsZero())
}

func TestDetachedMetadataSharedBudgetAndRetirement(t *testing.T) {
	root := metadataRoot(t)
	budget := NewDetachedMetadataBudget()
	var stores []*DetachedMetadataStore
	for i := 0; i < 12; i++ {
		stores = append(stores, metadataStore(t, root, budget, 1, []types.DiskMessage{metadataRecord(0, fmt.Sprintf("txn%d", i), 1, types.TransactionMarkerCommit)}))
	}
	require.Equal(t, int64(12), budget.used.keys)
	l := metadataLease(t, stores[0])
	before := budget.used
	require.ErrorIs(t, stores[0].Close(), disk.ErrDetachedBootstrapPending)
	require.Equal(t, before, budget.used)
	_, err := stores[0].Acquire()
	require.ErrorIs(t, err, disk.ErrDetachedBootstrapClosed)
	_, err = stores[0].AppendPrepared(context.Background(), l.Bounds(), metadataPrepared(t, root, 1, 2, nil))
	require.ErrorIs(t, err, disk.ErrDetachedBootstrapClosed)
	require.NoError(t, l.Close())
	require.Equal(t, int64(11), budget.used.keys)
	after := budget.used
	require.NoError(t, l.Close())
	require.NoError(t, stores[0].Close())
	require.Equal(t, after, budget.used)
	for _, s := range stores {
		require.NoError(t, s.Close())
	}
	require.Equal(t, detachedMetadataCharge{}, budget.used)
}
func TestDetachedMetadataQuotaFailurePreservesOwnership(t *testing.T) {
	root := metadataRoot(t)
	budget := newDetachedMetadataBudget(detachedMetadataLimits{1, 4, 100, 10000})
	raw, err := root.Adopt(metadataPrepared(t, root, 0, 2, []types.DiskMessage{metadataRecord(0, "one", 1, types.TransactionMarkerCommit), metadataRecord(1, "two", 1, types.TransactionMarkerCommit)}))
	require.NoError(t, err)
	_, err = NewDetachedMetadataStore(context.Background(), raw, budget)
	require.ErrorIs(t, err, ErrDetachedMetadataLimit)
	require.Equal(t, detachedMetadataCharge{}, budget.used)
	lease, err := raw.Acquire()
	require.NoError(t, err)
	require.NoError(t, lease.Close())
	require.NoError(t, raw.Close())
	s := metadataStore(t, root, budget, 1, []types.DiskMessage{metadataRecord(0, "one", 1, types.TransactionMarkerCommit)})
	bounds, err := s.raw.Bounds()
	require.NoError(t, err)
	suffix := metadataPrepared(t, root, 1, 2, []types.DiskMessage{metadataRecord(1, "two", 1, types.TransactionMarkerCommit)})
	_, err = s.AppendPrepared(context.Background(), bounds, suffix)
	require.ErrorIs(t, err, ErrDetachedMetadataLimit)
	actual, err := s.raw.Bounds()
	require.NoError(t, err)
	require.Equal(t, bounds, actual)
	suffixLease, err := suffix.Acquire()
	require.NoError(t, err)
	require.NoError(t, suffixLease.Close())
	require.NoError(t, suffix.Close())
	require.Equal(t, uint64(1), s.Work().Suffix.DecodedRecords)
}
func TestDetachedMetadataScratchCancellationAndRetry(t *testing.T) {
	root := metadataRoot(t)
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 1, []types.DiskMessage{metadataRecord(0, "", 0, "")})
	l := metadataLease(t, s)
	s.budget.scratch <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := l.ReadCommittedMetadata(ctx, 0, 1, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	<-s.budget.scratch
	page, err := l.ReadCommittedMetadata(context.Background(), 0, 1, nil)
	require.NoError(t, err)
	require.Len(t, page, 1)
}
func TestDetachedMetadataAppendCommitCancellationAndCleanupError(t *testing.T) {
	root := metadataRoot(t)
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 1, []types.DiskMessage{metadataRecord(0, "txn", 1, "")})
	old := metadataLease(t, s)
	suffix := metadataPrepared(t, root, 1, 2, []types.DiskMessage{metadataRecord(1, "txn", 1, types.TransactionMarkerCommit)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	injected := errors.New("cleanup debt")
	actualAppend := s.appendPrepared
	s.appendPrepared = func(ctx context.Context, b disk.DetachedRawBounds, r *disk.DetachedRawStore) (disk.DetachedAppendResult, error) {
		result, err := actualAppend(ctx, b, r)
		if err == nil {
			cancel()
			result.CleanupError = injected
		}
		return result, err
	}
	result, err := s.AppendPrepared(ctx, old.Bounds(), suffix)
	require.NoError(t, err)
	require.ErrorIs(t, result.CleanupError, injected)
	l := metadataLease(t, s)
	page, err := l.ReadCommittedMetadata(context.Background(), 0, 10, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, metadataOffsets(page))
	_, err = s.AppendPrepared(context.Background(), old.Bounds(), suffix)
	require.Error(t, err)
	require.Len(t, s.history.entries[transactionMarkerKey{"txn", 1}].entries, 1)
}
func TestDetachedMetadataAppendFailureAndCleanupRelease(t *testing.T) {
	root := metadataRoot(t)
	budget := NewDetachedMetadataBudget()
	s := metadataStore(t, root, budget, 1, []types.DiskMessage{metadataRecord(0, "txn", 1, "")})
	l := metadataLease(t, s)
	injected := errors.New("append failure")
	s.appendPrepared = func(context.Context, disk.DetachedRawBounds, *disk.DetachedRawStore) (disk.DetachedAppendResult, error) {
		return disk.DetachedAppendResult{}, injected
	}
	suffix := metadataPrepared(t, root, 1, 2, []types.DiskMessage{metadataRecord(1, "txn", 1, types.TransactionMarkerCommit)})
	_, err := s.AppendPrepared(context.Background(), l.Bounds(), suffix)
	require.ErrorIs(t, err, injected)
	page, err := l.ReadCommittedMetadata(context.Background(), 0, 10, nil)
	require.NoError(t, err)
	require.Empty(t, page)
	require.Equal(t, 0, len(s.history.entries[transactionMarkerKey{"txn", 1}].entries))
	require.Greater(t, budget.used.slots, int64(0))
	require.NoError(t, suffix.Close())
	closeRaw := s.closeRaw
	s.closeRaw = func() error { return errors.Join(closeRaw(), injected) }
	require.ErrorIs(t, s.Close(), injected)
	require.Greater(t, budget.used.bytes, int64(0))
	leaseClose := l.closeRaw
	l.closeRaw = func() error { return errors.Join(leaseClose(), injected) }
	require.ErrorIs(t, l.Close(), injected)
	require.Equal(t, detachedMetadataCharge{}, budget.used)
	require.ErrorIs(t, l.Close(), injected)
	require.Equal(t, detachedMetadataCharge{}, budget.used)
	l.closeRaw = leaseClose
	require.ErrorIs(t, s.Close(), injected)
	require.Equal(t, detachedMetadataCharge{}, budget.used)
	s.closeRaw = closeRaw
}

type metadataReentrantResolver struct{ store *DetachedMetadataStore }

func (r metadataReentrantResolver) TransactionDecision(string, int64) (string, bool) {
	r.store.historyMu.Lock()
	known := len(r.store.history.entries) > 0
	r.store.historyMu.Unlock()
	return types.TransactionStateCommitted, known
}
func TestDetachedMetadataResolverOutsideHistoryLockAndConcurrentLeases(t *testing.T) {
	root := metadataRoot(t)
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 2, []types.DiskMessage{metadataRecord(0, "txn", 1, ""), metadataRecord(1, "txn", 1, types.TransactionMarkerCommit)})
	leases := []*DetachedMetadataLease{metadataLease(t, s), metadataLease(t, s)}
	var wg sync.WaitGroup
	for _, l := range leases {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				page, err := l.ReadCommittedMetadata(context.Background(), 0, 1, metadataReentrantResolver{s})
				if err != nil || len(page) != 1 {
					t.Errorf("read: %v %v", page, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

type metadataCancelResolver struct {
	cancel context.CancelFunc
	state  string
}

func (r metadataCancelResolver) TransactionDecision(string, int64) (string, bool) {
	r.cancel()
	return r.state, true
}
func TestDetachedMetadataResolverCancellationAtPageBoundary(t *testing.T) {
	root := metadataRoot(t)
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 2, []types.DiskMessage{metadataRecord(0, "txn", 1, ""), metadataRecord(1, "txn", 1, types.TransactionMarkerCommit)})
	for _, state := range []string{types.TransactionStateCommitted, types.TransactionStateOpen} {
		t.Run(state, func(t *testing.T) {
			l := metadataLease(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			page, err := l.ReadCommittedMetadata(ctx, 0, 1, metadataCancelResolver{cancel, state})
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, page)
			page, err = l.ReadCommittedMetadata(context.Background(), 0, 1, nil)
			require.NoError(t, err)
			require.Len(t, page, 1)
		})
	}
}
func TestDetachedMetadataBudgetCountsFailedConstructor(t *testing.T) {
	root := metadataRoot(t)
	budget := newDetachedMetadataBudget(detachedMetadataLimits{0, 0, 0, 1000})
	raw, err := root.Adopt(metadataPrepared(t, root, 0, 1, []types.DiskMessage{metadataRecord(0, "txn", 1, types.TransactionMarkerCommit)}))
	require.NoError(t, err)
	_, err = NewDetachedMetadataStore(context.Background(), raw, budget)
	require.ErrorIs(t, err, ErrDetachedMetadataLimit)
	require.Equal(t, uint64(1), budget.Work().DecodedRecords)
	require.Greater(t, budget.Work().ReadBytes, uint64(0))
	require.Equal(t, detachedMetadataCharge{}, budget.used)
	require.NoError(t, raw.Close())
}
func TestDetachedMetadataCapacityPeakAndIDLimits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limits detachedMetadataLimits
		id     string
	}{
		{"replacement capacity", detachedMetadataLimits{10, 2, 100, 10000}, "txn"},
		{"ID bytes", detachedMetadataLimits{10, 100, 2, 10000}, "txn"},
		{"history bytes", detachedMetadataLimits{10, 100, 100, 1024}, "txn"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := newDetachedMetadataBudget(tc.limits)
			h, err := newDetachedMarkerHistory(budget)
			require.NoError(t, err)
			defer h.release()
			list, err := h.ensure(transactionMarkerKey{tc.id, 1}, 1)
			if tc.name != "replacement capacity" {
				require.ErrorIs(t, err, ErrDetachedMetadataLimit)
				return
			}
			require.NoError(t, err)
			list.entries = append(list.entries, transactionMarkerInfo{offset: 1})
			_, err = h.ensure(transactionMarkerKey{tc.id, 1}, 1)
			require.ErrorIs(t, err, ErrDetachedMetadataLimit, "old and replacement backing arrays coexist")
			require.Equal(t, int64(1), budget.used.slots)
			require.Len(t, list.entries, 1)
			require.Equal(t, 1, cap(list.entries))
		})
	}
}

func TestDetachedMetadataExactByteBoundaryAndAbortedState(t *testing.T) {
	root := metadataRoot(t)
	first := metadataRecord(0, "", 0, "")
	encoded, err := util.SerializeDiskMessage(first)
	require.NoError(t, err)
	first.Payload = strings.Repeat("x", detachedMetadataPageBytes-len(encoded))
	aborted := metadataRecord(1, "txn", 1, "")
	aborted.TransactionState = types.TransactionStateAborted
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 4, []types.DiskMessage{first, aborted, metadataRecord(2, "txn", 1, types.TransactionMarkerCommit), metadataRecord(3, "", 0, "")})
	l := metadataLease(t, s)
	page, err := l.ReadCommittedMetadata(context.Background(), 0, 100, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, metadataOffsets(page))
	require.Equal(t, uint64(1), s.Work().Reads.DecodedRecords)
	page, err = l.ReadCommittedMetadata(context.Background(), 1, 100, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, metadataOffsets(page))
}
func TestDetachedMetadataCanceledAppendKeepsSuffix(t *testing.T) {
	root := metadataRoot(t)
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 1, nil)
	l := metadataLease(t, s)
	suffix := metadataPrepared(t, root, 1, 2, []types.DiskMessage{metadataRecord(1, "txn", 1, types.TransactionMarkerCommit)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.AppendPrepared(ctx, l.Bounds(), suffix)
	require.ErrorIs(t, err, context.Canceled)
	raw, err := suffix.Acquire()
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	require.NoError(t, suffix.Close())
	bounds, err := s.raw.Bounds()
	require.NoError(t, err)
	require.Equal(t, l.Bounds(), bounds)
}

func TestDetachedMetadataConcurrentAppendAndFixedLeases(t *testing.T) {
	root := metadataRoot(t)
	s := metadataStore(t, root, NewDetachedMetadataBudget(), 2, []types.DiskMessage{metadataRecord(0, "txn", 1, ""), metadataRecord(1, "txn", 1, types.TransactionMarkerCommit)})
	old := []*DetachedMetadataLease{metadataLease(t, s), metadataLease(t, s)}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, lease := range old {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 100; i++ {
				page, err := lease.ReadCommittedMetadata(context.Background(), 0, 10, nil)
				if err != nil || len(page) != 1 || page[0].Offset != 0 {
					t.Errorf("fixed lease changed during append: %v, %v", page, err)
					return
				}
			}
		}()
	}
	close(start)
	for offset := uint64(2); offset < 12; offset++ {
		marker := types.TransactionMarkerAbort
		if offset%2 == 1 {
			marker = types.TransactionMarkerCommit
		}
		bounds, err := s.raw.Bounds()
		require.NoError(t, err)
		_, err = s.AppendPrepared(context.Background(), bounds, metadataPrepared(t, root, offset, offset+1, []types.DiskMessage{metadataRecord(offset, "txn", 1, marker)}))
		require.NoError(t, err)
		fresh, err := s.Acquire()
		require.NoError(t, err)
		page, err := fresh.ReadCommittedMetadata(context.Background(), 0, 10, nil)
		require.NoError(t, err)
		if marker == types.TransactionMarkerCommit {
			require.Equal(t, []uint64{0}, metadataOffsets(page))
		} else {
			require.Empty(t, page)
		}
		require.NoError(t, fresh.Close())
	}
	wg.Wait()
	require.Equal(t, uint64(2), s.Work().Initial.DecodedRecords)
	require.Equal(t, uint64(10), s.Work().Suffix.DecodedRecords)
}
