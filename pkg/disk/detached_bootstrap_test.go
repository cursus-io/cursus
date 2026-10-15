package disk

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func detachedTestMetadata(t *testing.T, records []types.DiskMessage, end uint64, compacted bool) (BootstrapMetadata, []byte) {
	t.Helper()
	var raw []byte
	for _, record := range records {
		payload, err := util.SerializeDiskMessage(record)
		require.NoError(t, err)
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
		raw = append(raw, header[:]...)
		raw = append(raw, payload...)
	}
	id, err := newBootstrapID()
	require.NoError(t, err)
	m := BootstrapMetadata{Version: BootstrapVersion, TransferID: id, EndOffset: end, Compacted: compacted, Fence: bootstrapTestFence(1, end)}
	setDetachedTestDescriptors(&m, raw)
	return m, raw
}

func setDetachedTestDescriptors(m *BootstrapMetadata, raw []byte) {
	sum, empty := sha256.Sum256(raw), sha256.Sum256(nil)
	m.Files = []BootstrapFile{{ID: "records", Role: "canonical-log", Size: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])}, {ID: "catalog", Role: "snapshot-catalog", SHA256: hex.EncodeToString(empty[:])}}
}

func detachedTestRoot(t *testing.T) *DetachedBootstrapRoot {
	t.Helper()
	root, err := NewDetachedBootstrapRoot(t.TempDir(), DefaultBootstrapLimits(), 0, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	return root
}

func detachedTestStage(t *testing.T, root *DetachedBootstrapRoot, m BootstrapMetadata, raw []byte) *DetachedBootstrapStage {
	t.Helper()
	stage, err := root.Stage("orders", 1, m)
	require.NoError(t, err)
	for position := 0; position < len(raw); {
		end := min(position+BootstrapChunkBytes, len(raw))
		require.NoError(t, stage.WriteChunk("records", int64(position), raw[position:end]))
		position = end
	}
	return stage
}

func TestDetachedBootstrapRootLifecycle(t *testing.T) {
	dir := t.TempDir()
	root, err := NewDetachedBootstrapRoot(dir, DefaultBootstrapLimits(), 0, 0)
	require.NoError(t, err)
	_, err = NewDetachedBootstrapRoot(dir, DefaultBootstrapLimits(), 0, 0)
	require.Error(t, err)
	require.NoError(t, root.Close())
	require.NoError(t, root.Close())
	next, err := NewDetachedBootstrapRoot(dir, DefaultBootstrapLimits(), 0, 0)
	require.NoError(t, err)
	require.NoError(t, next.Close())
}

func TestDetachedRawPreservesFieldsPinsAndPhysicalOwnership(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	root := detachedTestRoot(t)
	records := []types.DiskMessage{
		{Topic: "orders", Partition: 1, Offset: 1, Payload: "aborted", ProducerID: "producer", SeqNum: 17, Epoch: 3, LeaderEpoch: 4, Key: "key", EventType: "event", SchemaVersion: 2, AggregateVersion: 3, Metadata: "metadata", EventID: "id", PayloadDigest: "digest", TransactionalID: "txn", TransactionState: types.TransactionStateAborted},
		{Topic: "orders", Partition: 1, Offset: 7, TransactionalID: "txn", TransactionMarker: types.TransactionMarkerAbort, ControlBatchType: types.ControlBatchTransaction, ControlBatchVersion: types.ControlBatchVersionCursusV2, ControlBatchCoordinatorEpoch: 5, ControlBatchKey: []byte{1, 2}, ControlBatchValue: []byte{3, 4}},
	}
	m, raw := detachedTestMetadata(t, records, 10, true)
	stage := detachedTestStage(t, root, m, raw)
	store, err := stage.Prepare(context.Background())
	require.NoError(t, err)
	require.False(t, stage.transfer.installed)
	require.NoError(t, stage.Abort())
	require.Error(t, stage.WriteChunk("records", 0, raw))
	_, err = stage.Prepare(context.Background())
	require.Error(t, err)
	require.DirExists(t, stage.transfer.dir)
	lease, err := store.Acquire()
	require.NoError(t, err)
	t.Cleanup(func() { _ = lease.Close(); _ = store.Close() })
	require.Equal(t, uint64(10), lease.Bounds().EndOffset)
	require.Equal(t, int64(len(raw)), lease.Bounds().RawBytes)
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+1, active)
	require.Greater(t, reserved, beforeBytes+int64(len(raw)))
	require.ErrorIs(t, store.Close(), ErrDetachedBootstrapPending)
	require.ErrorIs(t, root.Close(), ErrDetachedBootstrapPending)
	_, err = store.Acquire()
	require.ErrorIs(t, err, ErrDetachedBootstrapClosed)
	_, err = NewDetachedBootstrapRoot(root.dir, root.limits, 0, 0)
	require.Error(t, err, "pinned root must retain kernel lock")
	page, err := lease.ReadRaw(context.Background(), 0, 10, 1<<20)
	require.NoError(t, err)
	require.Equal(t, records, page.Messages)
	require.True(t, page.Exhausted)
	require.Equal(t, uint64(10), page.NextOffset)
	require.NoError(t, lease.Close())
	require.NoDirExists(t, stage.transfer.dir)
	require.NoError(t, lease.Close())
	require.NoError(t, store.Close())
	require.NoError(t, root.Close())
	active, reserved = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes, reserved)
	entries, err := os.ReadDir(root.dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, ".cursus.lock", entries[0].Name(), "no live files, markers, pending manifests, or HWM")
}

func TestDetachedRawPagingUsesBoundedDiskIndexAndForwardCursor(t *testing.T) {
	root := detachedTestRoot(t)
	records := make([]types.DiskMessage, 400)
	for i := range records {
		records[i] = types.DiskMessage{Topic: "orders", Partition: 1, Offset: uint64(i*3 + 1), Payload: strings.Repeat("x", 200)}
	}
	m, raw := detachedTestMetadata(t, records, 1300, true)
	store, err := detachedTestStage(t, root, m, raw).Prepare(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	lease, err := store.Acquire()
	require.NoError(t, err)
	defer func() { require.NoError(t, lease.Close()) }()
	require.Positive(t, lease.Bounds().IndexBytes)
	require.Zero(t, lease.Work().ReadBytes, "Acquire must not rescan records")
	payload, err := util.SerializeDiskMessage(records[0])
	require.NoError(t, err)
	_, err = lease.ReadRaw(context.Background(), 0, 1, len(payload)-1)
	require.ErrorIs(t, err, ErrRetainedPageLimit)
	before := lease.Work()
	var all []types.DiskMessage
	next := uint64(0)
	for {
		page, err := lease.ReadRaw(context.Background(), next, 7, 2*len(payload))
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Messages), 7)
		require.LessOrEqual(t, page.DecodedBytes, 2*len(payload))
		require.Greater(t, page.NextOffset, next)
		all = append(all, page.Messages...)
		next = page.NextOffset
		if page.Exhausted {
			break
		}
	}
	require.Equal(t, records, all)
	after := lease.Work()
	require.Equal(t, uint64(len(raw)), after.ReadBytes-before.ReadBytes, "forward pagination reads each raw byte once")
	require.Equal(t, uint64(len(records)), after.DecodedRecords-before.DecodedRecords)
	before = after
	page, err := lease.ReadRaw(context.Background(), 901, 1, 1<<20)
	require.NoError(t, err)
	require.Equal(t, records[300], page.Messages[0])
	after = lease.Work()
	require.Less(t, after.ReadBytes-before.ReadBytes, uint64(4096+2*(len(payload)+4)), "backward seek must not rescan prefix")
	require.LessOrEqual(t, after.IndexReadBytes-before.IndexReadBytes, uint64(16*8), "binary search index work")
	page, err = lease.ReadRaw(context.Background(), 1200, 4, 1<<20)
	require.NoError(t, err)
	require.Empty(t, page.Messages)
	require.True(t, page.Exhausted)
	require.Equal(t, uint64(1300), page.NextOffset)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	page, err = lease.ReadRaw(ctx, 0, 10, 1<<20)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, page.Messages)
}

func TestDetachedCanonicalValidationMatchesInstall(t *testing.T) {
	record := types.DiskMessage{Topic: "orders", Partition: 1, Offset: 0, Payload: "payload"}
	cases := []struct {
		name      string
		records   []types.DiskMessage
		end       uint64
		compacted bool
		mutate    func(*BootstrapMetadata, *[]byte)
		valid     bool
	}{
		{name: "valid", records: []types.DiskMessage{record}, end: 1, valid: true},
		{name: "empty", end: 0, valid: true},
		{name: "compacted_empty", end: 5, compacted: true, valid: true},
		{name: "unmarked_empty", end: 1},
		{name: "unmarked_gap", records: []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 2}}, end: 3},
		{name: "trailing_gap", records: []types.DiskMessage{record}, end: 2},
		{name: "duplicate", records: []types.DiskMessage{record, record}, end: 1, compacted: true},
		{name: "order", records: []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 2}, record}, end: 3, compacted: true},
		{name: "identity", records: []types.DiskMessage{{Topic: "other", Partition: 1}}, end: 1},
		{name: "partition", records: []types.DiskMessage{{Topic: "orders", Partition: 2}}, end: 1},
		{name: "out_of_end", records: []types.DiskMessage{record}, end: 0, compacted: true},
		{name: "truncated", records: []types.DiskMessage{record}, end: 1, mutate: func(m *BootstrapMetadata, b *[]byte) { *b = (*b)[:len(*b)-1]; setDetachedTestDescriptors(m, *b) }},
		{name: "checksum", records: []types.DiskMessage{record}, end: 1, mutate: func(m *BootstrapMetadata, _ *[]byte) { m.Files[0].SHA256 = strings.Repeat("0", 64) }},
		{name: "partial_header", end: 1, mutate: func(m *BootstrapMetadata, b *[]byte) { *b = []byte{0, 0}; setDetachedTestDescriptors(m, *b) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := detachedTestRoot(t)
			m, raw := detachedTestMetadata(t, tc.records, tc.end, tc.compacted)
			if tc.mutate != nil {
				tc.mutate(&m, &raw)
			}
			stage := detachedTestStage(t, root, m, raw)
			store, prepareErr := stage.Prepare(context.Background())
			handler, _ := bootstrapTestHandler(t, 1)
			receiver, err := handler.StageBootstrap(m)
			require.NoError(t, err)
			if len(raw) > 0 {
				require.NoError(t, receiver.WriteChunk("records", 0, raw))
			}
			installErr := receiver.Install()
			if tc.valid {
				require.NoError(t, prepareErr)
				require.NoError(t, installErr)
				require.NoError(t, store.Close())
			} else {
				require.Error(t, prepareErr)
				require.Error(t, installErr)
				require.NoDirExists(t, stage.transfer.dir)
				require.NoError(t, receiver.Abort())
			}
		})
	}
}

func TestDetachedStageAdmissionAndChunkValidation(t *testing.T) {
	root := detachedTestRoot(t)
	m, raw := detachedTestMetadata(t, []types.DiskMessage{{Topic: "orders", Partition: 1, Payload: "value"}}, 1, false)
	_, err := root.Stage("wrong", 1, m)
	require.Error(t, err)
	bad := cloneBootstrapMetadata(m)
	bad.Files[1].SHA256 = strings.Repeat("0", 64)
	_, err = root.Stage("orders", 1, bad)
	require.Error(t, err)
	stage, err := root.Stage("orders", 1, m)
	require.NoError(t, err)
	require.Error(t, stage.WriteChunk("records", 1, raw[:1]))
	require.Error(t, stage.WriteChunk("records", -1, nil))
	require.Error(t, stage.WriteChunk("records", 0, append(append([]byte{}, raw...), 0)))
	require.NoError(t, stage.WriteChunk("records", 0, raw[:10]))
	require.NoError(t, stage.WriteChunk("records", 0, raw[:10]))
	require.Error(t, stage.WriteChunk("records", 5, raw[5:15]))
	require.Error(t, stage.WriteChunk("records", 0, []byte("conflict")))
	_, err = stage.Prepare(context.Background())
	require.Error(t, err)
	require.NoDirExists(t, stage.transfer.dir)
	stage = detachedTestStage(t, root, m, raw)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = stage.Prepare(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoDirExists(t, stage.transfer.dir)
}

func TestDetachedPreparedOwnershipSurvivesRunningTTL(t *testing.T) {
	root := detachedTestRoot(t)
	m, raw := detachedTestMetadata(t, nil, 0, false)
	stage := detachedTestStage(t, root, m, raw)
	entered, release := make(chan struct{}), make(chan struct{})
	root.beforePreparePublish = func() { close(entered); <-release }
	prepared := make(chan *DetachedRawStore, 1)
	failures := make(chan error, 1)
	go func() { store, err := stage.Prepare(context.Background()); prepared <- store; failures <- err }()
	<-entered
	ttlStarted, ttlDone := make(chan struct{}), make(chan struct{})
	timer := time.AfterFunc(0, func() { close(ttlStarted); _ = stage.transfer.close(); close(ttlDone) })
	<-ttlStarted
	require.False(t, timer.Stop(), "TTL callback is already running while Prepare holds ownership lock")
	close(release)
	store := <-prepared
	require.NoError(t, <-failures)
	<-ttlDone
	require.DirExists(t, stage.transfer.dir)
	require.NoError(t, stage.Abort())
	lease, err := store.Acquire()
	require.NoError(t, err)
	require.NoError(t, lease.Close())
	require.NoError(t, store.Close())
}

func TestDetachedCreationAndPreparationRaceRootClose(t *testing.T) {
	for _, phase := range []string{"creation", "preparation"} {
		t.Run(phase, func(t *testing.T) {
			root := detachedTestRoot(t)
			m, raw := detachedTestMetadata(t, nil, 0, false)
			entered, release := make(chan struct{}), make(chan struct{})
			barrier := func() { close(entered); <-release }
			result := make(chan error, 1)
			if phase == "creation" {
				root.beforeCreate = barrier
				go func() { _, err := root.Stage("orders", 1, m); result <- err }()
			} else {
				stage := detachedTestStage(t, root, m, raw)
				root.beforePreparePublish = barrier
				go func() { _, err := stage.Prepare(context.Background()); result <- err }()
			}
			<-entered
			require.ErrorIs(t, root.Close(), ErrDetachedBootstrapPending)
			_, err := NewDetachedBootstrapRoot(root.dir, root.limits, 0, 0)
			require.Error(t, err)
			close(release)
			require.ErrorIs(t, <-result, ErrDetachedBootstrapClosed)
			require.NoError(t, root.Close())
		})
	}
}

func TestDetachedFinalLeaseFailureRetainsRootUntilAdmissionCleanup(t *testing.T) {
	beforeActive, beforeBytes := bootstrapBudgetSnapshot()
	root := detachedTestRoot(t)
	m, raw := detachedTestMetadata(t, []types.DiskMessage{{Topic: "orders", Partition: 1, Payload: "retained"}}, 1, false)
	stage := detachedTestStage(t, root, m, raw)
	store, err := stage.Prepare(context.Background())
	require.NoError(t, err)
	lease, err := store.Acquire()
	require.NoError(t, err)
	injected := errors.New("delete denied")
	stage.transfer.removeAll = func(string) error { return injected }
	t.Cleanup(func() {
		stage.transfer.removeAll = os.RemoveAll
		_ = lease.Close()
		_ = store.Close()
		_ = root.Close()
	})
	require.ErrorIs(t, root.Close(), ErrDetachedBootstrapPending)
	require.False(t, bootstrapCleanupContains(stage.transfer), "pinned children must never enter global cleanup")
	require.ErrorIs(t, lease.Close(), injected)
	require.True(t, bootstrapCleanupContains(stage.transfer))
	active, reserved := bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive+1, active)
	require.Equal(t, beforeBytes+stage.transfer.byteOwner.reserved, reserved, "failed cleanup retains the full physical reservation")
	require.ErrorIs(t, root.Close(), injected)
	stage.transfer.removeAll = os.RemoveAll
	other := detachedTestRoot(t)
	admitted, err := other.Stage("orders", 1, m)
	require.NoError(t, err)
	require.NoDirExists(t, stage.transfer.dir)
	require.False(t, bootstrapCleanupContains(stage.transfer))
	require.NoError(t, root.Close())
	require.NoError(t, admitted.Abort())
	active, reserved = bootstrapBudgetSnapshot()
	require.Equal(t, beforeActive, active)
	require.Equal(t, beforeBytes, reserved)
}

func TestDetachedStartupPreservesUnknownAndRejectsRecognizedNonDirectory(t *testing.T) {
	dir := t.TempDir()
	id, err := newBootstrapID()
	require.NoError(t, err)
	recognized := filepath.Join(dir, detachedBootstrapPrefix+id)
	require.NoError(t, os.Mkdir(recognized, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(recognized, "records"), []byte("orphan"), 0600))
	for _, name := range []string{"unknown", ".detached.bootstrap-invalid", "partition_1.bootstrap.pending"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0600))
	}
	root, err := NewDetachedBootstrapRoot(dir, DefaultBootstrapLimits(), 0, 0)
	require.NoError(t, err)
	require.NoDirExists(t, recognized)
	for _, name := range []string{"unknown", ".detached.bootstrap-invalid", "partition_1.bootstrap.pending"} {
		require.FileExists(t, filepath.Join(dir, name))
	}
	require.NoError(t, root.Close())
	require.NoError(t, os.WriteFile(recognized, []byte("not directory"), 0600))
	_, err = NewDetachedBootstrapRoot(dir, DefaultBootstrapLimits(), 0, 0)
	require.Error(t, err)
	require.FileExists(t, recognized)
}

func TestDetachedLeasePartialOpenFailureDoesNotPin(t *testing.T) {
	root := detachedTestRoot(t)
	m, raw := detachedTestMetadata(t, nil, 0, false)
	stage := detachedTestStage(t, root, m, raw)
	store, err := stage.Prepare(context.Background())
	require.NoError(t, err)
	var opened *os.File
	root.openFile = func(path string) (*os.File, error) {
		if filepath.Base(path) == "range-index" {
			return nil, fmt.Errorf("index open denied")
		}
		var err error
		opened, err = os.Open(path)
		return opened, err
	}
	_, err = store.Acquire()
	require.Error(t, err)
	_, err = opened.Stat()
	require.Error(t, err, "partial-open records handle must close")
	require.Zero(t, stage.transfer.detached.leases)
	require.NoError(t, store.Close())
	require.NoDirExists(t, stage.transfer.dir)
}

func TestDetachedStartupDeletionFailureBlocksOpenAndReleasesLock(t *testing.T) {
	dir := t.TempDir()
	id, err := newBootstrapID()
	require.NoError(t, err)
	orphan := filepath.Join(dir, detachedBootstrapPrefix+id)
	require.NoError(t, os.Mkdir(orphan, 0700))
	injected := errors.New("orphan deletion denied")
	_, err = newDetachedBootstrapRoot(dir, DefaultBootstrapLimits(), 0, 0, func(path string) error { require.Equal(t, orphan, path); return injected })
	require.ErrorIs(t, err, injected)
	require.DirExists(t, orphan)
	root, err := NewDetachedBootstrapRoot(dir, DefaultBootstrapLimits(), 0, 0)
	require.NoError(t, err)
	require.NoDirExists(t, orphan)
	require.NoError(t, root.Close())
}

func TestDetachedRawCancellationWhileWaitingAndMalformedIO(t *testing.T) {
	root := detachedTestRoot(t)
	m, raw := detachedTestMetadata(t, []types.DiskMessage{{Topic: "orders", Partition: 1, Payload: "one"}, {Topic: "orders", Partition: 1, Offset: 1, Payload: "two"}}, 2, false)
	stage := detachedTestStage(t, root, m, raw)
	store, err := stage.Prepare(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	lease, err := store.Acquire()
	require.NoError(t, err)
	defer func() { require.NoError(t, lease.Close()) }()
	lease.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { _, err := lease.ReadRaw(ctx, 0, 10, 1<<20); finished <- err }()
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		lease.mu.Unlock()
		t.Fatal("canceled reader waited for lease lock")
	}
	lease.mu.Unlock()
	before := lease.Work()
	nextCtx := detachedTestContext{Context: context.Background(), check: func() error {
		if lease.work.DecodedRecords > before.DecodedRecords {
			return context.Canceled
		}
		return nil
	}}
	page, err := lease.ReadRaw(nextCtx, 0, 10, 1<<20)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, page.Messages, "cancel after first decode must not publish partial success")
	// Out-of-band mutation is unsupported, but short reads still fail closed.
	require.NoError(t, os.Truncate(filepath.Join(stage.transfer.dir, "records"), int64(len(raw)-1)))
	page, err = lease.ReadRaw(context.Background(), 0, 10, 1<<20)
	require.Error(t, err)
	require.Empty(t, page.Messages)
}

type detachedTestContext struct {
	context.Context
	check func() error
}

func (ctx detachedTestContext) Err() error { return ctx.check() }

func TestDetachedCanonicalScanCancellationBetweenFrames(t *testing.T) {
	root := detachedTestRoot(t)
	m, raw := detachedTestMetadata(t, []types.DiskMessage{{Topic: "orders", Partition: 1}, {Topic: "orders", Partition: 1, Offset: 1}}, 2, false)
	stage := detachedTestStage(t, root, m, raw)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	visited := 0
	err := scanCompactionSegmentContext(ctx, filepath.Join(stage.transfer.dir, "records"), 0, false, false, func(compactionFrame) error { visited++; cancel(); return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, visited)
	require.NoError(t, stage.Abort())
}

func TestDetachedPrepareRejectsNonregularIncomingAndEmptyIndexIsZero(t *testing.T) {
	root := detachedTestRoot(t)
	m, raw := detachedTestMetadata(t, nil, 0, false)
	stage := detachedTestStage(t, root, m, raw)
	path := filepath.Join(stage.transfer.dir, "records")
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0700))
	_, err := stage.Prepare(context.Background())
	require.Error(t, err)
	require.NoDirExists(t, stage.transfer.dir)
	stage = detachedTestStage(t, root, m, raw)
	store, err := stage.Prepare(context.Background())
	require.NoError(t, err)
	lease, err := store.Acquire()
	require.NoError(t, err)
	require.Zero(t, lease.Bounds().IndexBytes)
	page, err := lease.ReadRaw(context.Background(), 0, 1, 1)
	require.NoError(t, err)
	require.True(t, page.Exhausted)
	require.NoError(t, lease.Close())
	require.NoError(t, store.Close())
}

func TestDetachedRawCancellationAfterDecodeAtReturnBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		records int
	}{{"fixed_end", 1}, {"record_limit", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			root := detachedTestRoot(t)
			records := make([]types.DiskMessage, tc.records)
			for i := range records {
				records[i] = types.DiskMessage{Topic: "orders", Partition: 1, Offset: uint64(i), Payload: "record"}
			}
			m, raw := detachedTestMetadata(t, records, uint64(tc.records), false)
			store, err := detachedTestStage(t, root, m, raw).Prepare(context.Background())
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close()) }()
			lease, err := store.Acquire()
			require.NoError(t, err)
			defer func() { require.NoError(t, lease.Close()) }()
			ctx := detachedTestContext{Context: context.Background(), check: func() error {
				if lease.work.DecodedRecords > 0 {
					return context.Canceled
				}
				return nil
			}}
			page, err := lease.ReadRaw(ctx, 0, 1, 1<<20)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, page.Messages)
			require.False(t, page.Exhausted)
			retry, err := lease.ReadRaw(context.Background(), 0, 1, 1<<20)
			require.NoError(t, err)
			require.Equal(t, records[:1], retry.Messages, "canceled decode must not consume retry's first record")
		})
	}
}
