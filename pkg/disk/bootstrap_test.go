package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func bootstrapTestHandler(t *testing.T, partition int) (*DiskHandler, *config.Config) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 0
	cfg.DiskMinFreePercent = 0
	cfg.SegmentSize = 1 << 20
	cfg.IndexSize = 4096
	cfg.IndexIntervalBytes = 64
	cfg.SegmentRollTimeMS = 0
	cfg.DiskFlushIntervalMS = 1000
	cfg.RetentionCheckIntervalMS = 60000
	cfg.CompactionCheckIntervalMS = 60000
	d, err := NewDiskHandler(cfg, "orders", partition)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d, cfg
}
func bootstrapTestFence(partition int, end uint64) BootstrapFence {
	return BootstrapFence{Topic: "orders", Partition: partition, ReceiverID: "follower", SourceID: "leader", LeaderID: "leader", LeaderEpoch: 3, LifecycleEpoch: 7, CommittedHWM: end, RecoveryProtocol: 3}
}
func bootstrapTestExport(t *testing.T, d *DiskHandler, start, end uint64) *BootstrapExport {
	t.Helper()
	e, err := d.ExportBootstrap(start, end, bootstrapTestFence(1, end), []byte(`{"snapshots":[]}`))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	return e
}
func bootstrapTestReceive(t *testing.T, d *DiskHandler, e *BootstrapExport) *BootstrapReceiver {
	t.Helper()
	r, err := d.StageBootstrap(e.Manifest())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := r.Abort()
		if err != nil {
			require.ErrorIs(t, err, ErrBootstrapPending)
		}
	})
	for _, f := range e.Manifest().Files {
		for offset := int64(0); offset < f.Size; {
			chunk, err := e.ReadChunk(f.ID, offset, BootstrapChunkBytes)
			require.NoError(t, err)
			require.NotEmpty(t, chunk)
			require.NoError(t, r.WriteChunk(f.ID, offset, chunk))
			require.NoError(t, r.WriteChunk(f.ID, offset, chunk))
			offset += int64(len(chunk))
		}
	}
	return r
}
func bootstrapRawRecords(t *testing.T, path string) []types.DiskMessage {
	t.Helper()
	var records []types.DiskMessage
	require.NoError(t, scanCompactionSegment(path, 0, false, true, func(frame compactionFrame) error { records = append(records, frame.message); return nil }))
	return records
}

func TestBootstrapNilDuplicateChunks(t *testing.T) {
	for _, catalog := range [][]byte{nil, []byte(`{"snapshots":[]}`)} {
		t.Run(fmt.Sprintf("catalog_bytes_%d", len(catalog)), func(t *testing.T) {
			source, _ := bootstrapTestHandler(t, 1)
			export, err := source.ExportBootstrap(0, 0, bootstrapTestFence(1, 0), catalog)
			require.NoError(t, err)
			defer func() { require.NoError(t, export.Close()) }()
			target, _ := bootstrapTestHandler(t, 1)
			receiver := bootstrapTestReceive(t, target, export)
			for _, file := range export.Manifest().Files {
				require.NoError(t, receiver.WriteChunk(file.ID, 0, nil))
			}
			require.NoError(t, receiver.Install())
			for _, file := range export.Manifest().Files {
				require.NoError(t, receiver.WriteChunk(file.ID, 0, nil))
				if file.Size > 0 {
					require.ErrorContains(t, receiver.WriteChunk(file.ID, 0, []byte("!")), "conflicting duplicate")
				}
			}
		})
	}
}

func TestBootstrapPreservesCompactedLogicalBoundsAndRawTransactions(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	records := []types.DiskMessage{
		{Topic: "orders", Partition: 1, Offset: 1, Payload: "aborted payload", TransactionalID: "txn", TransactionState: types.TransactionStateAborted},
		{Topic: "orders", Partition: 1, Offset: 5, Payload: "control", TransactionalID: "txn", TransactionMarker: types.TransactionMarkerAbort},
	}
	require.NoError(t, source.WriteCompactedReplicaRange(0, 10, records))
	e := bootstrapTestExport(t, source, 0, 10)
	require.True(t, e.Manifest().Compacted)
	target, cfg := bootstrapTestHandler(t, 1)
	neighbor, err := NewDiskHandler(cfg, "orders", 10)
	require.NoError(t, err)
	defer func() { require.NoError(t, neighbor.Close()) }()
	_, err = neighbor.AppendMessageSync("orders", 10, &types.Message{Payload: "neighbor"})
	require.NoError(t, err)
	_, err = target.AppendMessageSync("orders", 1, &types.Message{Payload: "old"})
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(target.BaseName, 0o700))
	for _, name := range []string{target.BaseName + ".lineage", target.BaseName + ".hwm", target.BaseName + ".producers", filepath.Join(target.BaseName, "partition_1_snapshots_v2.dat"), filepath.Join(target.BaseName, "partition_1_stream.idx")} {
		require.NoError(t, os.WriteFile(name, []byte("stale"), 0o600))
	}
	unrelated := target.BaseName + "_notes.txt"
	require.NoError(t, os.WriteFile(unrelated, []byte("preserve"), 0o600))
	r := bootstrapTestReceive(t, target, e)
	require.NoError(t, r.Install())
	require.NoError(t, r.Install())
	require.Equal(t, records, bootstrapRawRecords(t, target.GetSegmentPath(0)))
	require.Equal(t, uint64(10), target.GetLatestOffset())
	require.ErrorIs(t, target.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 10}}), ErrBootstrapPending)
	_, err = target.AppendMessage("orders", 1, &types.Message{Payload: "blocked"})
	require.ErrorIs(t, err, ErrBootstrapPending)
	_, err = target.EnforceCompaction()
	require.ErrorIs(t, err, ErrBootstrapPending)
	require.ErrorIs(t, target.TruncateTo(0), ErrBootstrapPending)
	for _, name := range []string{target.BaseName + ".lineage", target.BaseName + ".hwm", target.BaseName + ".producers", filepath.Join(target.BaseName, "partition_1_snapshots_v2.dat"), filepath.Join(target.BaseName, "partition_1_stream.idx")} {
		_, err = os.Stat(name)
		require.True(t, os.IsNotExist(err), name)
	}
	_, err = os.Stat(target.BaseName + ".lock")
	require.NoError(t, err)
	data, err := os.ReadFile(unrelated)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(data))
	msgs, err := neighbor.ReadMessages(0, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "neighbor", msgs[0].Payload)
	meta, err := target.PendingBootstrap()
	require.NoError(t, err)
	require.Equal(t, e.Manifest(), *meta)
	catalog, err := target.PendingBootstrapCatalog()
	require.NoError(t, err)
	require.JSONEq(t, `{"snapshots":[]}`, string(catalog))
	require.NoError(t, target.Close())
	target, err = NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, target.Close()) }()
	require.Equal(t, uint64(10), target.GetLatestOffset())
	require.Equal(t, records, bootstrapRawRecords(t, target.GetSegmentPath(0)))
	require.ErrorIs(t, target.writeAvailabilityError(), ErrBootstrapPending)
	catalog, err = target.PendingBootstrapCatalog()
	require.NoError(t, err)
	require.NotEmpty(t, catalog)
	retry, err := target.StageBootstrap(e.Manifest())
	require.NoError(t, err)
	require.NoError(t, retry.Install())
	require.Error(t, target.CompleteBootstrap("00000000000000000000000000000000"))
	require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
	require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
	retry, err = target.StageBootstrap(e.Manifest())
	require.NoError(t, err)
	require.NoError(t, retry.Install())
	offset, err := target.AppendMessageSync("orders", 1, &types.Message{Payload: "next"})
	require.NoError(t, err)
	require.Equal(t, uint64(10), offset)
}

func TestBootstrapEmptyAndFullyCompactedRanges(t *testing.T) {
	for _, end := range []uint64{0, 8} {
		t.Run(fmt.Sprint(end), func(t *testing.T) {
			source, _ := bootstrapTestHandler(t, 1)
			if end != 0 {
				require.NoError(t, source.WriteCompactedReplicaRange(0, end, nil))
			}
			e := bootstrapTestExport(t, source, 0, end)
			target, cfg := bootstrapTestHandler(t, 1)
			r := bootstrapTestReceive(t, target, e)
			require.NoError(t, r.Install())
			require.Equal(t, end, target.GetLatestOffset())
			require.NoError(t, target.Close())
			target, err := NewDiskHandler(cfg, "orders", 1)
			require.NoError(t, err)
			defer func() { require.NoError(t, target.Close()) }()
			require.Equal(t, end, target.GetLatestOffset())
			require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
		})
	}
}

func TestBootstrapCrashReplacementReplaysAndMissingOrCorruptFileFailsClosed(t *testing.T) {
	for _, damage := range []string{"none", "missing", "corrupt", "manifest", "catalog", "version", "path"} {
		t.Run(damage, func(t *testing.T) {
			source, _ := bootstrapTestHandler(t, 1)
			require.NoError(t, source.WriteCompactedReplicaRange(0, 5, []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 2, Payload: "kept"}}))
			e := bootstrapTestExport(t, source, 0, 5)
			target, cfg := bootstrapTestHandler(t, 1)
			r := bootstrapTestReceive(t, target, e)
			bootstrapAfterReplace = func(id string) error {
				if id == "records" {
					return errors.New("simulated crash")
				}
				return nil
			}
			err := r.Install()
			bootstrapAfterReplace = nil
			require.ErrorContains(t, err, "simulated crash")
			require.Error(t, r.Install())
			require.NoError(t, target.Close())
			switch damage {
			case "missing":
				require.NoError(t, os.Remove(target.GetSegmentPath(0)))
			case "corrupt":
				require.NoError(t, os.WriteFile(target.GetSegmentPath(0), []byte("corrupt"), 0o600))
			case "manifest":
				require.NoError(t, os.WriteFile(bootstrapPendingPath(target.BaseName), []byte(`{"payload":{},"sha256":"wrong"}`), 0o600))
			case "catalog":
				require.NoError(t, os.Remove(filepath.Join(r.transfer.dir, "catalog")))
			case "version", "path":
				var tx bootstrapTransaction
				require.NoError(t, readBootstrapEnvelope(bootstrapPendingPath(target.BaseName), &tx))
				if damage == "version" {
					tx.Version = 100
				} else {
					tx.Delete = append(tx.Delete, "../unrelated")
				}
				require.NoError(t, writeBootstrapEnvelope(bootstrapPendingPath(target.BaseName), tx))
			}
			reopened, err := NewDiskHandler(cfg, "orders", 1)
			if damage != "none" {
				require.Error(t, err)
				require.Nil(t, reopened)
				return
			}
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			meta, err := reopened.PendingBootstrap()
			require.NoError(t, err)
			require.Equal(t, e.Manifest().TransferID, meta.TransferID)
			require.Equal(t, uint64(5), reopened.GetLatestOffset())
			require.NoError(t, reopened.CompleteBootstrap(meta.TransferID))
		})
	}
}

func TestBootstrapReadSessionExcludesReplacement(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, source.WriteCompactedReplicaRange(0, 5, nil))
	e := bootstrapTestExport(t, source, 0, 5)
	target, _ := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, target, e)
	session, err := target.OpenForRead(0)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- r.Install() }()
	select {
	case err := <-done:
		t.Fatalf("replacement bypassed reader: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	require.NoError(t, session.Close())
	require.NoError(t, session.Close())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("replacement did not drain reader")
	}
	require.Equal(t, int32(0), target.GetActiveReaders())
	require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
}

func TestBootstrapChunksRejectConflictsAndUnsafeMetadata(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	_, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: "hello"})
	require.NoError(t, err)
	e := bootstrapTestExport(t, source, 0, 1)
	target, _ := bootstrapTestHandler(t, 1)
	r, err := target.StageBootstrap(e.Manifest())
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Abort()) }()
	chunk, err := e.ReadChunk("records", 0, BootstrapChunkBytes)
	require.NoError(t, err)
	require.Error(t, r.WriteChunk("records", 1, chunk))
	require.Error(t, r.WriteChunk("../records", 0, chunk))
	require.Error(t, r.Install())
	require.NoError(t, r.WriteChunk("records", 0, chunk))
	require.NoError(t, r.WriteChunk("records", 0, chunk))
	conflict := bytes.Clone(chunk)
	conflict[0] ^= 1
	require.Error(t, r.WriteChunk("records", 0, conflict))
	m := e.Manifest()
	m.Files[0].ID = "../records"
	_, err = target.StageBootstrap(m)
	require.Error(t, err)
	m = e.Manifest()
	m.Fence.Partition = 10
	_, err = target.StageBootstrap(m)
	require.Error(t, err)
	m = e.Manifest()
	m.EndOffset = 2
	_, err = target.StageBootstrap(m)
	require.Error(t, err)
	_, err = source.ExportBootstrap(0, 1, bootstrapTestFence(1, 1), make([]byte, BootstrapCatalogBytes+1))
	var limit *BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	for _, name := range []string{"partition_10.hwm", "partition_1_notes.txt", "../partition_1.hwm", "partition_1/../partition_10.hwm"} {
		require.False(t, bootstrapOwnedName(target.BaseName, name), name)
	}
}

func TestBootstrapExportSurvivesSourceRetention(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, source.WriteCompactedReplicaRange(0, 5, []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 2, Payload: "retained export"}}))
	e := bootstrapTestExport(t, source, 0, 5)
	before, err := e.ReadChunk("records", 0, BootstrapChunkBytes)
	require.NoError(t, err)
	// Retention may remove the original segment as soon as the copy is complete.
	source.maintenanceMu.Lock()
	require.NoError(t, os.Remove(source.GetSegmentPath(0)))
	source.maintenanceMu.Unlock()
	after, err := e.ReadChunk("records", 0, BootstrapChunkBytes)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, e.Close())
	_, err = e.ReadChunk("records", 0, 1)
	require.Error(t, err)
}

func TestBootstrapContiguousRangeRetainsLineageAndDigest(t *testing.T) {
	for _, count := range []int{0, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			source, _ := bootstrapTestHandler(t, 1)
			for i := 0; i < count; i++ {
				_, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: fmt.Sprint(i), LeaderEpoch: 2})
				require.NoError(t, err)
			}
			end := uint64(count)
			want, err := source.LineageCandidates(end)
			require.NoError(t, err)
			wantDigest, err := source.RetainedRangeDigest(0, end)
			require.NoError(t, err)
			e := bootstrapTestExport(t, source, 0, end)
			require.False(t, e.Manifest().Compacted)
			target, _ := bootstrapTestHandler(t, 1)
			r := bootstrapTestReceive(t, target, e)
			require.NoError(t, r.Install())
			_, err = target.LineageCandidates(end)
			require.ErrorIs(t, err, ErrBootstrapPending)
			_, err = target.RetainedRangeDigest(0, end)
			require.ErrorIs(t, err, ErrBootstrapPending)
			require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
			got, err := target.LineageCandidates(end)
			require.NoError(t, err)
			require.Equal(t, want, got)
			digest, err := target.RetainedRangeDigest(0, end)
			require.NoError(t, err)
			require.Equal(t, wantDigest, digest)
		})
	}
}

func TestBootstrapRetainedDigestDetectsEarlierDivergenceAndLogicalBounds(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			source, _ := bootstrapTestHandler(t, 1)
			var records []types.DiskMessage
			if !empty {
				records = []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 1, Payload: "first"}, {Topic: "orders", Partition: 1, Offset: 4, Payload: "same last"}}
			}
			require.NoError(t, source.WriteCompactedReplicaRange(0, 9, records))
			want, err := source.RetainedRangeDigest(0, 9)
			require.NoError(t, err)
			short, err := source.RetainedRangeDigest(0, 8)
			require.NoError(t, err)
			require.NotEqual(t, want, short)
			_, err = source.RetainedRangeDigest(1, 9)
			require.Error(t, err)
			e := bootstrapTestExport(t, source, 0, 9)
			target, cfg := bootstrapTestHandler(t, 1)
			r := bootstrapTestReceive(t, target, e)
			require.NoError(t, r.Install())
			require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
			got, err := target.RetainedRangeDigest(0, 9)
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.NoError(t, target.Close())
			target, err = NewDiskHandler(cfg, "orders", 1)
			require.NoError(t, err)
			defer func() { require.NoError(t, target.Close()) }()
			got, err = target.RetainedRangeDigest(0, 9)
			require.NoError(t, err)
			require.Equal(t, want, got)
			if !empty {
				other, _ := bootstrapTestHandler(t, 1)
				records[0].Payload = "divergent first"
				require.NoError(t, other.WriteCompactedReplicaRange(0, 9, records))
				different, err := other.RetainedRangeDigest(0, 9)
				require.NoError(t, err)
				require.NotEqual(t, want, different)
			}
		})
	}
}

func TestBootstrapCanonicalizesLegacyPhysicalEncoding(t *testing.T) {
	legacy, cfg := bootstrapTestHandler(t, 1)
	require.NoError(t, legacy.Close())
	var oldLog bytes.Buffer
	for i, payload := range []string{"old", "same last"} {
		message := types.DiskMessage{Topic: "orders", Partition: 1, Offset: uint64(i), Payload: payload}
		encoded, err := util.SerializeDiskMessage(message)
		require.NoError(t, err)
		leaderEpochOffset := 4 + 2 + len("orders") + 4 + 8 + 2 + 8 + 8
		old := append([]byte("CDM2"), encoded[4:leaderEpochOffset]...)
		old = append(old, encoded[leaderEpochOffset+8:len(encoded)-8]...)
		require.NoError(t, binary.Write(&oldLog, binary.BigEndian, uint32(len(old))))
		_, err = oldLog.Write(old)
		require.NoError(t, err)
	}
	require.NoError(t, os.WriteFile(legacy.GetSegmentPath(0), oldLog.Bytes(), 0o600))
	legacy, err := NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, legacy.Close()) }()
	want, err := legacy.RetainedRangeDigest(0, 2)
	require.NoError(t, err)
	e := bootstrapTestExport(t, legacy, 0, 2)
	canonical, err := e.ReadChunk("records", 0, BootstrapChunkBytes)
	require.NoError(t, err)
	require.NotEqual(t, oldLog.Bytes(), canonical)
	require.Equal(t, "CDM5", string(canonical[4:8]))
	target, _ := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, target, e)
	require.NoError(t, r.Install())
	require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
	got, err := target.RetainedRangeDigest(0, 2)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestBootstrapTransferBudgetAndExpiration(t *testing.T) {
	base := filepath.Join(t.TempDir(), "partition_1")
	var transfers []*bootstrapTransfer
	defer func() {
		for _, transfer := range transfers {
			require.NoError(t, transfer.close())
		}
	}()
	for i := 0; i < BootstrapMaxActive; i++ {
		transfer, err := newBootstrapTransfer(base, BootstrapMetadata{}, 0, DefaultBootstrapLimits())
		require.NoError(t, err)
		transfers = append(transfers, transfer)
	}
	_, err := newBootstrapTransfer(base, BootstrapMetadata{}, 0, DefaultBootstrapLimits())
	var limit *BootstrapLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "active transfers", limit.Resource)
	first := transfers[0]
	first.startTTL()
	first.timer.Reset(time.Millisecond)
	require.Eventually(t, func() bool { first.mu.Lock(); defer first.mu.Unlock(); return first.closed }, time.Second, time.Millisecond)
	_, err = os.Stat(first.dir)
	require.True(t, os.IsNotExist(err))
	replacement, err := newBootstrapTransfer(base, BootstrapMetadata{}, 0, DefaultBootstrapLimits())
	require.NoError(t, err)
	require.NoError(t, replacement.close())
}

func TestBootstrapRejectsUnmarkedGapBeforeReplacement(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	require.NoError(t, source.WriteCompactedReplicaRange(0, 5, []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 1, Payload: "gap"}}))
	e := bootstrapTestExport(t, source, 0, 5)
	m := e.Manifest()
	m.Compacted = false
	target, _ := bootstrapTestHandler(t, 1)
	_, err := target.AppendMessageSync("orders", 1, &types.Message{Payload: "original"})
	require.NoError(t, err)
	r, err := target.StageBootstrap(m)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Abort()) }()
	for _, f := range m.Files {
		data, err := e.ReadChunk(f.ID, 0, BootstrapChunkBytes)
		require.NoError(t, err)
		require.NoError(t, r.WriteChunk(f.ID, 0, data))
	}
	require.Error(t, r.Install())
	pending, err := target.PendingBootstrap()
	require.NoError(t, err)
	require.Nil(t, pending)
	messages, err := target.ReadMessages(0, 1)
	require.NoError(t, err)
	require.Equal(t, "original", messages[0].Payload)
}

func TestBootstrapEveryReplacementCrashReplaysRepeatedly(t *testing.T) {
	for _, mode := range []string{"compacted", "contiguous", "empty"} {
		steps := []string{"records", "range-index"}
		if mode == "compacted" {
			steps = append(steps, "marker")
		}
		if mode != "empty" {
			steps = append(steps, "tail-log", "tail-index")
		}
		for _, step := range steps {
			t.Run(mode+"/"+step, func(t *testing.T) {
				source, _ := bootstrapTestHandler(t, 1)
				var end uint64
				if mode == "compacted" {
					end = 5
					require.NoError(t, source.WriteCompactedReplicaRange(0, end, []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 2, Payload: "new"}}))
				}
				if mode == "contiguous" {
					end = 1
					_, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: "new"})
					require.NoError(t, err)
				}
				want := bootstrapRawRecords(t, source.GetSegmentPath(0))
				e := bootstrapTestExport(t, source, 0, end)
				target, cfg := bootstrapTestHandler(t, 1)
				r := bootstrapTestReceive(t, target, e)
				bootstrapAfterReplace = func(id string) error {
					if id == step {
						return errors.New("crash at " + step)
					}
					return nil
				}
				err := r.Install()
				bootstrapAfterReplace = nil
				require.ErrorContains(t, err, "crash at "+step)
				require.ErrorIs(t, target.RollSegmentAt(target.GetLatestOffset()), ErrBootstrapPending)
				require.ErrorIs(t, target.OpenIndexFiles(), ErrBootstrapPending)
				require.ErrorIs(t, target.CloseIndexFiles(), ErrBootstrapPending)
				_, err = target.SendCurrentSegmentToConn(nil)
				require.ErrorIs(t, err, ErrBootstrapPending)
				_, err = target.AppendMessageSync("orders", 1, &types.Message{Payload: "blocked"})
				require.ErrorIs(t, err, ErrBootstrapPending)
				require.NoError(t, target.Close())
				for attempt := 0; attempt < 2; attempt++ {
					target, err = NewDiskHandler(cfg, "orders", 1)
					require.NoError(t, err)
					require.Equal(t, end, target.GetLatestOffset())
					require.Equal(t, want, bootstrapRawRecords(t, target.GetSegmentPath(0)))
					pending, err := target.PendingBootstrap()
					require.NoError(t, err)
					require.Equal(t, e.Manifest(), *pending)
					if attempt == 1 {
						require.NoError(t, target.CompleteBootstrap(pending.TransferID))
					}
					require.NoError(t, target.Close())
				}
			})
		}
	}
}

func TestBootstrapCompletionCleanupFailureDoesNotUndoDurableSuccess(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	e := bootstrapTestExport(t, source, 0, 0)
	target, cfg := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, target, e)
	require.NoError(t, r.Install())
	cleanup := bootstrapCleanupCompleted
	bootstrapCleanupCompleted = func(string) error { return errors.New("simulated cleanup failure") }
	err := target.CompleteBootstrap(e.Manifest().TransferID)
	bootstrapCleanupCompleted = cleanup
	require.NoError(t, err)
	require.NoError(t, target.writeAvailabilityError())
	pending, err := target.PendingBootstrap()
	require.NoError(t, err)
	require.Nil(t, pending)
	require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
	_, err = os.Stat(r.transfer.dir)
	require.NoError(t, err)
	require.NoError(t, target.Close())
	target, err = NewDiskHandler(cfg, "orders", 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, target.Close()) }()
	_, err = os.Stat(r.transfer.dir)
	require.True(t, os.IsNotExist(err))
}

func TestBootstrapInstalledDuplicateReadUsesReplacementGate(t *testing.T) {
	source, _ := bootstrapTestHandler(t, 1)
	_, err := source.AppendMessageSync("orders", 1, &types.Message{Payload: "original"})
	require.NoError(t, err)
	e := bootstrapTestExport(t, source, 0, 1)
	chunk, err := e.ReadChunk("records", 0, BootstrapChunkBytes)
	require.NoError(t, err)
	target, _ := bootstrapTestHandler(t, 1)
	r := bootstrapTestReceive(t, target, e)
	require.NoError(t, r.Install())
	require.NoError(t, target.CompleteBootstrap(e.Manifest().TransferID))
	target.replacementMu.Lock()
	done := make(chan error, 1)
	go func() { done <- r.WriteChunk("records", 0, chunk) }()
	select {
	case err := <-done:
		target.replacementMu.Unlock()
		t.Fatalf("duplicate bypassed replacement gate: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	target.replacementMu.Unlock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("duplicate read did not resume")
	}
}
