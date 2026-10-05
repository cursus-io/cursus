package transaction

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestDecodeJournalSnapshotRejectsUnversionedAndUnknownVersions(t *testing.T) {
	snapshot := testJournalSnapshot("tx-versioned", 1, StateCommitted)
	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeJournalSnapshot(payload); err == nil {
		t.Fatal("expected an unversioned journal snapshot to fail")
	}

	payload, err = json.Marshal(journalRecord{Version: journalFormatVersion + 1, Transaction: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeJournalSnapshot(payload); err == nil {
		t.Fatal("expected an unsupported journal version to fail")
	}
}
func TestJournalLoadsLatestSnapshotPerTransactionalID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}

	first := testJournalSnapshot("tx-a", 1, StateOpen)
	second := testJournalSnapshot("tx-a", 2, StateCommitting)
	other := testJournalSnapshot("tx-b", 1, StateCommitted)
	for _, snap := range []*Snapshot{first, second, other, second} {
		if err := journal.Append(snap); err != nil {
			t.Fatal(err)
		}
	}

	loaded, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected two transactions, got %d", len(loaded))
	}
	if got := loaded["tx-a"]; got == nil || got.Revision != 2 || got.State != StateCommitting {
		t.Fatalf("unexpected tx-a snapshot: %+v", got)
	}
	if got := loaded["tx-b"]; got == nil || got.State != StateCommitted {
		t.Fatalf("unexpected tx-b snapshot: %+v", got)
	}
}

func TestJournalDeltaBoundsGrowingTransactionWriteAmplification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}

	snap := testJournalSnapshot("growing", 1, StateOpen)
	for revision := uint64(1); revision <= 100; revision++ {
		snap.Revision = revision
		snap.UpdatedAt = snap.UpdatedAt.Add(time.Second)
		snap.Messages = append(snap.Messages, MessageOperation{
			Topic: "orders", Partition: 0,
			Message: types.Message{Payload: strings.Repeat("x", 4096), SeqNum: revision},
		})
		snap.SequenceByPartition = map[string]uint64{"orders:0": revision}
		snap.RequestAssignments = map[string]RequestAssignment{
			"latest": {Topic: "orders", Partition: 0, Sequence: revision},
		}
		if err := journal.Append(snap); err != nil {
			t.Fatalf("append revision %d: %v", revision, err)
		}
	}

	full, err := json.Marshal(journalRecord{Version: journalFormatVersion, Transaction: snap})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > int64(len(full))*2 {
		t.Fatalf("delta journal size = %d, final snapshot = %d; cumulative rewrites were not bounded", info.Size(), len(full))
	}

	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	state, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, snap, state["growing"])
}

func TestJournalDeltaRecoversCollectionsAndMapDeletes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	require.NoError(t, err)
	first := testJournalSnapshot("delta", 1, StateOpen)
	first.Messages = []MessageOperation{{Topic: "orders", Message: types.Message{Payload: "one"}}}
	first.Streams = []StreamOperation{{Topic: "events", Key: "one", Message: types.Message{Payload: "stream-one"}}}
	first.Offsets = []OffsetOperation{{Topic: "orders", Group: "workers", Offset: 1}}
	first.Participants = []Participant{{Topic: "orders", Partition: 0}}
	first.SequenceByPartition = map[string]uint64{"orders:0": 1, "removed:0": 9}
	first.RequestAssignments = map[string]RequestAssignment{"one": {Topic: "orders", Sequence: 1}, "removed": {Topic: "removed", Sequence: 9}}
	require.NoError(t, journal.Append(first))

	second := cloneSnapshot(first)
	second.Revision = 2
	second.State = StateCommitting
	second.Messages = append(second.Messages, MessageOperation{Topic: "orders", Message: types.Message{Payload: "two"}})
	second.Streams = append(second.Streams, StreamOperation{Topic: "events", Key: "two", Message: types.Message{Payload: "stream-two"}})
	second.Offsets = append(second.Offsets, OffsetOperation{Topic: "orders", Group: "workers", Offset: 2})
	second.Participants = append(second.Participants, Participant{Topic: "events", Partition: 1})
	delete(second.SequenceByPartition, "removed:0")
	second.SequenceByPartition["orders:0"] = 2
	delete(second.RequestAssignments, "removed")
	second.RequestAssignments["one"] = RequestAssignment{Topic: "orders", Sequence: 2}
	require.NoError(t, journal.Append(second))

	reopened, err := OpenJournal(path)
	require.NoError(t, err)
	state, err := reopened.Load()
	require.NoError(t, err)
	require.Equal(t, second, state["delta"])
}

func TestJournalRejectsDeltaWithoutBaseline(t *testing.T) {
	payload, err := json.Marshal(journalRecord{Version: journalFormatVersion, Delta: &snapshotDelta{
		BaseEpoch: 1, BaseRevision: 1, Metadata: *testJournalSnapshot("orphan", 2, StateCommitted),
	}})
	require.NoError(t, err)
	full, delta, _, err := decodeJournalRecord(payload)
	require.NoError(t, err)
	_, _, err = mergeJournalRecord(map[string]*Snapshot{}, full, delta)
	require.ErrorContains(t, err, "has no baseline")
}

func TestJournalDecodesVersionTwoFullSnapshot(t *testing.T) {
	want := testJournalSnapshot("version-two", 7, StateCommitted)
	payload, err := json.Marshal(journalRecord{Version: 2, Transaction: want})
	require.NoError(t, err)
	got, delta, nextEpoch, err := decodeJournalRecord(payload)
	require.NoError(t, err)
	require.Nil(t, delta)
	require.Equal(t, want, got)
	require.Equal(t, uint64(want.Epoch+1), nextEpoch)
}

func TestJournalRepairsIncompleteTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testJournalSnapshot("tx-a", 1, StateOpen)); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	// #nosec G304 -- path is the journal created beneath t.TempDir.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0, 1}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	loaded, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded["tx-a"] == nil {
		t.Fatal("valid journal record was not recovered")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("expected incomplete tail to be truncated to %d, got %d", before.Size(), after.Size())
	}
}

func TestJournalAppendDiscardsUnacknowledgedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testJournalSnapshot("tx-a", 1, StateOpen)); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Load(); err != nil {
		t.Fatal(err)
	}

	// #nosec G304 -- path is the journal created beneath t.TempDir.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0, 0, 20, '{'}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := journal.Append(testJournalSnapshot("tx-b", 1, StateCommitted)); err != nil {
		t.Fatal(err)
	}
	loaded, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 || loaded["tx-a"] == nil || loaded["tx-b"] == nil {
		t.Fatalf("unexpected recovered snapshots: %+v", loaded)
	}
}
func TestJournalRejectsCorruptNonTailRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testJournalSnapshot("tx-a", 1, StateOpen)); err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testJournalSnapshot("tx-b", 1, StateOpen)); err != nil {
		t.Fatal(err)
	}

	// #nosec G304 -- path is the journal created beneath t.TempDir.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[5] ^= 0xff
	// #nosec G703 -- path is the journal file created beneath t.TempDir.
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Load(); err == nil {
		t.Fatal("expected non-tail checksum corruption to fail recovery")
	}
}

func testJournalSnapshot(id string, revision uint64, state State) *Snapshot {
	revisionSeconds, ok := util.SafeUint64ToInt64(revision)
	if !ok {
		panic("test revision exceeds int64")
	}
	now := time.Unix(1_700_000_000+revisionSeconds, 0).UTC()
	return &Snapshot{
		ID:        id,
		Producer:  "producer-" + id,
		Epoch:     1,
		Revision:  revision,
		State:     state,
		CreatedAt: now,
		UpdatedAt: now,
	}
}
