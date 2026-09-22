package transaction

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
)

func TestJournalCompactionKeepsLatestSnapshotPerID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}

	for revision := uint64(1); revision <= journalCompactionRecords+2; revision++ {
		id := "tx-a"
		if revision%2 == 0 {
			id = "tx-b"
		}
		if err := journal.Append(testJournalSnapshot(id, revision, StateCommitted)); err != nil {
			t.Fatalf("append revision %d: %v", revision, err)
		}
	}
	if !journal.shouldCompactLocked() {
		t.Fatalf("journal did not accumulate superseded record debt: records=%d latest=%d", journal.records, len(journal.latest))
	}

	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Load(); err != nil {
		t.Fatal(err)
	}
	if !reopened.shouldCompactLocked() {
		t.Fatalf("journal lost superseded record debt on restart: records=%d latest=%d", reopened.records, len(reopened.latest))
	}
	if err := reopened.Append(testJournalSnapshot("tx-a", journalCompactionRecords+3, StateCommitted)); err != nil {
		t.Fatal(err)
	}
	if reopened.records >= journalCompactionRecords {
		t.Fatalf("journal record count was not compacted: %d", reopened.records)
	}
	state, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := state["tx-a"].Revision; got != journalCompactionRecords+3 {
		t.Fatalf("tx-a revision = %d, want %d", got, journalCompactionRecords+3)
	}
	if got := state["tx-b"].Revision; got != journalCompactionRecords+2 {
		t.Fatalf("tx-b revision = %d, want %d", got, journalCompactionRecords+2)
	}
	if matches, err := filepath.Glob(path + ".compact-*"); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("compaction left temporary files: %v", matches)
	}
	if info, err := os.Stat(path); err != nil || info.Size() >= journalCompactionBytes {
		t.Fatalf("compacted journal size = %v, err = %v", info, err)
	}
}

func TestJournalDoesNotCompactLiveDistinctIDs(t *testing.T) {
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.journal"))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < journalCompactionRecords+2; i++ {
		if err := journal.Append(testJournalSnapshot("tx-"+strconv.Itoa(i), 1, StateCommitted)); err != nil {
			t.Fatalf("append distinct transaction %d: %v", i, err)
		}
	}
	if journal.shouldCompactLocked() {
		t.Fatalf("live distinct transaction IDs triggered compaction: records=%d latest=%d bytes=%d", journal.records, len(journal.latest), journal.validEnd)
	}
	reopened, err := OpenJournal(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Load(); err != nil {
		t.Fatal(err)
	}
	if reopened.shouldCompactLocked() {
		t.Fatalf("restart made live distinct transaction IDs compactable: records=%d latest=%d bytes=%d", reopened.records, len(reopened.latest), reopened.validEnd)
	}
}

func TestJournalDoesNotCompactLargeLiveSnapshot(t *testing.T) {
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.journal"))
	if err != nil {
		t.Fatal(err)
	}
	snap := testJournalSnapshot("large-live", 1, StateCommitted)
	snap.Messages = []MessageOperation{{Message: types.Message{Payload: strings.Repeat("x", journalCompactionBytes)}}}
	if err := journal.Append(snap); err != nil {
		t.Fatal(err)
	}
	if journal.validEnd < journalCompactionBytes {
		t.Fatalf("journal did not exceed byte threshold: %d", journal.validEnd)
	}
	if journal.shouldCompactLocked() {
		t.Fatalf("large live snapshot triggered compaction: records=%d latest=%d bytes=%d", journal.records, len(journal.latest), journal.validEnd)
	}
}

func TestJournalCompactsSupersededBytes(t *testing.T) {
	journal, err := OpenJournal(filepath.Join(t.TempDir(), "transactions.journal"))
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", journalCompactionBytes/2)
	for revision := uint64(1); revision <= 3; revision++ {
		snap := testJournalSnapshot("large-repeated", revision, StateCommitted)
		snap.Messages = []MessageOperation{{Message: types.Message{Payload: payload}}}
		if err := journal.Append(snap); err != nil {
			t.Fatalf("append revision %d: %v", revision, err)
		}
	}
	if !journal.shouldCompactLocked() {
		t.Fatalf("superseded bytes did not trigger compaction: valid=%d live=%d", journal.validEnd, journal.latestBytes)
	}

	snap := testJournalSnapshot("large-repeated", 4, StateCommitted)
	snap.Messages = []MessageOperation{{Message: types.Message{Payload: payload}}}
	if err := journal.Append(snap); err != nil {
		t.Fatal(err)
	}
	if journal.records != 2 {
		t.Fatalf("records after byte compaction = %d, want 2", journal.records)
	}
	state, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := state["large-repeated"]; got == nil || got.Revision != 4 {
		t.Fatalf("latest large snapshot = %+v, want revision 4", got)
	}
}

func TestJournalRewriteDoesNotResurrectRemovedTransactions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(testJournalSnapshot("expired", 1, StateCommitted)); err != nil {
		t.Fatal(err)
	}
	keep := testJournalSnapshot("active", 2, StateOpen)
	if err := journal.Append(keep); err != nil {
		t.Fatal(err)
	}
	if err := journal.Rewrite(map[string]*Snapshot{"active": keep}); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	state, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := state["expired"]; ok {
		t.Fatal("removed transaction was resurrected after journal rewrite")
	}
	if got := state["active"]; got == nil || got.Revision != keep.Revision {
		t.Fatalf("active transaction = %+v, want revision %d", got, keep.Revision)
	}
}

func TestJournalLoadReturnsIsolatedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testJournalSnapshot("isolated", 3, StateOpen)
	snapshot.Offsets = []OffsetOperation{{Topic: "orders", Group: "workers", Partition: 0, Offset: 11}}
	snapshot.Messages = []MessageOperation{{
		Topic:     "orders",
		Partition: 0,
		Message: types.Message{
			Payload:           "original",
			ControlBatchKey:   []byte{1, 2},
			ControlBatchValue: []byte{3, 4},
		},
	}}
	if err := journal.Append(snapshot); err != nil {
		t.Fatal(err)
	}

	loaded, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	loaded["isolated"].Revision = 99
	loaded["isolated"].Offsets[0].Offset = 99
	loaded["isolated"].Messages[0].Message.Payload = "mutated"
	loaded["isolated"].Messages[0].Message.ControlBatchKey[0] = 99

	private := journal.latest["isolated"]
	if private.Revision != 3 || private.Offsets[0].Offset != 11 ||
		private.Messages[0].Message.Payload != "original" ||
		private.Messages[0].Message.ControlBatchKey[0] != 1 {
		t.Fatalf("caller mutated private journal state: %+v", private)
	}
}
