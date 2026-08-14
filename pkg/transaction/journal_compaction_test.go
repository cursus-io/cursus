package transaction

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
)

func TestJournalCompactionKeepsLatestSnapshotPerID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}

	for revision := uint64(1); revision <= journalCompactionRecords+1; revision++ {
		id := "tx-a"
		if revision%2 == 0 {
			id = "tx-b"
		}
		if err := journal.Append(testJournalSnapshot(id, revision, StateCommitted)); err != nil {
			t.Fatalf("append revision %d: %v", revision, err)
		}
	}
	if journal.records >= journalCompactionRecords {
		t.Fatalf("journal record count was not compacted: %d", journal.records)
	}

	reopened, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	state, err := reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := state["tx-a"].Revision; got != journalCompactionRecords+1 {
		t.Fatalf("tx-a revision = %d, want %d", got, journalCompactionRecords+1)
	}
	if got := state["tx-b"].Revision; got != journalCompactionRecords {
		t.Fatalf("tx-b revision = %d, want %d", got, journalCompactionRecords)
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
