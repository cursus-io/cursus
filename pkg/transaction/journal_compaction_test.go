package transaction

import (
	"os"
	"path/filepath"
	"testing"
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
