package transaction

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenJournalSyncsNewFileAndDirectoryEntries(t *testing.T) {
	originalSync := syncJournalCreationDirectory
	t.Cleanup(func() { syncJournalCreationDirectory = originalSync })

	root := t.TempDir()
	dir := filepath.Join(root, "state")
	path := filepath.Join(dir, "transactions.journal")
	var synced []string
	syncJournalCreationDirectory = func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return nil
	}

	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if journal == nil {
		t.Fatal("OpenJournal returned a nil journal")
	}
	if len(synced) != 2 || synced[0] != dir || synced[1] != root {
		t.Fatalf("directory syncs = %v, want [%s %s]", synced, dir, root)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat journal: %v", err)
	}
}

func TestOpenJournalResyncsExistingJournalDirectory(t *testing.T) {
	originalSync := syncJournalCreationDirectory
	t.Cleanup(func() { syncJournalCreationDirectory = originalSync })

	dir := t.TempDir()
	path := filepath.Join(dir, "transactions.journal")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var synced []string
	syncJournalCreationDirectory = func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return nil
	}

	if _, err := OpenJournal(path); err != nil {
		t.Fatal(err)
	}
	if len(synced) != 1 || synced[0] != filepath.Clean(dir) {
		t.Fatalf("directory syncs = %v, want existing journal directory %s", synced, dir)
	}
}

func TestOpenJournalRejectsDirectorySyncFailure(t *testing.T) {
	originalSync := syncJournalCreationDirectory
	t.Cleanup(func() { syncJournalCreationDirectory = originalSync })
	syncJournalCreationDirectory = func(string) error {
		return errors.New("injected directory sync failure")
	}

	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if journal != nil || err == nil || !strings.Contains(err.Error(), "persist transaction journal entry") {
		t.Fatalf("OpenJournal = (%v, %v), want no journal and directory sync failure", journal, err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("journal remains visible after failed creation sync: %v", statErr)
	}
}

func TestJournalAppendDoesNotRecreateMissingAuthoritativeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transactions.journal")
	journal, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	err = journal.Append(testJournalSnapshot("tx-missing", 1, StateOpen))
	if err == nil || !strings.Contains(err.Error(), "open transaction journal") {
		t.Fatalf("Append error = %v, want missing authoritative journal failure", err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("append recreated missing journal: %v", statErr)
	}
}
