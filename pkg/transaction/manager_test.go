package transaction

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
)

func beginInitialized(t *testing.T, m *Manager, txnID string) (string, int64) {
	t.Helper()
	producer, epoch, err := m.InitProducer(txnID)
	if err != nil {
		t.Fatalf("init producer failed: %v", err)
	}
	if err := m.Begin(txnID, producer, epoch); err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	return producer, epoch
}

func TestManagerRejectsBeginWithoutInitProducer(t *testing.T) {
	m := NewManager()
	if err := m.Begin("tx-1", "producer-1", 0); err == nil {
		t.Fatal("expected uninitialized transaction begin to fail")
	}
}

func TestManagerFencesLowerProducerEpoch(t *testing.T) {
	m := NewManager()
	producer, epoch := beginInitialized(t, m, "tx-1")
	if err := m.Abort("tx-1", producer, epoch); err != nil {
		t.Fatalf("abort failed: %v", err)
	}
	if err := m.Begin("tx-1", producer, epoch-1); err == nil {
		t.Fatal("expected lower epoch to be fenced")
	}
	if err := m.Begin("tx-1", producer, epoch+1); err == nil {
		t.Fatal("expected uninitialized higher epoch to be fenced")
	}
}

func TestManagerRejectsNonOwnerOperations(t *testing.T) {
	m := NewManager()
	producer, epoch := beginInitialized(t, m, "tx-1")
	err := m.AddMessage("tx-1", "other-producer", epoch, MessageOperation{Topic: "t1", Message: types.Message{Payload: "x"}})
	if err == nil {
		t.Fatal("expected producer mismatch to fail")
	}
	err = m.AddMessage("tx-1", producer, epoch-1, MessageOperation{Topic: "t1", Message: types.Message{Payload: "x"}})
	if err == nil {
		t.Fatal("expected stale epoch to fail")
	}
}

func TestManagerExportImportState(t *testing.T) {
	m := NewManager()
	producer, epoch := beginInitialized(t, m, "tx-1")
	if err := m.AddOffsets("tx-1", producer, epoch, []OffsetOperation{{Topic: "t1", Group: "g1", Member: "m1", Generation: 2, Partition: 0, Offset: 9}}); err != nil {
		t.Fatalf("add offsets failed: %v", err)
	}

	restored := NewManager()
	restored.ImportState(m.ExportState())
	tx, err := restored.Status("tx-1")
	if err != nil {
		t.Fatalf("restored status failed: %v", err)
	}
	if tx.Producer != producer || tx.Epoch != epoch || len(tx.Offsets) != 1 || tx.Offsets[0].Offset != 9 {
		t.Fatalf("unexpected restored transaction: %+v", tx)
	}
}

func TestManagerFinalStateIsIdempotentForSameOwner(t *testing.T) {
	m := NewManager()
	producer, epoch := beginInitialized(t, m, "tx-1")
	if _, err := m.PrepareCommit("tx-1", producer, epoch); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	if err := m.Commit("tx-1"); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if _, err := m.PrepareCommit("tx-1", producer, epoch); err != nil {
		t.Fatalf("same-owner retry should be idempotent: %v", err)
	}
	if _, err := m.PrepareCommit("tx-1", producer, epoch-1); err == nil {
		t.Fatal("expected stale epoch retry to be fenced")
	}
}

func TestManagerInitProducerBumpsEpochAndFencesOldProducer(t *testing.T) {
	m := NewManager()
	producer, epoch, err := m.InitProducer("tx-init")
	if err != nil {
		t.Fatalf("init producer failed: %v", err)
	}
	if producer == "" || epoch != 0 {
		t.Fatalf("unexpected first producer session producer=%s epoch=%d", producer, epoch)
	}
	if err := m.Begin("tx-init", producer, epoch); err != nil {
		t.Fatalf("begin failed: %v", err)
	}

	producer2, epoch2, err := m.InitProducer("tx-init")
	if err != nil {
		t.Fatalf("second init producer failed: %v", err)
	}
	if producer2 != producer || epoch2 != epoch+1 {
		t.Fatalf("unexpected bumped producer session producer=%s epoch=%d", producer2, epoch2)
	}
	if err := m.AddMessage("tx-init", producer, epoch, MessageOperation{Topic: "t1", Message: types.Message{Payload: "zombie"}}); err == nil {
		t.Fatal("expected old epoch to be fenced")
	}
	if err := m.Begin("tx-init", producer2, epoch2); err != nil {
		t.Fatalf("begin with bumped epoch failed: %v", err)
	}
}

func TestManagerInitProducerStateSurvivesExportImport(t *testing.T) {
	m := NewManager()
	producer, epoch, err := m.InitProducer("tx-restore-producer")
	if err != nil {
		t.Fatalf("init producer failed: %v", err)
	}

	restored := NewManager()
	restored.ImportState(m.ExportState())
	producer2, epoch2, err := restored.InitProducer("tx-restore-producer")
	if err != nil {
		t.Fatalf("restored init producer failed: %v", err)
	}
	if producer2 != producer || epoch2 != epoch+1 {
		t.Fatalf("expected restored epoch bump producer=%s epoch=%d, got producer=%s epoch=%d", producer, epoch+1, producer2, epoch2)
	}
}

func TestManagerInitProducerRejectsCommittingTransaction(t *testing.T) {
	m := NewManager()
	producer, epoch := beginInitialized(t, m, "tx-committing")
	if _, err := m.PrepareCommit("tx-committing", producer, epoch); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	if _, _, err := m.InitProducer("tx-committing"); err == nil {
		t.Fatal("expected committing transaction to reject producer reinitialization")
	}
}

func TestManagerDeleteRemovesOneTransaction(t *testing.T) {
	m := NewManager()
	_, _ = beginInitialized(t, m, "tx-delete")
	keepProducer, keepEpoch := beginInitialized(t, m, "tx-keep")

	m.Delete("tx-delete")

	if _, err := m.Status("tx-delete"); err == nil {
		t.Fatal("expected deleted transaction to be missing")
	}
	tx, err := m.Status("tx-keep")
	if err != nil {
		t.Fatalf("expected unrelated transaction to remain: %v", err)
	}
	if tx.Producer != keepProducer || tx.Epoch != keepEpoch {
		t.Fatalf("unexpected remaining transaction: %+v", tx)
	}
}

func TestManagerPruneExpiredKeepsActiveTransactions(t *testing.T) {
	m := NewManagerWithExpiration(time.Hour)
	old := time.Now().Add(-2 * time.Hour)
	m.ApplySnapshot(&Snapshot{ID: "committed", Producer: "p1", Epoch: 0, State: StateCommitted, CreatedAt: old, UpdatedAt: old})
	m.ApplySnapshot(&Snapshot{ID: "aborted", Producer: "p2", Epoch: 0, State: StateAborted, CreatedAt: old, UpdatedAt: old})
	m.ApplySnapshot(&Snapshot{ID: "open", Producer: "p3", Epoch: 0, State: StateOpen, CreatedAt: old, UpdatedAt: old})
	m.ApplySnapshot(&Snapshot{ID: "committing", Producer: "p4", Epoch: 0, State: StateCommitting, CreatedAt: old, UpdatedAt: old})

	if removed := m.PruneExpired(time.Now()); removed != 2 {
		t.Fatalf("expected 2 terminal transactions to expire, got %d", removed)
	}
	if _, err := m.Status("committed"); err == nil {
		t.Fatal("expected committed transaction to expire")
	}
	if _, err := m.Status("aborted"); err == nil {
		t.Fatal("expected aborted transaction to expire")
	}
	if _, err := m.Status("open"); err != nil {
		t.Fatalf("expected open transaction to remain: %v", err)
	}
	if _, err := m.Status("committing"); err != nil {
		t.Fatalf("expected committing transaction to remain: %v", err)
	}
}
