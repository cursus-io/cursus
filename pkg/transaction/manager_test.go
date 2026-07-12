package transaction

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
)

func TestManagerFencesLowerProducerEpoch(t *testing.T) {
	m := NewManager()
	if err := m.Begin("tx-1", "producer-1", 3); err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if err := m.Abort("tx-1"); err != nil {
		t.Fatalf("abort failed: %v", err)
	}
	if err := m.Begin("tx-1", "producer-1", 2); err == nil {
		t.Fatal("expected lower epoch to be fenced")
	}
}

func TestManagerRejectsNonOwnerOperations(t *testing.T) {
	m := NewManager()
	if err := m.Begin("tx-1", "producer-1", 4); err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	err := m.AddMessage("tx-1", "other-producer", 4, MessageOperation{Topic: "t1", Message: types.Message{Payload: "x"}})
	if err == nil {
		t.Fatal("expected producer mismatch to fail")
	}
	err = m.AddMessage("tx-1", "producer-1", 3, MessageOperation{Topic: "t1", Message: types.Message{Payload: "x"}})
	if err == nil {
		t.Fatal("expected stale epoch to fail")
	}
}

func TestManagerExportImportState(t *testing.T) {
	m := NewManager()
	if err := m.Begin("tx-1", "producer-1", 1); err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if err := m.AddOffsets("tx-1", "producer-1", 1, []OffsetOperation{{Topic: "t1", Group: "g1", Member: "m1", Generation: 2, Partition: 0, Offset: 9}}); err != nil {
		t.Fatalf("add offsets failed: %v", err)
	}

	restored := NewManager()
	restored.ImportState(m.ExportState())
	tx, err := restored.Status("tx-1")
	if err != nil {
		t.Fatalf("restored status failed: %v", err)
	}
	if tx.Producer != "producer-1" || tx.Epoch != 1 || len(tx.Offsets) != 1 || tx.Offsets[0].Offset != 9 {
		t.Fatalf("unexpected restored transaction: %+v", tx)
	}
}
