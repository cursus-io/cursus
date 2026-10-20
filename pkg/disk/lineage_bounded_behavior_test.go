package disk

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
)

// This regression also runs against the original implementation: an interrupted
// cold proof must leave completed durable chunks instead of restarting at zero.
func TestLineageCanceledColdBuildPersistsCompletedChunks(t *testing.T) {
	d, _ := newLineageHandler(t)
	batch := make([]types.DiskMessage, 6000)
	for i := range batch {
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: "record"}
	}
	if err := d.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	ctx := &scanCancelContext{Context: context.Background(), limit: 2500}
	_, err := d.MatchLineageCandidatesContext(ctx, nil, 6000)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want cancellation: %v", err)
	}
	if _, err := os.Stat(d.BaseName + ".lineage"); err != nil {
		t.Fatalf("completed chunks were discarded: %v", err)
	}
	if got := readLineageJournal(t, d)[0].EndOffset; got == 0 || got >= 6000 {
		t.Fatalf("expected a completed partial chunk, got %d", got)
	}
	if d.lineage != nil {
		t.Fatal("unfinished target became trusted")
	}
	if _, err := d.MatchLineageCandidatesContext(context.Background(), nil, 6000); err != nil {
		t.Fatal(err)
	}
	if got := readLineageJournal(t, d)[0].EndOffset; got != 6000 {
		t.Fatalf("resume tail %d", got)
	}
}

func TestLineageRejectsWrongRecordIdentity(t *testing.T) {
	d, _ := newLineageHandler(t)
	if err := d.WriteBatchSync([]types.DiskMessage{{Topic: "another-topic", Offset: 0, Payload: "record"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.LineageCandidates(1); err == nil {
		t.Fatal("wrong partition identity became cumulative evidence")
	}
}
