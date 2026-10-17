package disk

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
)

// runLineagePeriodicPass executes the sync-loop publication boundary without
// timer races, keeping the same data sync and lock order as production.
func runLineagePeriodicPass(t *testing.T, d *DiskHandler) {
	t.Helper()
	unlock, err := d.lockLineageBuilder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	err = d.prepareLineage(context.Background(), true)
	if err != nil && !errors.Is(err, errLineageRebuildRequired) {
		t.Fatal(err)
	}
}
func lineageSidecarBytes(t *testing.T, d *DiskHandler) []byte {
	t.Helper()
	data, err := os.ReadFile(d.BaseName + ".lineage")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestLineageInitializationIsLazy(t *testing.T) {
	d, cfg := newLineageHandler(t)
	path := d.BaseName + ".lineage"
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("startup published lineage: %v", err)
	}
	appendLineageRecords(t, d, "zero", "one", "two")
	runLineagePeriodicPass(t, d)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("write/background initialized lineage: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	stale := []byte("old unsupported sidecar must remain untouched until requested")
	if err := os.WriteFile(path, stale, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	appendLineageRecords(t, reopened, "three")
	if err := reopened.RollSegmentAt(4); err != nil {
		t.Fatal(err)
	}
	reopened.Flush()
	if err := reopened.TruncateTo(3); err != nil {
		t.Fatal(err)
	}
	runLineagePeriodicPass(t, reopened)
	if reopened.lineage != nil || !bytes.Equal(lineageSidecarBytes(t, reopened), stale) {
		t.Fatal("restart/write/maintenance rebuilt lineage eagerly")
	}
	_ = lineageTail(t, reopened, 3)
	if records := readLineageJournal(t, reopened); records[0].EndOffset != 3 {
		t.Fatal("explicit proof did not initialize durable lineage")
	}
}

func TestLineageSynchronousPathsDoNotPublish(t *testing.T) {
	d, _ := newLineageHandler(t)
	_ = lineageTail(t, d, 0)
	initial := lineageSidecarBytes(t, d)
	assertUnchanged := func() {
		t.Helper()
		if !bytes.Equal(initial, lineageSidecarBytes(t, d)) {
			t.Fatal("synchronous log path published sidecar")
		}
	}
	appendLineageRecords(t, d, "direct")
	assertUnchanged()
	if err := d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 1, Payload: "batch"}}); err != nil {
		t.Fatal(err)
	}
	assertUnchanged()
	if err := d.WriteBatch([]types.DiskMessage{{Topic: "orders", Offset: 2, Payload: "async"}}); err != nil {
		t.Fatal(err)
	}
	d.Flush()
	assertUnchanged()
	if err := d.RollSegmentAt(3); err != nil {
		t.Fatal(err)
	}
	assertUnchanged()
	appendLineageRecords(t, d, "shutdown")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	assertUnchanged()
}

func TestLineageBackgroundPublicationBounded(t *testing.T) {
	d, _ := newLineageHandler(t)
	_ = lineageTail(t, d, 0)
	cache := d.lineageProgress
	batch := make([]types.DiskMessage, 3073)
	for i := range batch {
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: "record"}
	}
	if err := d.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	for _, want := range []uint64{1024, 2048} {
		runLineagePeriodicPass(t, d)
		if records := readLineageJournal(t, d); records[0].EndOffset != want {
			t.Fatalf("background tail=%d want=%d", records[0].EndOffset, want)
		}
		if d.lineageProgress != cache {
			t.Fatal("background pass rebuilt retained history")
		}
	}
	proof := lineageTail(t, d, 3073)
	if records := readLineageJournal(t, d); records[0].EndOffset != 3073 {
		t.Fatal("explicit proof did not catch up completely")
	}
	rebuilt, _ := newLineageHandler(t)
	if err := rebuilt.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	if want := lineageTail(t, rebuilt, 3073); proof != want {
		t.Fatal("bounded background checkpoints changed cumulative prefix")
	}
	complete := lineageSidecarBytes(t, d)
	runLineagePeriodicPass(t, d)
	if !bytes.Equal(complete, lineageSidecarBytes(t, d)) {
		t.Fatal("idle pass rewrote sidecar")
	}
}

func TestLineageBackgroundDoesNotRebuildInvalidated(t *testing.T) {
	for _, mode := range []string{"missing", "stale", "replaced", "truncate", "retention"} {
		t.Run(mode, func(t *testing.T) {
			d, cfg := newLineageHandler(t)
			appendLineageRecords(t, d, "zero", "one", "two")
			if err := d.RollSegmentAt(3); err != nil {
				t.Fatal(err)
			}
			_ = lineageTail(t, d, 3)
			old := lineageSidecarBytes(t, d)
			appendLineageRecords(t, d, "three", "four", "five")
			_ = lineageTail(t, d, 6)
			path := d.BaseName + ".lineage"
			switch mode {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("replacement-sidecar"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "stale":
				if err := os.WriteFile(path, old, 0o600); err != nil {
					t.Fatal(err)
				}
			case "truncate":
				if err := d.TruncateTo(4); err != nil {
					t.Fatal(err)
				}
			case "retention":
				d.SetRetentionPolicy(0, 1)
				d.EnforceRetention(cfg)
			}
			var before []byte
			if mode != "missing" {
				before = lineageSidecarBytes(t, d)
			}
			runLineagePeriodicPass(t, d)
			appendLineageRecords(t, d, "after-invalidation")
			runLineagePeriodicPass(t, d)
			if mode == "missing" {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("background recreated missing sidecar: %v", err)
				}
			} else if !bytes.Equal(before, lineageSidecarBytes(t, d)) {
				t.Fatal("background rebuilt invalidated sidecar")
			}
			if d.lineage != nil {
				t.Fatal("background exposed invalidated cache")
			}
			proof := lineageTail(t, d, d.GetFlushedOffset())
			if proof.StartOffset != d.GetFirstOffset() {
				t.Fatal("proof retry used stale retained start")
			}
		})
	}
}

// rewriteLineagePreservingStat reproduces a same-inode, same-size rewrite whose
// timestamp is indistinguishable from the last successful publication.
func rewriteLineagePreservingStat(t *testing.T, path string, content []byte) {
	t.Helper()
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != int64(len(content)) {
		t.Fatal("fixture changed sidecar size")
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(content, 0); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || !os.SameFile(before, after) {
		t.Fatal("fixture did not preserve cached file metadata")
	}
}

func TestLineageHeaderValidationWithUnchangedStat(t *testing.T) {
	for _, trigger := range []string{"background-idle", "background-append", "explicit"} {
		for _, damage := range []string{"stale-tail", "magic", "checksum"} {
			t.Run(trigger+"/"+damage, func(t *testing.T) {
				d, _ := newLineageHandler(t)
				appendLineageRecords(t, d, "zero", "one", "two")
				_ = lineageTail(t, d, 3)
				old := lineageSidecarBytes(t, d)
				appendLineageRecords(t, d, "three", "four", "five")
				want := lineageTail(t, d, 6)
				published := lineageSidecarBytes(t, d)
				if len(old) != 128 || len(published) != 128 {
					t.Fatal("fixture must contain only the fixed header")
				}
				damaged := append([]byte(nil), published...)
				switch damage {
				case "stale-tail":
					damaged = old
				case "magic":
					damaged[0] ^= 1
				case "checksum":
					damaged[len(damaged)-1] ^= 1
				}
				rewriteLineagePreservingStat(t, d.BaseName+".lineage", damaged)
				if trigger == "explicit" {
					if got := lineageTail(t, d, 6); got != want {
						t.Fatal("rebuilding changed prefix")
					}
					if !bytes.Equal(lineageSidecarBytes(t, d), published) {
						t.Fatal("unchanged-tail proof reused stale or corrupt publication")
					}
					_ = readLineageJournal(t, d)
					return
				}
				if trigger == "background-append" {
					appendLineageRecords(t, d, "six")
				}
				runLineagePeriodicPass(t, d)
				if !bytes.Equal(lineageSidecarBytes(t, d), damaged) {
					t.Fatal("background overwrote invalidated publication")
				}
				if d.lineage != nil || !d.lineageAutoSuppressed {
					t.Fatal("background retained cache despite changed header")
				}
				appendLineageRecords(t, d, "after-invalidation")
				runLineagePeriodicPass(t, d)
				if !bytes.Equal(lineageSidecarBytes(t, d), damaged) {
					t.Fatal("background retried invalidated publication")
				}
				if ok, err := d.VerifyLineagePrefix(want, d.GetFlushedOffset()); err != nil || !ok {
					t.Fatalf("explicit rebuild changed prefix: %v %v", ok, err)
				}
				if records := readLineageJournal(t, d); records[0].EndOffset != d.GetFlushedOffset() {
					t.Fatal("explicit rebuild did not durably catch up")
				}
			})
		}
	}
}
