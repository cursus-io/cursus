package disk

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func newLineageHandler(t *testing.T) (*DiskHandler, *config.Config) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	cfg.DiskFlushIntervalMS = 60_000
	d, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// A coalesced lineage request may still be syncing the log. Reset the
		// fault hook under the same I/O exclusion before Close joins the worker.
		d.ioMu.Lock()
		d.syncFileFn = nil
		d.ioMu.Unlock()
		_ = d.Close()
	})
	return d, cfg
}

func appendLineageRecords(t *testing.T, d *DiskHandler, payloads ...string) {
	t.Helper()
	for _, payload := range payloads {
		if _, err := d.AppendMessageSync("orders", 0, &types.Message{Payload: payload, LeaderEpoch: 7}); err != nil {
			t.Fatal(err)
		}
	}
}

func lineageTail(t *testing.T, d *DiskHandler, tail uint64) LineagePrefix {
	t.Helper()
	p, err := d.LineageCandidates(tail)
	if err != nil {
		t.Fatal(err)
	}
	if len(p) == 0 || len(p) > 64 || p[0].EndOffset != tail {
		t.Fatalf("invalid candidates: %+v", p)
	}
	return p[0]
}

func TestLineageCumulativePrefixRejectsReconvergence(t *testing.T) {
	a, _ := newLineageHandler(t)
	b, _ := newLineageHandler(t)
	appendLineageRecords(t, a, "same", "leader", "same-again")
	appendLineageRecords(t, b, "same", "follower", "same-again")
	for _, end := range []uint64{0, 1, 2, 3} {
		proof := lineageTail(t, b, end)
		match, err := a.VerifyLineagePrefix(proof, 3)
		if err != nil || match != (end <= 1) {
			t.Fatalf("end %d: match=%v err=%v", end, match, err)
		}
	}
	if ok, err := a.VerifyLineagePrefix(lineageTail(t, a, 3), 2); err != nil || ok {
		t.Fatalf("proof beyond HWM: %v %v", ok, err)
	}
}

func TestLineageEmptyAppendAndTruncate(t *testing.T) {
	d, _ := newLineageHandler(t)
	empty := lineageTail(t, d, 0)
	if empty.StartOffset != 0 || empty.EndOffset != 0 {
		t.Fatal(empty)
	}
	appendLineageRecords(t, d, "one", "two", "three")
	old := lineageTail(t, d, 3)
	if err := d.TruncateTo(1); err != nil {
		t.Fatal(err)
	}
	appendLineageRecords(t, d, "replacement", "three")
	if got := lineageTail(t, d, 3); got.Digest == old.Digest {
		t.Fatal("truncated prefix reused")
	}
}

func TestLineageSyncFailureDoesNotExposeProof(t *testing.T) {
	d, _ := newLineageHandler(t)
	appendLineageRecords(t, d, "one")
	_ = lineageTail(t, d, 1)
	d.ioMu.Lock()
	d.syncFileFn = func(*os.File) error { return errors.New("injected sync failure") }
	d.ioMu.Unlock()
	if _, err := d.AppendMessageSync("orders", 0, &types.Message{Payload: "failed"}); err == nil {
		t.Fatal("expected sync failure")
	}
	if p, err := d.LineageCandidates(2); err == nil || len(p) != 0 {
		t.Fatalf("published failed write: %+v %v", p, err)
	}
	d.ioMu.Lock()
	d.syncFileFn = nil
	d.ioMu.Unlock()
	if p, err := d.LineageCandidates(1); err == nil || len(p) != 0 {
		t.Fatalf("terminal writer exposed cached proof: %+v %v", p, err)
	}
}

func readLineageJournal(t *testing.T, d *DiskHandler) []lineageJournalRecord {
	t.Helper()
	data, err := os.ReadFile(d.BaseName + ".lineage")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(data, []byte(lineageMagic)) {
		t.Fatalf("invalid lineage version: %q", data)
	}
	data = data[len(lineageMagic):]
	size := binary.Size(lineageJournalRecord{})
	var records []lineageJournalRecord
	for len(data) != 0 {
		if len(data) < size+sha256.Size {
			t.Fatal("partial lineage record")
		}
		sum := sha256.Sum256(append([]byte(lineageMagic), data[:size]...))
		if !bytes.Equal(sum[:], data[size:size+sha256.Size]) {
			t.Fatal("invalid lineage checksum")
		}
		var record lineageJournalRecord
		if err := binary.Read(bytes.NewReader(data[:size]), binary.BigEndian, &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
		data = data[size+sha256.Size:]
	}
	return records
}

func TestLineageSparseCheckpointsRotationAndBatchMatching(t *testing.T) {
	a, _ := newLineageHandler(t)
	b, _ := newLineageHandler(t)
	for start := 0; start < 1500; start += 500 {
		batch := make([]types.DiskMessage, 500)
		for i := range batch {
			batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(start + i), Payload: fmt.Sprintf("record-%d", start+i)}
		}
		if err := a.WriteBatchSync(batch); err != nil {
			t.Fatal(err)
		}
		if start == 500 {
			batch[400].Payload = "divergent"
		}
		if err := b.WriteBatchSync(batch); err != nil {
			t.Fatal(err)
		}
		if err := a.RollSegmentAt(uint64(start + 500)); err != nil {
			t.Fatal(err)
		}
		if err := b.RollSegmentAt(uint64(start + 500)); err != nil {
			t.Fatal(err)
		}
	}
	// An explicit request initializes and durably publishes the full prefix.
	_ = lineageTail(t, a, 1500)
	journal := readLineageJournal(t, a)
	if journal[0].Kind != 1 || journal[0].EndOffset != 1500 || len(journal) != 1+1500/lineageCheckpointInterval {
		t.Fatalf("unexpected sparse journal: %+v", journal)
	}
	proofs, err := b.LineageCandidates(1500)
	if err != nil {
		t.Fatal(err)
	}
	match, err := a.MatchLineageCandidates(proofs, 1500)
	if err != nil || match == nil || match.EndOffset != 477 {
		t.Fatalf("match=%+v err=%v", match, err)
	}
	match, err = a.MatchLineageCandidates(proofs, 500)
	if err != nil || match == nil || match.EndOffset != 477 {
		t.Fatalf("HWM match=%+v err=%v", match, err)
	}
	for _, end := range []uint64{255, 256, 257, 499, 500, 501, 899, 900} {
		proof := lineageTail(t, a, end)
		if ok, err := b.VerifyLineagePrefix(proof, 1500); err != nil || !ok {
			t.Fatalf("checkpoint end %d: %v %v", end, ok, err)
		}
	}
	cache := a.lineage
	for i := 0; i < 30; i++ {
		appendLineageRecords(t, a, "tail")
		if a.lineage != cache {
			t.Fatal("normal append rebuilt the retained prefix")
		}
	}
	runLineagePeriodicPass(t, a)
	if got := len(readLineageJournal(t, a)); got != len(journal)+1 {
		t.Fatalf("tail rewrites grew journal: %d -> %d", len(journal), got)
	}
	if _, err := a.MatchLineageCandidates(make([]LineagePrefix, 65), 1530); err == nil {
		t.Fatal("accepted unbounded candidates")
	}
}

func TestLineageCandidateBounds(t *testing.T) {
	for _, bounds := range [][2]uint64{{0, 0}, {0, 1}, {0, 64}, {0, math.MaxUint64}, {55, math.MaxUint64}, {math.MaxUint64, math.MaxUint64}} {
		ends := lineageCandidateEnds(bounds[0], bounds[1])
		if len(ends) == 0 || len(ends) > 64 || ends[0] != bounds[1] || ends[len(ends)-1] != bounds[0] {
			t.Fatalf("bounds %v: %v", bounds, ends)
		}
		for i := 1; i < len(ends); i++ {
			if ends[i] >= ends[i-1] {
				t.Fatalf("unordered/duplicate candidates %v", ends)
			}
		}
	}
}

func TestLineageSidecarRestartRebuild(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt", "stale", "unknown-version", "valid"} {
		t.Run(mode, func(t *testing.T) {
			d, cfg := newLineageHandler(t)
			appendLineageRecords(t, d, "one")
			_ = lineageTail(t, d, 1)
			old, err := os.ReadFile(d.BaseName + ".lineage")
			if err != nil {
				t.Fatal(err)
			}
			appendLineageRecords(t, d, "two", "three")
			want := lineageTail(t, d, 3)
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(d.GetSegmentPath(0))
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing":
				err = os.Remove(d.BaseName + ".lineage")
			case "corrupt":
				err = os.WriteFile(d.BaseName+".lineage", []byte("damaged"), 0o600)
			case "stale":
				err = os.WriteFile(d.BaseName+".lineage", old, 0o600)
			case "unknown-version":
				copy(old, "CLIN9999")
				err = os.WriteFile(d.BaseName+".lineage", old, 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := NewDiskHandler(cfg, "orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reopened.Close() }()
			if got := lineageTail(t, reopened, 3); got != want {
				t.Fatalf("restart proof changed: %+v != %+v", got, want)
			}
			if records := readLineageJournal(t, reopened); records[0].EndOffset != 3 {
				t.Fatal(records)
			}
			after, err := os.ReadFile(d.GetSegmentPath(0))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("rebuild changed authoritative log: %v", err)
			}
		})
	}
}

func TestLineageSidecarDamageAndPublicationFailure(t *testing.T) {
	d, _ := newLineageHandler(t)
	appendLineageRecords(t, d, "one", "two")
	want := lineageTail(t, d, 2)
	for _, contents := range [][]byte{nil, []byte("corrupt")} {
		if contents == nil {
			if err := os.Remove(d.BaseName + ".lineage"); err != nil {
				t.Fatal(err)
			}
		} else if err := os.WriteFile(d.BaseName+".lineage", contents, 0o600); err != nil {
			t.Fatal(err)
		}
		if got := lineageTail(t, d, 2); got != want {
			t.Fatalf("rebuild changed proof: %+v", got)
		}
		_ = readLineageJournal(t, d)
	}
	if err := os.Remove(d.BaseName + ".lineage"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(d.BaseName+".lineage", 0o700); err != nil {
		t.Fatal(err)
	}
	// Sidecar failure must not reject an already durable data append.
	appendLineageRecords(t, d, "three")
	if proofs, err := d.LineageCandidates(3); err == nil || len(proofs) != 0 {
		t.Fatalf("published without durable sidecar: %+v %v", proofs, err)
	}
	if err := os.Remove(d.BaseName + ".lineage"); err != nil {
		t.Fatal(err)
	}
	_ = lineageTail(t, d, 3)
}

func TestLineageLegacyCanonicalAndUnknownEpoch(t *testing.T) {
	legacy, cfg := newLineageHandler(t)
	current, _ := newLineageHandler(t)
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	var batch []types.DiskMessage
	for i, payload := range []string{"same", "old", "same-again"} {
		message := types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: payload}
		batch = append(batch, message)
		encoded, err := util.SerializeDiskMessage(message)
		if err != nil {
			t.Fatal(err)
		}
		leaderEpochOffset := 4 + 2 + len("orders") + 4 + 8 + 2 + 8 + 8
		old := append([]byte("CDM2"), encoded[4:leaderEpochOffset]...)
		old = append(old, encoded[leaderEpochOffset+8:len(encoded)-8]...)
		if err := binary.Write(&log, binary.BigEndian, uint32(len(old))); err != nil {
			t.Fatal(err)
		}
		_, _ = log.Write(old)
	}
	if err := os.WriteFile(legacy.GetSegmentPath(0), log.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if err := current.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	want := lineageTail(t, reopened, 3)
	if got := lineageTail(t, current, 3); got != want {
		t.Fatalf("encoding changed canonical proof: %+v != %+v", got, want)
	}
	epochs, err := reopened.LineageEpochs()
	if err != nil || len(epochs) != 1 || epochs[0] != (LineageEpochSpan{StartOffset: 0, EndOffset: 3, LeaderEpoch: 0}) {
		t.Fatalf("unknown epoch spans: %+v %v", epochs, err)
	}
	divergent, _ := newLineageHandler(t)
	batch[1].Payload = "different"
	if err := divergent.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	if ok, err := divergent.VerifyLineagePrefix(want, 3); err != nil || ok {
		t.Fatalf("unknown epoch proved divergent history: %v %v", ok, err)
	}
}

func TestLineageCanonicalIncludesEveryPersistedField(t *testing.T) {
	zero := types.DiskMessage{}
	seed := lineageSeed(0)
	baseline, err := lineageHash(seed, zero)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < reflect.TypeOf(zero).NumField(); i++ {
		message := zero
		value := reflect.ValueOf(&message).Elem().Field(i)
		switch value.Kind() {
		case reflect.String:
			value.SetString("changed")
		case reflect.Int16, reflect.Int32, reflect.Int64:
			value.SetInt(1)
		case reflect.Uint32, reflect.Uint64:
			value.SetUint(1)
		case reflect.Slice:
			value.SetBytes([]byte{1})
		default:
			t.Fatalf("uncovered field type: %v", value.Kind())
		}
		digest, err := lineageHash(seed, message)
		if err != nil || digest == baseline {
			t.Fatalf("field %s absent from canonical hash: %v", reflect.TypeOf(zero).Field(i).Name, err)
		}
	}
}

func TestLineageRetentionReanchorsAndCompactionRejects(t *testing.T) {
	t.Run("retention", func(t *testing.T) {
		d, cfg := newLineageHandler(t)
		appendLineageRecords(t, d, "zero", "one", "two")
		if err := d.RollSegmentAt(3); err != nil {
			t.Fatal(err)
		}
		appendLineageRecords(t, d, "three", "four")
		old := lineageTail(t, d, 5)
		d.SetRetentionPolicy(0, 1)
		d.EnforceRetention(cfg)
		proof := lineageTail(t, d, 5)
		if proof.StartOffset != 3 || proof.Digest == old.Digest {
			t.Fatalf("retention did not reanchor: %+v", proof)
		}
		if ok, err := d.VerifyLineagePrefix(old, 5); err != nil || ok {
			t.Fatalf("different retained starts compared: %v %v", ok, err)
		}
		if _, err := d.LineageCandidates(2); err == nil {
			t.Fatal("accepted tail before retention")
		}
	})
	t.Run("compaction", func(t *testing.T) {
		d, _ := newLineageHandler(t)
		d.SetCleanupPolicy(config.CleanupPolicyCompact)
		for _, payload := range []string{"old", "new"} {
			if _, err := d.AppendMessageSync("orders", 0, &types.Message{Key: "key", Payload: payload}); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.RollSegmentAt(2); err != nil {
			t.Fatal(err)
		}
		_ = lineageTail(t, d, 2)
		result, err := d.EnforceCompaction()
		if err != nil || result.RecordsRemoved != 1 {
			t.Fatalf("compaction: %+v %v", result, err)
		}
		if proofs, err := d.LineageCandidates(2); !errors.Is(err, ErrLineageUnsupported) || len(proofs) != 0 {
			t.Fatalf("compacted proof: %+v %v", proofs, err)
		}
	})
	t.Run("replica-range", func(t *testing.T) {
		d, _ := newLineageHandler(t)
		if err := d.WriteCompactedReplicaRange(0, 4, []types.DiskMessage{{Topic: "orders", Offset: 3, Payload: "retained"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := d.LineageCandidates(4); !errors.Is(err, ErrLineageUnsupported) {
			t.Fatalf("range proof: %v", err)
		}
	})
}

func TestLineageExplicitProofSyncAndClosed(t *testing.T) {
	d, cfg := newLineageHandler(t)
	_ = lineageTail(t, d, 0)
	if err := d.WriteBatch([]types.DiskMessage{{Topic: "orders", Offset: 0, Payload: "async"}}); err != nil {
		t.Fatal(err)
	}
	if records := readLineageJournal(t, d); records[0].EndOffset != 0 {
		t.Fatal("published before log sync")
	}
	_ = lineageTail(t, d, 1)
	if records := readLineageJournal(t, d); records[0].EndOffset != 1 {
		t.Fatal("proof did not publish durable tail")
	}
	if err := d.WriteBatch([]types.DiskMessage{{Topic: "orders", Offset: 1, Payload: "shutdown"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if records := readLineageJournal(t, d); records[0].EndOffset != 1 {
		t.Fatal("shutdown unexpectedly published sidecar")
	}
	if _, err := d.LineageCandidates(2); err == nil {
		t.Fatal("closed handler exposed proof")
	}
	reopened, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	_ = lineageTail(t, reopened, 2)
}

func TestLineageEpochTransitions(t *testing.T) {
	d, _ := newLineageHandler(t)
	batch := make([]types.DiskMessage, 6)
	for i, epoch := range []int64{0, 0, 7, 7, 0, 8} {
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), LeaderEpoch: epoch}
	}
	if err := d.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	want := []LineageEpochSpan{{StartOffset: 0, EndOffset: 2}, {StartOffset: 2, EndOffset: 4, LeaderEpoch: 7}, {StartOffset: 4, EndOffset: 5}, {StartOffset: 5, EndOffset: 6, LeaderEpoch: 8}}
	spans, err := d.LineageEpochs()
	if err != nil || !reflect.DeepEqual(spans, want) {
		t.Fatalf("epoch spans: %+v %v", spans, err)
	}
	spans[0].LeaderEpoch = 99
	spans, err = d.LineageEpochs()
	if err != nil || !reflect.DeepEqual(spans, want) {
		t.Fatal("epoch API returned mutable cache")
	}
	records := readLineageJournal(t, d)
	if len(records) != 4 || records[0].EpochStart != 5 || records[0].LeaderEpoch != 8 {
		t.Fatalf("epoch journal: %+v", records)
	}
}

func TestLineagePublicationFailureSuppressesAutomaticRetries(t *testing.T) {
	d, _ := newLineageHandler(t)
	batch := make([]types.DiskMessage, 1024)
	for i := range batch {
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: "retained"}
	}
	if err := d.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	_ = lineageTail(t, d, 1024)
	path := d.BaseName + ".lineage"
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	appendLineageRecords(t, d, "publication-fails")
	runLineagePeriodicPass(t, d)
	for i := 0; i < 8; i++ {
		appendLineageRecords(t, d, "still-durable")
		d.Flush()
		runLineagePeriodicPass(t, d)
	}
	if proofs, err := d.LineageCandidates(1033); err == nil || len(proofs) != 0 {
		t.Fatalf("proof escaped failed publication: %+v %v", proofs, err)
	}
	// Removing the obstruction makes any accidental automatic retry observable:
	// it would rehash the full retained history and create the sidecar again.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	appendLineageRecords(t, d, "await-explicit-retry")
	d.Flush()
	runLineagePeriodicPass(t, d)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("automatic rebuild retried after failure: %v", err)
	}
	_ = lineageTail(t, d, 1034)
	if records := readLineageJournal(t, d); records[0].EndOffset != 1034 {
		t.Fatal("explicit retry lost durable appends")
	}
	cache := d.lineageProgress
	appendLineageRecords(t, d, "incremental-again")
	runLineagePeriodicPass(t, d)
	if d.lineageProgress != cache {
		t.Fatal("recovery did not resume incremental publication")
	}
	if records := readLineageJournal(t, d); records[0].EndOffset != 1035 {
		t.Fatal("publication stayed suppressed after recovery")
	}
}

func TestLineagePeriodicSyncPublishes(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	cfg.DiskFlushIntervalMS = 10
	d, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	_ = lineageTail(t, d, 0)
	published := make(chan struct{}, 1)
	d.SetOnSync(func(offset uint64) {
		if offset >= 1 {
			select {
			case published <- struct{}{}:
			default:
			}
		}
	})
	if err := d.WriteBatch([]types.DiskMessage{{Topic: "orders", Offset: 0, Payload: "periodic"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("periodic sync did not complete")
	}
}

func TestLineageConcurrentProofAppendRotationAndRetention(t *testing.T) {
	d, cfg := newLineageHandler(t)
	appendLineageRecords(t, d, "zero", "one", "two")
	// Keep the prefix stable while retention locks and secures closed segments.
	// Actual deletion and reanchoring are covered by the retention test above.
	d.SetRetentionPolicy(0, 0)
	start := make(chan struct{})
	errs := make(chan error, 3)
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 24; i++ {
			if _, err := d.AppendMessageSync("orders", 0, &types.Message{Payload: "concurrent"}); err != nil {
				errs <- err
				return
			}
			if i%4 == 3 {
				if err := d.RollSegmentAt(d.GetFlushedOffset()); err != nil {
					errs <- err
					return
				}
			}
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 24; i++ {
			d.EnforceRetention(cfg)
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 24; i++ {
			proofs, err := d.LineageCandidates(3)
			if err != nil {
				errs <- err
				return
			}
			match, err := d.MatchLineageCandidates(proofs, d.GetFlushedOffset())
			if err != nil || match == nil || *match != proofs[0] {
				errs <- fmt.Errorf("concurrent prefix mismatch: %+v %v", match, err)
				return
			}
		}
	}()
	close(start)
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	select {
	case <-finished:
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("concurrent lineage operations stalled")
	}
}
