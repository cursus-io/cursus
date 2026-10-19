package disk

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
)

func TestLineageCanceledChunksResumeLinearWork(t *testing.T) {
	d, _ := newLineageHandler(t)
	batch := make([]types.DiskMessage, 2500)
	var bytes uint64
	digest := lineageSeed(0)
	for i := range batch {
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), LeaderEpoch: int64(i), Payload: "small"}
		payload, err := util.SerializeDiskMessage(batch[i])
		if err != nil {
			t.Fatal(err)
		}
		bytes += uint64(len(payload) + 4)
		digest, err = lineageHash(digest, batch[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := d.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	for _, want := range []uint64{1024, 2048, 2500} {
		ctx, cancel := context.WithCancel(context.Background())
		d.lineageSyncFn = func(f *os.File) error { err := f.Sync(); cancel(); return err }
		_, err := d.MatchLineageCandidatesContext(ctx, nil, 2500)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
		if d.lineageProgress == nil || d.lineageProgress.cursor.End != want {
			t.Fatalf("lost progress, want %d", want)
		}
		if d.lineage != nil {
			t.Fatal("incomplete request published trusted view")
		}
		cancel()
	}
	d.lineageSyncFn = nil
	p := d.lineageProgress
	if p.recordsHashed != 2500 || p.bytesRead != bytes {
		t.Fatalf("nonlinear reconstruction work: records=%d bytes=%d want=%d", p.recordsHashed, p.bytesRead, bytes)
	}
	want := LineagePrefix{StartOffset: 0, EndOffset: 2500, Digest: hex.EncodeToString(digest[:])}
	if ok, err := d.VerifyLineagePrefix(want, 2500); err != nil || !ok {
		t.Fatalf("canonical differential %v %v", ok, err)
	}
	if p.recordsHashed != 2500 || p.bytesRead != bytes {
		t.Fatal("unchanged tail rescanned history")
	}
	beforeJournal := p.journalBytes
	if _, err := d.LineageCandidates(2500); err != nil {
		t.Fatal(err)
	}
	read, hashed := p.bytesRead, p.recordsHashed
	if _, err := d.LineageCandidates(2500); err != nil {
		t.Fatal(err)
	}
	if p.bytesRead != read || p.recordsHashed != hashed || p.journalBytes != beforeJournal {
		t.Fatal("unchanged candidate set repeated historical/journal work")
	}
	appendLineageRecords(t, d, "suffix")
	before := p.recordsHashed
	if _, err := d.MatchLineageCandidatesContext(context.Background(), nil, 2501); err != nil {
		t.Fatal(err)
	}
	if p.recordsHashed-before != 1 {
		t.Fatal("append hashed more than suffix")
	}
	if p.chunks != 4 {
		t.Fatalf("chunks=%d", p.chunks)
	}
}

func TestLineageByteCheckpointsAndBounds(t *testing.T) {
	d, _ := newLineageHandler(t)
	batch := make([]types.DiskMessage, 7)
	for i := range batch {
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: strings.Repeat("x", 600000)}
	}
	if err := d.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	if _, err := d.MatchLineageCandidatesContext(context.Background(), nil, 7); err != nil {
		t.Fatal(err)
	}
	p := d.lineageProgress
	if len(p.index.checkpoints) < 4 {
		t.Fatalf("byte checkpoints=%d", len(p.index.checkpoints))
	}
	before := p.bytesRead
	if _, err := d.lineagePrefix(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	if p.bytesRead-before > lineageByteLimit+4 {
		t.Fatalf("historical scan read %d bytes", p.bytesRead-before)
	}
}

func TestLineageSidecarSyncDoesNotDelayDataNotification(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	cfg.DiskFlushIntervalMS = 10
	d, err := NewDiskHandler(cfg, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }); _ = d.Close() })
	_ = lineageTail(t, d, 0)
	d.lineageMu.Lock()
	d.lineageSyncFn = func(f *os.File) error {
		select {
		case <-blocked:
		default:
			close(blocked)
		}
		<-release
		return f.Sync()
	}
	d.lineageMu.Unlock()
	notified := make(chan uint64, 64)
	d.SetOnSync(func(offset uint64) {
		select {
		case notified <- offset:
		default:
		}
	})
	if err := d.WriteBatch([]types.DiskMessage{{Topic: "orders", Offset: 0, Payload: "first"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("sidecar did not start")
	}
	written := make(chan error, 1)
	go func() {
		written <- d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 1, Payload: "second"}})
	}()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("sidecar fsync held data storage locks")
	}
	deadline := time.After(time.Second)
	for {
		select {
		case offset := <-notified:
			if offset >= 2 {
				once.Do(func() { close(release) })
				return
			}
		case <-deadline:
			t.Fatal("sidecar fsync delayed authoritative sync notification")
		}
	}
}

func TestLineageMutationDuringJournalRejectsPublication(t *testing.T) {
	d, _ := newLineageHandler(t)
	appendLineageRecords(t, d, "old")
	d.lineageSyncFn = func(f *os.File) error {
		d.mu.Lock()
		d.advanceContentGenerationLocked()
		d.mu.Unlock()
		return f.Sync()
	}
	if _, err := d.LineageCandidates(1); !errors.Is(err, ErrStaleContentGeneration) {
		t.Fatalf("stale publication: %v", err)
	}
	if d.lineage != nil || d.lineageProgress != nil {
		t.Fatal("stale generation escaped")
	}
	d.lineageSyncFn = nil
	_ = lineageTail(t, d, 1)
}

func TestLineageShutdownWaitsOutsideStorageAndRejectsLatePublication(t *testing.T) {
	d, _ := newLineageHandler(t)
	appendLineageRecords(t, d, "record")
	entered, release := make(chan struct{}), make(chan struct{})
	// Fatal paths must unblock the builder before fixture cleanup joins Close.
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	d.lineageSyncFn = func(f *os.File) error { close(entered); <-release; return f.Sync() }
	proof := make(chan error, 1)
	go func() { _, err := d.LineageCandidates(1); proof <- err }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case <-d.done:
	case <-time.After(time.Second):
		t.Fatal("close did not stop admission")
	}
	// done closes while Close still holds mu briefly. Wait for its release;
	// a single TryLock can mistake ordinary contention for a shutdown deadlock.
	available := make(chan struct{})
	go func() {
		d.mu.Lock()
		d.mu.Unlock()
		close(available)
	}()
	select {
	case <-available:
	case <-time.After(time.Second):
		t.Fatal("close waits for builder while holding storage mutex")
	}
	close(release)
	if err := <-proof; err == nil {
		t.Fatal("late proof published after shutdown")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close leaked builder")
	}
	if d.lineage != nil {
		t.Fatal("late published view")
	}
}

func TestLineageMixedFrameCanceledChunksMeasurePhysicalWork(t *testing.T) {
	d, _ := newLineageHandler(t)
	batch := make([]types.DiskMessage, 1200)
	var physicalBytes uint64
	digest := lineageSeed(0)
	for i := range batch {
		payload := "small"
		if i%100 == 0 {
			payload = strings.Repeat("x", 600000)
		}
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: payload, LeaderEpoch: int64(i % 3)}
		encoded, err := util.SerializeDiskMessage(batch[i])
		if err != nil {
			t.Fatal(err)
		}
		physicalBytes += uint64(len(encoded) + 4)
		digest, err = lineageHash(digest, batch[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := d.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	for calls := 0; ; calls++ {
		if calls > 20 {
			t.Fatal("mixed-frame reconstruction failed to progress")
		}
		ctx, cancel := context.WithCancel(context.Background())
		d.lineageSyncFn = func(f *os.File) error { err := f.Sync(); cancel(); return err }
		_, err := d.MatchLineageCandidatesContext(ctx, nil, 1200)
		cancel()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
		if d.lineageProgress.complete {
			break
		}
	}
	d.lineageSyncFn = nil
	p := d.lineageProgress
	// Each omitted large frame costs exactly one four-byte lookahead header;
	// its payload is read only in the next chunk, never replayed from segment base.
	if want := physicalBytes + 4*(p.chunks-1); p.bytesRead != want {
		t.Fatalf("physical read bytes=%d want=%d chunks=%d", p.bytesRead, want, p.chunks)
	}
	if p.recordsHashed != 1200 {
		t.Fatalf("canonical work=%d", p.recordsHashed)
	}
	want := LineagePrefix{StartOffset: 0, EndOffset: 1200, Digest: hex.EncodeToString(digest[:])}
	if ok, err := d.VerifyLineagePrefix(want, 1200); err != nil || !ok {
		t.Fatalf("mixed canonical proof: %v %v", ok, err)
	}
	info, err := os.Stat(d.BaseName + ".lineage")
	if err != nil {
		t.Fatal(err)
	}
	// The initial placeholder and subsequent fixed-tail overwrites are real
	// writes even though only one tail slot contributes to final file size.
	if want := uint64(info.Size()) + p.chunks*uint64(len(lineageRecordBytes(lineageJournalRecord{}))); p.journalBytes != want {
		t.Fatalf("journal work=%d want=%d", p.journalBytes, want)
	}
}

type observedLineageContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *observedLineageContext) Err() error {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Err()
}

func TestLineageTwoProofsAndWorkerUseOneBuilder(t *testing.T) {
	d, _ := newLineageHandler(t)
	_ = lineageTail(t, d, 0)
	batch := make([]types.DiskMessage, 2000)
	for i := range batch {
		batch[i] = types.DiskMessage{Topic: "orders", Offset: uint64(i), Payload: "record"}
	}
	if err := d.WriteBatchSync(batch); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var active, maxActive atomic.Int32
	d.lineageSyncFn = func(f *os.File) error {
		n := active.Add(1)
		if n > maxActive.Load() {
			maxActive.Store(n)
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		err := f.Sync()
		active.Add(-1)
		return err
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := d.MatchLineageCandidatesContext(context.Background(), nil, 2000); first <- err }()
	<-entered
	observed := &observedLineageContext{Context: context.Background(), entered: make(chan struct{})}
	go func() { _, err := d.MatchLineageCandidatesContext(observed, nil, 2000); second <- err }()
	<-observed.entered // second proof is now attempting the same gate
	d.requestLineageUpdate()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for len(d.lineageRequests) != 0 {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("worker did not consume coalesced request")
		}
	}
	for i := 0; i < 10; i++ {
		d.requestLineageUpdate()
	}
	if len(d.lineageRequests) != 1 {
		t.Fatal("background requests did not coalesce")
	}
	select {
	case err := <-second:
		t.Fatalf("second builder escaped gate: %v", err)
	default:
	}
	if !d.mu.TryLock() {
		t.Fatal("queued builders hold storage mutex")
	}
	d.mu.Unlock()
	once.Do(func() { close(release) })
	for _, result := range []<-chan error{first, second} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("serialized proof stalled")
		}
	}
	d.lineageMu.Lock()
	p := d.lineageProgress
	if maxActive.Load() != 1 || p.recordsHashed != 2000 || p.chunks != 3 {
		t.Errorf("duplicated builder work: concurrent=%d records=%d chunks=%d", maxActive.Load(), p.recordsHashed, p.chunks)
	}
	d.lineageSyncFn = nil
	d.lineageMu.Unlock()
}
