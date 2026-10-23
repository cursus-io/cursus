package disk

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cursus-io/cursus/pkg/types"
)

func rawTestStart(t *testing.T, d *DiskHandler) rawFrameCursor {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	return rawFrameCursor{Generation: d.contentGenerationLocked(), Anchor: d.segments[0], End: d.segments[0], Segment: d.segments[0]}
}

func rawTestCapture(t *testing.T, d *DiskHandler, cursor rawFrameCursor, budget rawFrameBudget) rawFrameBatch {
	t.Helper()
	unlock, err := d.lockLineageStorage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	return d.captureRawFramesLocked(context.Background(), cursor, budget)
}

func rawTestValidate(d *DiskHandler, cursor rawFrameCursor, b rawFrameBatch, mode rawFrameMode, end uint64) (rawFrameCursor, bool, int, error) {
	count := 0
	for _, frame := range b.Frames {
		next, _, included, done, err := d.validateRawFrame(cursor, frame, mode, end, b.Tail)
		if err != nil {
			return cursor, false, count, err
		}
		cursor = next
		if included {
			count++
		}
		if done {
			return cursor, true, count, nil
		}
	}
	return cursor, false, count, b.Err
}

func TestRawFrameBudgetsAndOversizedFirstRecord(t *testing.T) {
	d, _ := newLineageHandler(t)
	if err := d.WriteBatchSync([]types.DiskMessage{{Topic: "orders", Offset: 0, Payload: strings.Repeat("x", 2*lineageByteLimit)}, {Topic: "orders", Offset: 1, Payload: "small"}}); err != nil {
		t.Fatal(err)
	}
	cursor := rawTestStart(t, d)
	for _, budget := range []rawFrameBudget{{0, 1, 1}, {1, 0, 1}, {1, 1, 0}} {
		if b := rawTestCapture(t, d, cursor, budget); b.Err == nil {
			t.Fatal("zero budget accepted")
		}
	}
	b := rawTestCapture(t, d, cursor, rawFrameBudget{1024, 100, 2})
	if b.Err != nil || len(b.Frames) != 1 || b.BytesRead > MaxMessageSize+4 {
		t.Fatalf("oversize allowance: frames=%d bytes=%d err=%v", len(b.Frames), b.BytesRead, b.Err)
	}
	next, done, count, err := rawTestValidate(d, cursor, b, rawFramesContiguous, 2)
	if err != nil || done || count != 1 || next.End != 1 {
		t.Fatalf("first chunk: %+v %v %d %v", next, done, count, err)
	}
	b = rawTestCapture(t, d, next, rawFrameBudget{1024, 100, 2})
	next, done, count, err = rawTestValidate(d, next, b, rawFramesContiguous, 2)
	if err != nil || !done || count != 1 || next.End != 2 {
		t.Fatalf("resumed chunk: %+v %v %d %v", next, done, count, err)
	}
}

func TestRawFrameHeaderLookaheadChargedAndCursorPreserved(t *testing.T) {
	d, _ := newLineageHandler(t)
	appendLineageRecords(t, d, "same", "same")
	cursor := rawTestStart(t, d)
	all := rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10})
	firstSize := uint64(len(all.Frames[0].Payload) + 4)
	b := rawTestCapture(t, d, cursor, rawFrameBudget{firstSize + 4, 10, 10})
	if len(b.Frames) != 1 || b.BytesRead != firstSize+4 {
		t.Fatalf("lookahead bytes %d frames %d", b.BytesRead, len(b.Frames))
	}
	next, _, _, err := rawTestValidate(d, cursor, b, rawFramesContiguous, 2)
	if err != nil {
		t.Fatal(err)
	}
	if next.Position != int64(firstSize) || next.End != 1 {
		t.Fatal("omitted payload advanced cursor")
	}
	if b := rawTestCapture(t, d, next, rawFrameBudget{1 << 20, 10, 10}); len(b.Frames) != 2 {
		t.Fatal("omitted frame lost on resume")
	}
}

func TestRawFrameRotationOldEOFAndGeneration(t *testing.T) {
	d, _ := newLineageHandler(t)
	appendLineageRecords(t, d, "zero")
	cursor := rawTestStart(t, d)
	b := rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10})
	cursor, done, _, err := rawTestValidate(d, cursor, b, rawFramesContiguous, 1)
	if err != nil || !done {
		t.Fatal(err)
	}
	appendLineageRecords(t, d, "one")
	if err := d.RollSegmentAt(2); err != nil {
		t.Fatal(err)
	}
	appendLineageRecords(t, d, "two")
	b = rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10})
	next, done, count, err := rawTestValidate(d, cursor, b, rawFramesContiguous, 3)
	if err != nil || !done || count != 2 || next.End != 3 {
		t.Fatalf("append/rotation resume %v %d %v", done, count, err)
	}
	missing := cursor
	missing.Segment = 1
	if b := rawTestCapture(t, d, missing, rawFrameBudget{1 << 20, 10, 10}); !errors.Is(b.Err, ErrStaleContentGeneration) {
		t.Fatal("missing exact segment skipped")
	}
	other, _ := newLineageHandler(t)
	if b := rawTestCapture(t, other, cursor, rawFrameBudget{1 << 20, 10, 10}); !errors.Is(b.Err, ErrStaleContentGeneration) {
		t.Fatal("foreign owner accepted")
	}
	d.mu.Lock()
	if err := d.advanceContentGenerationLocked(); err != nil {
		t.Fatal(err)
	}
	d.mu.Unlock()
	if b := rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10}); !errors.Is(b.Err, ErrStaleContentGeneration) {
		t.Fatal("mutated generation accepted")
	}
}

func TestRawFrameMarkedEmptySegmentTransitionBudget(t *testing.T) {
	d, _ := newLineageHandler(t)
	d.mu.Lock()
	d.compactionMu.Lock()
	for i := uint64(0); i < 7; i++ {
		if i > 0 {
			if err := os.WriteFile(d.GetSegmentPath(i), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			d.segments = append(d.segments, i)
		}
		d.compactedSegments[i] = 0
	}
	atomic.StoreUint64(&d.FlushedOffset, 7)
	d.compactionMu.Unlock()
	d.mu.Unlock()
	cursor := rawTestStart(t, d)
	first := rawTestCapture(t, d, cursor, rawFrameBudget{1024, 10, 2})
	if _, _, _, err := rawTestValidate(d, cursor, first, rawFramesContiguous, 7); !errors.Is(err, ErrLineageUnsupported) {
		t.Fatal("contiguous mode accepted marked holes")
	}
	var transitions uint64
	for i := 0; i < 4; i++ {
		b := rawTestCapture(t, d, cursor, rawFrameBudget{1024, 10, 2})
		if b.Transitions > 2 || len(b.Frames) > 2 || b.BytesRead != 0 {
			t.Fatal("empty transitions unbounded")
		}
		transitions += b.Transitions
		next, done, count, err := rawTestValidate(d, cursor, b, rawFramesMarkedGaps, 7)
		if err != nil || count != 0 {
			t.Fatal(err)
		}
		cursor = next
		if done {
			if transitions != 7 {
				t.Fatalf("transition work %d", transitions)
			}
			return
		}
	}
	t.Fatal("empty segment scan did not finish")
}

func TestRawFrameFixedPrefixAndCorruptEOF(t *testing.T) {
	for _, tail := range []int{1, 2} {
		t.Run(string(rune('0'+tail)), func(t *testing.T) {
			d, _ := newLineageHandler(t)
			appendLineageRecords(t, d, "zero")
			if tail == 2 {
				appendLineageRecords(t, d, "one")
			}
			d.ioMu.Lock()
			_, err := d.file.Write([]byte{0, 0})
			d.ioMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			cursor := rawTestStart(t, d)
			b := rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10})
			if b.Err == nil {
				t.Fatal("partial header invisible")
			}
			_, done, _, err := rawTestValidate(d, cursor, b, rawFramesContiguous, 1)
			if tail == 1 && err == nil {
				t.Fatal("durable tail accepted corrupt EOF")
			}
			if tail == 2 && (err != nil || !done) {
				t.Fatalf("fixed prefix inspected unrelated future: %v", err)
			}
		})
	}
}

func TestRawFrameMarkedGapAndUnmarkedGap(t *testing.T) {
	d, _ := newLineageHandler(t)
	if err := d.WriteCompactedReplicaRange(0, 5, []types.DiskMessage{{Topic: "orders", Offset: 2, Payload: "two"}, {Topic: "orders", Offset: 4, Payload: "four"}}); err != nil {
		t.Fatal(err)
	}
	cursor := rawTestStart(t, d)
	b := rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10})
	next, done, count, err := rawTestValidate(d, cursor, b, rawFramesMarkedGaps, 3)
	if err != nil || !done || count != 1 || next.End != 3 {
		t.Fatalf("marked gap %v %d %v", done, count, err)
	}
	b = rawTestCapture(t, d, next, rawFrameBudget{1 << 20, 10, 10})
	_, done, count, err = rawTestValidate(d, next, b, rawFramesMarkedGaps, 5)
	if err != nil || !done || count != 1 {
		t.Fatalf("gap resume %v %d %v", done, count, err)
	}
	d.compactionMu.Lock()
	clear(d.compactedSegments)
	d.compactionMu.Unlock()
	b = rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10})
	if _, _, _, err := rawTestValidate(d, cursor, b, rawFramesMarkedGaps, 5); err == nil {
		t.Fatal("unmarked hole accepted")
	}
}

func TestRawFrameRejectsMalformedIdentityOrderingAndSize(t *testing.T) {
	for _, test := range []struct {
		name    string
		records []types.DiskMessage
		header  uint32
		split   bool
	}{
		{name: "wrong-partition", records: []types.DiskMessage{{Topic: "orders", Partition: 1, Offset: 0}}},
		{name: "duplicate", records: []types.DiskMessage{{Topic: "orders", Offset: 0}, {Topic: "orders", Offset: 0}}},
		{name: "descending", records: []types.DiskMessage{{Topic: "orders", Offset: 1}, {Topic: "orders", Offset: 0}}},
		{name: "outside-segment", records: []types.DiskMessage{{Topic: "orders", Offset: 0}, {Topic: "orders", Offset: 1}}, split: true},
		{name: "oversize-header", header: MaxMessageSize + 1},
		{name: "partial-payload", header: 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, _ := newLineageHandler(t)
			if len(test.records) > 0 {
				if err := d.WriteBatchSync(test.records); err != nil {
					t.Fatal(err)
				}
			}
			if test.split {
				d.mu.Lock()
				d.segments = append(d.segments, 1)
				d.mu.Unlock()
				if err := os.WriteFile(d.GetSegmentPath(1), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if test.header > 0 {
				var header [4]byte
				binary.BigEndian.PutUint32(header[:], test.header)
				d.ioMu.Lock()
				_, err := d.file.Write(header[:])
				d.ioMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			cursor := rawTestStart(t, d)
			b := rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10})
			if _, _, _, err := rawTestValidate(d, cursor, b, rawFramesMarkedGaps, b.Tail); err == nil {
				t.Fatal("malformed physical history accepted")
			}
		})
	}
}

func TestRawFrameRejectsInvalidLocalCursorAndMode(t *testing.T) {
	d, _ := newLineageHandler(t)
	appendLineageRecords(t, d, "record")
	cursor := rawTestStart(t, d)
	for _, invalid := range []rawFrameCursor{
		{Generation: cursor.Generation, Anchor: 0, End: 2, Segment: 0},
		{Generation: cursor.Generation, Anchor: 1, End: 0, Segment: 0},
		{Generation: cursor.Generation, Anchor: 0, End: 0, Segment: 0, Position: -1},
	} {
		if b := rawTestCapture(t, d, invalid, rawFrameBudget{1 << 20, 10, 10}); b.Err == nil {
			t.Fatal("invalid local cursor accepted")
		}
	}
	b := rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10})
	if _, _, _, err := rawTestValidate(d, cursor, b, rawFrameMode(2), 1); err == nil {
		t.Fatal("unknown scan mode accepted")
	}
	if _, _, _, err := rawTestValidate(d, cursor, b, rawFramesContiguous, 2); err == nil {
		t.Fatal("endpoint beyond snapshot tail accepted")
	}
	invalid := cursor
	invalid.Position = 1
	if _, _, _, err := rawTestValidate(d, invalid, b, rawFramesContiguous, 1); err == nil {
		t.Fatal("mismatched physical continuation accepted")
	}
}

func TestRawFrameTruncateReappendABAInvalidatesCursor(t *testing.T) {
	d, _ := newLineageHandler(t)
	appendLineageRecords(t, d, "zero", "one", "two")
	proof := lineageTail(t, d, 3)
	oldProgress := d.lineageProgress
	cursor := rawTestStart(t, d)
	b := rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10})
	cursor, done, _, err := rawTestValidate(d, cursor, b, rawFramesContiguous, 2)
	if err != nil || !done {
		t.Fatal(err)
	}
	if err := d.TruncateTo(1); err != nil {
		t.Fatal(err)
	}
	appendLineageRecords(t, d, "one", "two")
	if d.GetFlushedOffset() != 3 {
		t.Fatal("ABA fixture changed logical tail")
	}
	if b := rawTestCapture(t, d, cursor, rawFrameBudget{1 << 20, 10, 10}); !errors.Is(b.Err, ErrStaleContentGeneration) {
		t.Fatal("truncate/reappend reused physical cursor")
	}
	if got := lineageTail(t, d, 3); got != proof {
		t.Fatal("identical reappend changed canonical evidence")
	}
	if d.lineageProgress == oldProgress {
		t.Fatal("ABA retained prior generation's builder progress")
	}
}
