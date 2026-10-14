package disk

import (
	"math"
	"testing"
)

func assertLineageRollbackBound(t *testing.T, start, tail, required uint64) {
	t.Helper()
	ends := lineageCandidateEnds(start, tail)
	if len(ends) == 0 || len(ends) > 64 || ends[0] != tail || ends[len(ends)-1] != start {
		t.Fatalf("invalid candidates [%d,%d]: %v", start, tail, ends)
	}
	safeEnd := tail - required // min(longest common prefix, authoritative source HWM)
	var rollback uint64
	found := false
	for i, end := range ends {
		if end < start || end > tail || (i > 0 && end >= ends[i-1]) {
			t.Fatalf("unordered/out-of-range candidates: %v", ends)
		}
		if !found && end <= safeEnd {
			rollback, found = tail-end, true
		}
	}
	if !found || rollback < required {
		t.Fatalf("unsafe rollback: start=%d tail=%d D=%d R=%d", start, tail, required, rollback)
	}
	if required == 0 {
		if rollback != 0 {
			t.Fatalf("matching tail discarded %d records", rollback)
		}
		return
	}
	if required == 1 && rollback != 1 {
		t.Fatalf("one bad tail record: start=%d tail=%d R=%d", start, tail, rollback)
	}
	limit := uint64(4)
	if tail-start <= math.MaxInt64 {
		limit = 2
	}
	// Division avoids overflow when checking R < limit*D over all uint64 spans.
	if rollback/required >= limit {
		t.Fatalf("amplified rollback: start=%d tail=%d D=%d R=%d limit=%d", start, tail, required, rollback, limit)
	}
}

func TestLineageCandidatesExhaustiveRollbackBound(t *testing.T) {
	for span := uint64(0); span <= 256; span++ {
		for _, start := range []uint64{0, 37, math.MaxUint64 - span} {
			for required := uint64(0); required <= span; required++ {
				assertLineageRollbackBound(t, start, start+span, required)
			}
		}
	}
}

func TestLineageCandidatesUint64RollbackBound(t *testing.T) {
	for _, bounds := range [][2]uint64{{0, 999999}, {42, 1000041}, {0, math.MaxInt64}, {0, math.MaxUint64}, {55, math.MaxUint64}, {math.MaxUint64 - 1024, math.MaxUint64}} {
		span := bounds[1] - bounds[0]
		for _, required := range []uint64{0, 1, span - 1, span} {
			assertLineageRollbackBound(t, bounds[0], bounds[1], required)
		}
		for bit := uint(1); bit < 64; bit++ {
			power := uint64(1) << bit
			for _, required := range []uint64{power - 1, power, power + 1} {
				if required <= span {
					assertLineageRollbackBound(t, bounds[0], bounds[1], required)
				}
			}
		}
	}
}
