package sdk

import (
	"testing"
	"time"
)

func TestNewBackoff_InitialState(t *testing.T) {
	b := newBackoff(100*time.Millisecond, 5*time.Second)
	if b == nil {
		t.Fatal("expected non-nil backoff")
	}
	if b.min != 100*time.Millisecond {
		t.Errorf("expected min=100ms, got %v", b.min)
	}
	if b.max != 5*time.Second {
		t.Errorf("expected max=5s, got %v", b.max)
	}
	if b.current != 100*time.Millisecond {
		t.Errorf("expected current=100ms, got %v", b.current)
	}
	if b.factor != 2.0 {
		t.Errorf("expected factor=2.0, got %v", b.factor)
	}
}

func TestNewBackoff_ZeroMinClamped(t *testing.T) {
	b := newBackoff(0, 1*time.Second)
	if b.min != time.Millisecond {
		t.Errorf("expected min clamped to 1ms, got %v", b.min)
	}
}

func TestNewBackoff_MaxLessThanMinClamped(t *testing.T) {
	b := newBackoff(500*time.Millisecond, 100*time.Millisecond)
	if b.max != 500*time.Millisecond {
		t.Errorf("expected max clamped to min (500ms), got %v", b.max)
	}
}

func TestDuration_WithinExpectedRange(t *testing.T) {
	minDur := 100 * time.Millisecond
	maxDur := 10 * time.Second
	b := newBackoff(minDur, maxDur)

	d := b.duration()
	// First call: base is 100ms, jitter up to 10ms, so range is [100ms, 110ms)
	if d < minDur || d >= minDur+minDur/10+1 {
		t.Errorf("first duration %v outside expected range [%v, %v)", d, minDur, minDur+minDur/10+1)
	}
}

func TestDuration_Increases(t *testing.T) {
	b := newBackoff(10*time.Millisecond, 10*time.Second)

	d1 := b.duration()
	d2 := b.duration()
	d3 := b.duration()

	// Due to exponential factor of 2, each base doubles.
	// d1 base=10ms, d2 base=20ms, d3 base=40ms
	// Even with jitter, d2 should be > d1 and d3 > d2 when bases differ enough.
	if d2 <= d1 {
		t.Errorf("expected d2 (%v) > d1 (%v)", d2, d1)
	}
	if d3 <= d2 {
		t.Errorf("expected d3 (%v) > d2 (%v)", d3, d2)
	}
}

func TestDuration_CapsAtMax(t *testing.T) {
	maxDur := 200 * time.Millisecond
	b := newBackoff(100*time.Millisecond, maxDur)

	// Call enough times to exceed max
	for i := 0; i < 20; i++ {
		d := b.duration()
		// Max base is 200ms, jitter up to 20ms => max possible duration is 220ms - 1ns
		upperBound := maxDur + maxDur/10
		if d > upperBound {
			t.Errorf("duration %v exceeded cap upper bound %v", d, upperBound)
		}
	}
}

func TestReset(t *testing.T) {
	b := newBackoff(10*time.Millisecond, 10*time.Second)

	// Advance several times
	for i := 0; i < 5; i++ {
		b.duration()
	}

	b.reset()

	if b.current != b.min {
		t.Errorf("after reset, expected current=%v, got %v", b.min, b.current)
	}

	// After reset, first duration should be back in the min range
	d := b.duration()
	minDur := 10 * time.Millisecond
	upperBound := minDur + minDur/10 + 1
	if d < minDur || d >= upperBound {
		t.Errorf("post-reset duration %v outside expected range [%v, %v)", d, minDur, upperBound)
	}
}
