package sdk

import (
	"math/rand"
	"time"
)

type backoff struct {
	current time.Duration
	min     time.Duration
	max     time.Duration
	factor  float64
}

func newBackoff(min, max time.Duration) *backoff {
	if min <= 0 {
		min = time.Millisecond
	}
	if max < min {
		max = min
	}
	return &backoff{current: min, min: min, max: max, factor: 2.0}
}

// duration returns the next backoff duration with 10% jitter, then advances the internal state.
func (b *backoff) duration() time.Duration {
	if b.current <= 0 {
		b.current = time.Millisecond
	}

	jitterRange := int64(b.current) / 10
	var jitter time.Duration
	if jitterRange > 0 {
		jitter = time.Duration(rand.Int63n(jitterRange))
	}

	d := b.current + jitter
	b.current = time.Duration(float64(b.current) * b.factor)
	if b.current > b.max {
		b.current = b.max
	}
	return d
}

func (b *backoff) reset() {
	b.current = b.min
}
