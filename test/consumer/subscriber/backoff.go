package subscriber

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

func (b *backoff) duration() time.Duration {
	if b.current <= 0 {
		b.current = time.Millisecond
	}

	// #nosec G404 -- retry jitter is not security-sensitive randomness.
	jitter := time.Duration(rand.Int63n(int64(b.current) / 10))
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
