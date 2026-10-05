package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/cursus-io/cursus/pkg/metrics"
)

type requestMemoryBudget struct {
	mu          sync.Mutex
	maxRequests int
	maxBytes    uint64
	requests    int
	bytes       uint64
	changed     chan struct{}
}

func newRequestMemoryBudget(maxRequests int, maxBytes int64) *requestMemoryBudget {
	if maxRequests <= 0 {
		maxRequests = 1
	}
	if maxBytes <= 0 {
		maxBytes = 1
	}
	return &requestMemoryBudget{
		maxRequests: maxRequests,
		maxBytes:    uint64(maxBytes),
		changed:     make(chan struct{}),
	}
}

func (b *requestMemoryBudget) reserve(ctx context.Context, bytes uint64) (func(), error) {
	if b == nil {
		return func() {}, nil
	}
	if bytes > b.maxBytes {
		metrics.RequestAdmissionRejections.WithLabelValues("bytes").Inc()
		return nil, fmt.Errorf("request memory requirement %d exceeds broker limit %d", bytes, b.maxBytes)
	}
	for {
		b.mu.Lock()
		if b.requests < b.maxRequests && bytes <= b.maxBytes-b.bytes {
			b.requests++
			b.bytes += bytes
			metrics.RequestsInflight.Set(float64(b.requests))
			metrics.RequestBytesInflight.Set(float64(b.bytes))
			b.mu.Unlock()

			var once sync.Once
			return func() {
				once.Do(func() { b.release(bytes) })
			}, nil
		}
		changed := b.changed
		b.mu.Unlock()

		metrics.RequestAdmissionWaiters.Inc()
		select {
		case <-changed:
			metrics.RequestAdmissionWaiters.Dec()
		case <-ctx.Done():
			metrics.RequestAdmissionWaiters.Dec()
			metrics.RequestAdmissionRejections.WithLabelValues("context").Inc()
			return nil, ctx.Err()
		}
	}
}

func (b *requestMemoryBudget) release(bytes uint64) {
	b.mu.Lock()
	if b.requests > 0 {
		b.requests--
	}
	if bytes >= b.bytes {
		b.bytes = 0
	} else {
		b.bytes -= bytes
	}
	metrics.RequestsInflight.Set(float64(b.requests))
	metrics.RequestBytesInflight.Set(float64(b.bytes))
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}
