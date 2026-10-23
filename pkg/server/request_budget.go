package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/cursus-io/cursus/pkg/metrics"
	"github.com/cursus-io/cursus/pkg/wire"
)

type requestMemoryBudget struct {
	mu          sync.Mutex
	maxRequests int
	maxBytes    uint64
	requests    int
	bytes       uint64
	changed     chan struct{}
	recovery    *requestMemoryBudget
	unmetered   bool
}

func newBrokerRequestBudgets(maxRequests int, maxBytes int64, reserveInternal bool) (*requestMemoryBudget, *requestMemoryBudget) {
	if !reserveInternal {
		return newRequestMemoryBudget(maxRequests, maxBytes), nil
	}
	clientRequests := maxRequests / 2
	internalRequests := maxRequests - clientRequests
	clientBytes := maxBytes / 2
	internalBytes := maxBytes - clientBytes
	internal := newRequestMemoryBudget(internalRequests, internalBytes)
	// Recovery on the broker listener is snapshot catalog traffic. Reserve
	// the remaining internal slots/bytes for ISR replication and control.
	if internalRequests >= 2 {
		internal.recovery = newRequestMemoryBudget(max(1, internalRequests/4), max(1, internalBytes/4))
		internal.recovery.unmetered = true
	}
	return newRequestMemoryBudget(clientRequests, clientBytes), internal
}

func (b *requestMemoryBudget) reserveCommand(ctx context.Context, command wire.Command, bytes uint64) (func(), error) {
	if b == nil || b.recovery == nil || (command != wire.CommandListSnapshots && command != wire.CommandFetchSnapshot && command != wire.CommandCatchupSnapshots) {
		return b.reserve(ctx, bytes)
	}
	// Wait on the subset first: a waiting recovery request cannot occupy the
	// shared reservation needed by normal replication/control.
	releaseRecovery, err := b.recovery.reserve(ctx, bytes)
	if err != nil {
		return nil, err
	}
	release, err := b.reserve(ctx, bytes)
	if err != nil {
		releaseRecovery()
		return nil, err
	}
	return func() { release(); releaseRecovery() }, nil
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
			if !b.unmetered {
				metrics.RequestsInflight.Inc()
				metrics.RequestBytesInflight.Add(float64(bytes))
			}
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
	if !b.unmetered {
		metrics.RequestsInflight.Dec()
		metrics.RequestBytesInflight.Sub(float64(bytes))
	}
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}
