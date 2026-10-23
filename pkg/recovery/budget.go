// Package recovery provides bounded admission for background recovery work.
// Callers acquire before taking storage/FSM locks and release after owned work
// has actually stopped. Healthy replication and control use separate capacity.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var ErrBusy = errors.New("recovery admission busy")

// Limits apply to one broker process. Bytes are charged work bytes, not record
// counts. Source entries retain debt until repaid, even when no lease is active.
type Limits struct {
	BytesPerSecond, BurstBytes             int64
	SourceBytesPerSecond, SourceBurstBytes int64
	MaxUnitBytes                           int64
	Concurrency, SourceConcurrency         int
	MaxSources, MaxWaiters                 int
}

func (l Limits) Validate() error {
	if l.BytesPerSecond <= 0 || l.BytesPerSecond > 1<<40 || l.SourceBytesPerSecond <= 0 || l.SourceBytesPerSecond > l.BytesPerSecond ||
		l.BurstBytes <= 0 || l.BurstBytes > 1<<30 || l.SourceBurstBytes <= 0 || l.SourceBurstBytes > l.BurstBytes ||
		l.MaxUnitBytes < l.BurstBytes || l.MaxUnitBytes > 1<<30 ||
		l.Concurrency < 1 || l.Concurrency > 32 || l.SourceConcurrency < 1 || l.SourceConcurrency > l.Concurrency ||
		l.MaxSources < 1 || l.MaxSources > 4096 || l.MaxWaiters < 1 || l.MaxWaiters > 4096 {
		return fmt.Errorf("invalid recovery limits")
	}
	return nil
}

// Clock allows quota/refill tests to advance time without wall-clock sleeps.
type Clock interface {
	Now() time.Time
	Wait(context.Context, time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) Wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type bucket struct {
	tokens float64
	last   time.Time
	active int
}

func (b *bucket) refill(now time.Time, rate, burst int64) {
	if now.After(b.last) {
		b.tokens = math.Min(float64(burst), b.tokens+now.Sub(b.last).Seconds()*float64(rate))
		b.last = now
	}
}

func (b *bucket) delay(n, rate, burst int64) time.Duration {
	// A single oversized unit can start from a full bucket. Charging its full
	// size creates debt, which must be repaid before later work can start.
	need := float64(min(n, burst)) - b.tokens
	if need <= 0 {
		return 0
	}
	return max(time.Nanosecond, time.Duration(math.Ceil(need/float64(rate)*float64(time.Second))))
}

type Budget struct {
	mu          sync.Mutex
	limits      Limits
	clock       Clock
	broker      bucket
	sources     map[string]*bucket
	oversized   bool
	waiters     int
	workSlots   chan struct{}
	workWaiting map[string]int
}

func NewBudget(l Limits, clock Clock) (*Budget, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = realClock{}
	}
	return &Budget{limits: l, clock: clock, broker: bucket{tokens: float64(l.BurstBytes), last: clock.Now()}, sources: make(map[string]*bucket), workSlots: make(chan struct{}, max(1, l.Concurrency/4)), workWaiting: make(map[string]int)}, nil
}

// AcquireWork reserves a subset of broker slots for atomic export, checksum and
// install work. These phases cannot sleep inside storage/FSM gates. Their I/O
// work is measured separately from network bytes; the wire rate is not a disk
// bandwidth guarantee. Transfer leases must be released before entering here.
func (b *Budget) AcquireWork(ctx context.Context, source string) (func(), error) {
	if b == nil {
		return func() {}, nil
	}
	b.mu.Lock()
	if b.waiters >= b.limits.MaxWaiters {
		b.mu.Unlock()
		return nil, ErrBusy
	}
	b.waiters++
	b.mu.Unlock()
	unwait := func() { b.mu.Lock(); b.waiters--; b.mu.Unlock() }
	defer unwait()
	select {
	case b.workSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	b.mu.Lock()
	b.workWaiting[source]++
	b.mu.Unlock()
	// Keep the original waiter admission across both stages. Releasing and
	// reacquiring it lets new fetch waiters steal an already admitted owner's
	// place after it finally wins the scarce work slot.
	l, err := b.waitForLease(ctx, source, 0, true)
	b.mu.Lock()
	b.workWaiting[source]--
	if b.workWaiting[source] == 0 {
		delete(b.workWaiting, source)
	}
	b.mu.Unlock()
	if err != nil {
		<-b.workSlots
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(func() { _ = l.Finish(0); <-b.workSlots }) }, nil
}

// Lease retains concurrency and the reserved charge until Finish. Finish is
// idempotent. A canceled operation must join its I/O before finishing the lease.
type Lease struct {
	budget    *Budget
	source    *bucket
	reserved  int64
	oversized bool
	estimated bool
	once      sync.Once
}

// TryAcquireEstimated admits a response whose first indivisible unit is not
// known until it arrives. The wire/storage reader must enforce MaxUnitBytes.
// At most Concurrency such responses (SourceConcurrency per source) can be in
// flight. Their full charge becomes debt; subsequent turns wait for repayment.
// This is bounded optimistic accounting, not a strict instantaneous byte cap.
func (b *Budget) TryAcquireEstimated(ctx context.Context, source string, quantum int64) (*Lease, time.Duration, error) {
	l, delay, err := b.TryAcquire(ctx, source, quantum)
	if l != nil {
		l.estimated = true
	}
	return l, delay, err
}

// TryAcquire never blocks or spawns a goroutine. retry is a suggested wakeup;
// slot releases can permit earlier progress. Invalid requests return a permanent
// error; quota/slot/cardinality pressure returns ErrBusy.
func (b *Budget) TryAcquire(ctx context.Context, source string, bytes int64) (*Lease, time.Duration, error) {
	return b.tryAcquire(ctx, source, bytes, false)
}

func (b *Budget) tryAcquire(ctx context.Context, source string, bytes int64, work bool) (*Lease, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if source == "" || len(source) > 1024 || bytes < 0 || bytes > b.limits.MaxUnitBytes {
		return nil, 0, fmt.Errorf("invalid recovery reservation")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now, l := b.clock.Now(), b.limits
	b.broker.refill(now, l.BytesPerSecond, l.BurstBytes)
	s := b.sources[source]
	if s == nil {
		// Only fully refilled, idle entries may be evicted: deleting debt would
		// let callers bypass source quotas by repeatedly leaving and returning.
		if len(b.sources) >= l.MaxSources {
			for key, old := range b.sources {
				old.refill(now, l.SourceBytesPerSecond, l.SourceBurstBytes)
				if old.active == 0 && old.tokens >= float64(l.SourceBurstBytes) {
					delete(b.sources, key)
					break
				}
			}
		}
		if len(b.sources) >= l.MaxSources {
			return nil, 10 * time.Millisecond, ErrBusy
		}
		s = &bucket{tokens: float64(l.SourceBurstBytes), last: now}
		b.sources[source] = s
	}
	s.refill(now, l.SourceBytesPerSecond, l.SourceBurstBytes)
	oversized := bytes > l.BurstBytes || bytes > l.SourceBurstBytes
	brokerSlots, sourceSlots := l.Concurrency, l.SourceConcurrency
	if !work {
		// Reserve the next freed slots for an admitted atomic-work waiter;
		// continuously ready fetches must not starve bootstrap publication.
		brokerSlots -= len(b.workWaiting)
		sourceSlots -= b.workWaiting[source]
	}
	if b.broker.active >= brokerSlots || s.active >= sourceSlots || (oversized && b.oversized) {
		return nil, 10 * time.Millisecond, ErrBusy
	}
	delay := max(b.broker.delay(bytes, l.BytesPerSecond, l.BurstBytes), s.delay(bytes, l.SourceBytesPerSecond, l.SourceBurstBytes))
	if delay > 0 {
		return nil, delay, ErrBusy
	}
	b.broker.tokens -= float64(bytes)
	s.tokens -= float64(bytes)
	b.broker.active++
	s.active++
	if oversized {
		b.oversized = true
	}
	return &Lease{budget: b, source: s, reserved: bytes, oversized: oversized}, 0, nil
}

// Acquire is for independently owned bounded transfers, such as bootstrap.
// Schedulers use TryAcquire so quota wait never consumes a worker slot. Neither
// entry point may be called while holding an FSM, partition or storage lock.
func (b *Budget) Acquire(ctx context.Context, source string, bytes int64) (*Lease, error) {
	return b.acquire(ctx, source, bytes, false)
}

// AcquireEstimated is the bounded-waiter counterpart of TryAcquireEstimated.
// Synchronous compatibility callers release it after each response/apply page.
func (b *Budget) AcquireEstimated(ctx context.Context, source string, quantum int64) (*Lease, error) {
	l, err := b.acquire(ctx, source, quantum, false)
	if l != nil {
		l.estimated = true
	}
	return l, err
}

func (b *Budget) acquire(ctx context.Context, source string, bytes int64, work bool) (*Lease, error) {
	b.mu.Lock()
	if b.waiters >= b.limits.MaxWaiters {
		b.mu.Unlock()
		return nil, ErrBusy
	}
	b.waiters++
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.waiters--; b.mu.Unlock() }()
	return b.waitForLease(ctx, source, bytes, work)
}

func (b *Budget) waitForLease(ctx context.Context, source string, bytes int64, work bool) (*Lease, error) {
	for {
		lease, delay, err := b.tryAcquire(ctx, source, bytes, work)
		if !errors.Is(err, ErrBusy) {
			return lease, err
		}
		// Bound cancellation/slot-release observation without allocating one
		// timer or queue entry for every partition in the broker inventory.
		if err := b.clock.Wait(ctx, min(delay, 100*time.Millisecond)); err != nil {
			return nil, err
		}
	}
}

// Finish accounts actual completed/partial work, including failed work. The
// caller must reserve a proven upper bound. An underestimate is still charged
// in full, never forgiven, and reported so the caller can fail closed.
func (l *Lease) Finish(actual int64) (err error) {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		b := l.budget
		b.mu.Lock()
		defer b.mu.Unlock()
		if actual < 0 {
			actual = l.reserved
			err = fmt.Errorf("negative recovery charge")
		}
		if actual > l.reserved && (!l.estimated || actual > b.limits.MaxUnitBytes) {
			err = fmt.Errorf("recovery charge %d exceeds reservation %d", actual, l.reserved)
		}
		now, limits := b.clock.Now(), b.limits
		b.broker.refill(now, limits.BytesPerSecond, limits.BurstBytes)
		l.source.refill(now, limits.SourceBytesPerSecond, limits.SourceBurstBytes)
		refund := float64(l.reserved) - float64(actual)
		b.broker.tokens = math.Min(float64(limits.BurstBytes), b.broker.tokens+refund)
		l.source.tokens = math.Min(float64(limits.SourceBurstBytes), l.source.tokens+refund)
		b.broker.active--
		l.source.active--
		if l.oversized {
			b.oversized = false
		}
	})
	return err
}

type Usage struct {
	Active, Sources, Waiters int
	Tokens                   float64
	Oversized                bool
}

func (b *Budget) Usage() Usage {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.broker.refill(b.clock.Now(), b.limits.BytesPerSecond, b.limits.BurstBytes)
	return Usage{b.broker.active, len(b.sources), b.waiters, b.broker.tokens, b.oversized}
}
