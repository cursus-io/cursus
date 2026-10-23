package recovery

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu   sync.Mutex
	now  time.Time
	wait func(context.Context, time.Duration) error
}

func (f *fakeClock) Now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.now }
func (f *fakeClock) advance(d time.Duration) { f.mu.Lock(); f.now = f.now.Add(d); f.mu.Unlock() }
func (f *fakeClock) Wait(ctx context.Context, d time.Duration) error {
	if f.wait != nil {
		return f.wait(ctx, d)
	}
	f.advance(d)
	return ctx.Err()
}
func fixture(t *testing.T) (*Budget, *fakeClock) {
	t.Helper()
	c := &fakeClock{now: time.Unix(1, 0)}
	b, err := NewBudget(Limits{BytesPerSecond: 100, BurstBytes: 100, SourceBytesPerSecond: 50, SourceBurstBytes: 50, MaxUnitBytes: 200, Concurrency: 2, SourceConcurrency: 1, MaxSources: 2, MaxWaiters: 1}, c)
	if err != nil {
		t.Fatal(err)
	}
	return b, c
}
func acquire(t *testing.T, b *Budget, source string, bytes int64) *Lease {
	t.Helper()
	l, _, err := b.TryAcquire(context.Background(), source, bytes)
	if err != nil {
		t.Fatal(err)
	}
	return l
}
func finish(t *testing.T, l *Lease, bytes int64) {
	t.Helper()
	if err := l.Finish(bytes); err != nil {
		t.Fatal(err)
	}
}

func TestBudgetSourceIsolationRefillAndBurst(t *testing.T) {
	b, c := fixture(t)
	finish(t, acquire(t, b, "a", 50), 50)
	if _, delay, err := b.TryAcquire(context.Background(), "a", 50); !errors.Is(err, ErrBusy) || delay != time.Second {
		t.Fatalf("delay=%v err=%v", delay, err)
	}
	finish(t, acquire(t, b, "b", 50), 50)
	c.advance(time.Second - time.Nanosecond)
	if _, _, err := b.TryAcquire(context.Background(), "a", 50); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	c.advance(time.Nanosecond)
	finish(t, acquire(t, b, "a", 50), 50)
	c.advance(time.Hour)
	if got := b.Usage().Tokens; got != 100 {
		t.Fatalf("burst grew: %v", got)
	}
}

func TestBudgetConcurrencyCancellationAndIdempotentRelease(t *testing.T) {
	b, _ := fixture(t)
	a := acquire(t, b, "a", 20)
	if _, _, err := b.TryAcquire(context.Background(), "a", 1); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	z := acquire(t, b, "b", 20)
	if _, _, err := b.TryAcquire(context.Background(), "b", 1); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := b.TryAcquire(ctx, "a", 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	finish(t, a, 5)
	finish(t, a, 5)
	finish(t, z, 0)
	if got := b.Usage(); got.Active != 0 || got.Tokens != 95 {
		t.Fatalf("leak: %+v", got)
	}
}

func TestBudgetOversizedUnitProgressAndDebt(t *testing.T) {
	b, c := fixture(t)
	l := acquire(t, b, "a", 200)
	if !b.Usage().Oversized {
		t.Fatal("oversized allowance missing")
	}
	finish(t, l, 200)
	if _, delay, err := b.TryAcquire(context.Background(), "a", 50); !errors.Is(err, ErrBusy) || delay != 4*time.Second {
		t.Fatalf("debt disappeared: %v %v", delay, err)
	}
	c.advance(4 * time.Second)
	finish(t, acquire(t, b, "a", 50), 50)
	if _, _, err := b.TryAcquire(context.Background(), "a", 201); err == nil || errors.Is(err, ErrBusy) {
		t.Fatalf("invalid unit accepted: %v", err)
	}
}

func TestBudgetSourceCardinalityRetainsDebt(t *testing.T) {
	b, c := fixture(t)
	finish(t, acquire(t, b, "a", 50), 50)
	finish(t, acquire(t, b, "b", 50), 50)
	if _, _, err := b.TryAcquire(context.Background(), "c", 1); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	c.advance(time.Second)
	finish(t, acquire(t, b, "c", 1), 1)
	if b.Usage().Sources != 2 {
		t.Fatal("source table unbounded")
	}
}

func TestBudgetWaitersBoundedAndCanceledWithoutSlots(t *testing.T) {
	b, c := fixture(t)
	finish(t, acquire(t, b, "a", 50), 50)
	entered := make(chan struct{})
	c.wait = func(ctx context.Context, _ time.Duration) error { close(entered); <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := b.Acquire(ctx, "a", 1); done <- err }()
	<-entered
	if _, err := b.Acquire(context.Background(), "b", 1); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if b.Usage().Active != 0 {
		t.Fatal("wait held slot")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if b.Usage().Waiters != 0 {
		t.Fatal("waiter leak")
	}
}

func TestBudgetPartialFailureAndUnderestimateCharged(t *testing.T) {
	b, _ := fixture(t)
	l := acquire(t, b, "a", 20)
	if err := l.Finish(30); err == nil {
		t.Fatal("underestimate not reported")
	}
	if got := b.Usage(); got.Tokens != 70 || got.Active != 0 {
		t.Fatalf("charge lost: %+v", got)
	}
}
