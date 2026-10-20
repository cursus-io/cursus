// Package observation runs diagnostic work independently of request handlers.
package observation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Interval = time.Second
	Timeout  = 2 * time.Second
	MaxAge   = 5 * time.Second
)

// Status describes the last completed observation and the current attempt.
// A timeout does not imply that an uninterruptible syscall has returned.
type Status struct {
	CompletedAt time.Time `json:"completed_at"`
	StartedAt   time.Time `json:"started_at"`
	InProgress  bool      `json:"in_progress"`
	TimedOut    bool      `json:"timed_out"`
	Stale       bool      `json:"stale"`
	Stopped     bool      `json:"stopped"`
	Error       string    `json:"error,omitempty"`
	Failures    uint64    `json:"failures"`
}

func (s Status) Valid() bool {
	return !s.CompletedAt.IsZero() && !s.Stale && !s.TimedOut && !s.Stopped && s.Error == ""
}

type Snapshot[T any] struct {
	Status
	Value T
}

// Worker owns exactly one goroutine. Calls are sequential, including after a
// deadline: no replacement worker is spawned while a call remains blocked.
// Published values must be immutable, including referenced maps and slices.
type Worker[T any] struct {
	interval, timeout, maxAge time.Duration
	start                     sync.Once
	ctx                       context.Context
	cancel                    context.CancelFunc
	current                   atomic.Pointer[Snapshot[T]]
	done                      chan struct{}
}

func New[T any](interval, timeout, maxAge time.Duration) *Worker[T] {
	if interval <= 0 || timeout <= 0 || maxAge <= 0 {
		panic("observation durations must be positive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker[T]{interval: interval, timeout: timeout, maxAge: maxAge, ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

func (w *Worker[T]) Start(sample func(context.Context) (T, error)) {
	w.start.Do(func() { go w.run(sample) })
}

// Close cancels cooperative work and prevents subsequent attempts. It never
// waits for a syscall or application lock; Done closes when that work returns.
// A blocked call may retain this worker and its sources until process exit.
func (w *Worker[T]) Close() {
	w.cancel()
	w.start.Do(func() { close(w.done) })
}
func (w *Worker[T]) Done() <-chan struct{} { return w.done }

func (w *Worker[T]) Snapshot() Snapshot[T] {
	var s Snapshot[T]
	if current := w.current.Load(); current != nil {
		s = *current
	}
	now := time.Now()
	s.Stale = s.CompletedAt.IsZero() || now.Sub(s.CompletedAt) >= w.maxAge
	s.TimedOut = s.InProgress && now.Sub(s.StartedAt) >= w.timeout
	s.Stopped = w.ctx.Err() != nil
	return s
}

func (w *Worker[T]) run(sample func(context.Context) (T, error)) {
	defer close(w.done)
	var previous Snapshot[T]
	for w.ctx.Err() == nil {
		attempt := previous
		attempt.StartedAt = time.Now()
		attempt.InProgress = true
		w.current.Store(&attempt)
		ctx, cancel := context.WithTimeout(w.ctx, w.timeout)
		value, err := sample(ctx)
		completed := time.Now()
		if ctx.Err() != nil {
			err = errors.Join(err, ctx.Err())
		}
		// Do not accept a late success even if the context timer was delayed.
		if completed.Sub(attempt.StartedAt) >= w.timeout {
			err = errors.Join(err, context.DeadlineExceeded)
		}
		cancel()
		previous = Snapshot[T]{Status: Status{StartedAt: attempt.StartedAt, CompletedAt: completed, Failures: previous.Failures}, Value: value}
		if err != nil {
			previous.Error = err.Error()
			previous.Failures++
		}
		published := previous
		w.current.Store(&published)
		timer := time.NewTimer(w.interval)
		select {
		case <-w.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
