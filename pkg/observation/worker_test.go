package observation

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerCloseBeforeStartCancelsAndPreventsWork(t *testing.T) {
	w := New[int](time.Second, time.Second, time.Second)
	w.Close()
	require.ErrorIs(t, w.ctx.Err(), context.Canceled)
	select {
	case <-w.Done():
	default:
		t.Fatal("unstarted worker did not close")
	}
	var calls atomic.Int32
	w.Start(func(context.Context) (int, error) { calls.Add(1); return 1, nil })
	require.Zero(t, calls.Load())
	require.False(t, w.Snapshot().Valid())
}

func TestWorkerBoundsUncooperativeWorkAndRecovers(t *testing.T) {
	w := New[int](10*time.Millisecond, 40*time.Millisecond, 100*time.Millisecond)
	t.Cleanup(w.Close)
	require.False(t, w.Snapshot().Valid())
	require.True(t, w.Snapshot().CompletedAt.IsZero())
	blocked, release := make(chan struct{}), make(chan struct{})
	var calls, active, maximum atomic.Int32
	w.Start(func(context.Context) (int, error) {
		n := active.Add(1)
		defer active.Add(-1)
		if n > maximum.Load() {
			maximum.Store(n)
		}
		if calls.Add(1) == 1 {
			close(blocked)
			<-release
		}
		return 42, nil
	})
	<-blocked
	// Hundreds of reads across multiple deadlines must not start more work.
	until := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(until) {
		_ = w.Snapshot()
		time.Sleep(time.Millisecond)
	}
	s := w.Snapshot()
	require.True(t, s.InProgress)
	require.True(t, s.TimedOut)
	require.False(t, s.Valid())
	require.EqualValues(t, 1, calls.Load())
	close(release)
	require.Eventually(t, func() bool { return w.Snapshot().Valid() }, time.Second, time.Millisecond)
	s = w.Snapshot()
	require.Equal(t, 42, s.Value)
	require.EqualValues(t, 1, s.Failures, "late success must have failed its execution budget")
	require.EqualValues(t, 1, maximum.Load())
	w.Close()
	select {
	case <-w.Done():
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	require.False(t, w.Snapshot().Valid())
}

func TestWorkerExpiryAndFailureReplacePreviousSuccess(t *testing.T) {
	w := New[int](300*time.Millisecond, time.Second, 50*time.Millisecond)
	t.Cleanup(w.Close)
	var calls atomic.Int32
	w.Start(func(context.Context) (int, error) {
		if calls.Add(1) > 1 {
			return 0, errors.New("filesystem unavailable")
		}
		return 42, nil
	})
	require.Eventually(t, func() bool { return w.Snapshot().Valid() }, time.Second, time.Millisecond)
	completed := w.Snapshot().CompletedAt
	require.Eventually(t, func() bool { return w.Snapshot().Stale }, time.Second, time.Millisecond)
	require.Equal(t, completed, w.Snapshot().CompletedAt)
	require.False(t, w.Snapshot().Valid())
	require.Eventually(t, func() bool { return w.Snapshot().Error != "" }, time.Second, time.Millisecond)
	require.Contains(t, w.Snapshot().Error, "filesystem unavailable")
	require.False(t, w.Snapshot().Valid())
}

func TestWorkerCloseDoesNotJoinBlockedCallOrRestart(t *testing.T) {
	w := New[int](time.Millisecond, 20*time.Millisecond, time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	w.Start(func(context.Context) (int, error) { calls.Add(1); close(entered); <-release; return 1, nil })
	<-entered
	closed := make(chan struct{})
	go func() { w.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close waited for uncancellable work")
	}
	require.True(t, w.Snapshot().InProgress)
	require.True(t, w.Snapshot().Stopped)
	require.False(t, w.Snapshot().Valid())
	close(release)
	select {
	case <-w.Done():
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after release")
	}
	w.Start(func(context.Context) (int, error) { calls.Add(1); return 2, nil })
	require.EqualValues(t, 1, calls.Load())
}
