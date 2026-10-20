package contextlock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitForWriter(t *testing.T, m *RWMutex) {
	t.Helper()
	require.Eventually(t, func() bool {
		if m.TryRLock() {
			m.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
}

func TestQueuedWriterCannotBeBypassedByReaders(t *testing.T) {
	var m RWMutex
	m.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	acquired, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		if err := m.LockContext(ctx); err != nil {
			return
		}
		close(acquired)
		<-release
		m.Unlock()
	}()
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }); <-done })
	waitForWriter(t, &m)
	require.False(t, m.TryRLock(), "new readers must queue behind the writer")
	m.RUnlock()
	select {
	case <-acquired:
	case <-ctx.Done():
		t.Fatal("queued writer starved")
	}
	require.False(t, m.TryRLock())
	once.Do(func() { close(release) })
	<-done
	require.True(t, m.TryLock())
	m.Unlock()
}

func TestCancelQueuedWriterReleasesFollowingReader(t *testing.T) {
	var m RWMutex
	m.RLock()
	defer m.RUnlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.LockContext(ctx) }()
	waitForWriter(t, &m)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.True(t, m.TryRLock(), "canceled writer must leave the queue synchronously")
	m.RUnlock()
}

func TestCancellationRacingAcquisitionNeverRetainsLock(t *testing.T) {
	for i := 0; i < 100; i++ {
		var m RWMutex
		m.Lock()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			if m.RLockContext(ctx) == nil {
				m.RUnlock()
			}
		}()
		go cancel()
		m.Unlock()
		<-done
		require.True(t, m.TryLock())
		m.Unlock()
	}
	var m RWMutex
	require.Error(t, m.LockContext(nil)) //nolint:staticcheck // Exercise explicit rejection of an invalid context.
}
