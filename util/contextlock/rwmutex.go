// Package contextlock provides queued, cancellable read/write exclusion.
package contextlock

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/semaphore"
)

const readerCapacity int64 = 1 << 30

// RWMutex is usable without initialization and must not be copied after use.
// Waiting writers prevent later readers from bypassing them. Cancellation
// removes the waiter synchronously; no helper goroutine can retain ownership.
type RWMutex struct {
	once sync.Once
	gate *semaphore.Weighted
}

func (m *RWMutex) semaphore() *semaphore.Weighted {
	m.once.Do(func() { m.gate = semaphore.NewWeighted(readerCapacity) })
	return m.gate
}

func (m *RWMutex) acquire(ctx context.Context, weight int64) error {
	if ctx == nil {
		return fmt.Errorf("lock requires context")
	}
	return m.semaphore().Acquire(ctx, weight)
}

func (m *RWMutex) LockContext(ctx context.Context) error  { return m.acquire(ctx, readerCapacity) }
func (m *RWMutex) RLockContext(ctx context.Context) error { return m.acquire(ctx, 1) }
func (m *RWMutex) Lock()                                  { _ = m.LockContext(context.Background()) }
func (m *RWMutex) RLock()                                 { _ = m.RLockContext(context.Background()) }
func (m *RWMutex) Unlock()                                { m.semaphore().Release(readerCapacity) }
func (m *RWMutex) RUnlock()                               { m.semaphore().Release(1) }
func (m *RWMutex) TryLock() bool                          { return m.semaphore().TryAcquire(readerCapacity) }
func (m *RWMutex) TryRLock() bool                         { return m.semaphore().TryAcquire(1) }
