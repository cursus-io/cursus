package disk

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// lockProofContext acquires locks in the supplied order. Cancellation releases
// any acquired locks in reverse order, including while queued behind maintenance.
// Only the context-aware proof APIs use this; ordinary mutation ordering stays
// unchanged. The retry timer is allocated only when a lock is contended.
func lockProofContext(ctx context.Context, locks ...*sync.Mutex) (func(), error) {
	acquired := 0
	unlock := func() {
		for acquired > 0 {
			acquired--
			locks[acquired].Unlock()
		}
	}
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for acquired < len(locks) {
		if err := ctx.Err(); err != nil {
			unlock()
			return nil, err
		}
		if locks[acquired].TryLock() {
			acquired++
			continue
		}
		if ticker == nil {
			ticker = time.NewTicker(time.Millisecond)
		}
		select {
		case <-ctx.Done():
			unlock()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
	return unlock, nil
}

// A canceled request may leave a flush signal queued; flushLoop owns completing
// it and closes done without requiring the caller to remain waiting.
func (d *DiskHandler) flushProofContext(ctx context.Context) error {
	done := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.done:
		return fmt.Errorf("disk handler is closed")
	case d.flushSignal <- done:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.done:
		return fmt.Errorf("disk handler is closed")
	case <-done:
		return nil
	}
}
