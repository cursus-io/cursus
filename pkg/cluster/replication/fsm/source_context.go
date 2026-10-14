package fsm

import (
	"context"
	"sync"
	"time"
)

// Wait in the established transition -> materialization order without leaving a
// background goroutine holding fences after the request has expired.
func lockSourceContext(ctx context.Context, locks ...*sync.Mutex) (func(), error) {
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
