package fsm

import (
	"context"
	"github.com/cursus-io/cursus/util/contextlock"
)

// Acquire in transition -> materialization order. Cancellation removes queued
// waiters and releases earlier gates without leaving background lock owners.
func lockSourceContext(ctx context.Context, locks ...*contextlock.RWMutex) (func(), error) {
	acquired := 0
	unlock := func() {
		for acquired > 0 {
			acquired--
			locks[acquired].Unlock()
		}
	}
	for _, lock := range locks {
		if err := lock.LockContext(ctx); err != nil {
			unlock()
			return nil, err
		}
		acquired++
	}
	return unlock, nil
}
