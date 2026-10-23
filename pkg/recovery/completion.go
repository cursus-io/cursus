package recovery

import (
	"context"
	"sync"
)

type completionKey struct{}

// Completion extends a source lease through response serialization and socket
// writes. Cancellation cannot release it while either storage or the writer is
// still active. Calls without a network owner settle at the function boundary.
type Completion struct {
	mu       sync.Mutex
	finished bool
	release  []func()
}

func WithCompletion(ctx context.Context) (context.Context, *Completion) {
	c := &Completion{}
	return context.WithValue(ctx, completionKey{}, c), c
}

func SettleAfterResponse(ctx context.Context, release func()) {
	c, ok := ctx.Value(completionKey{}).(*Completion)
	if !ok {
		release()
		return
	}
	c.mu.Lock()
	if c.finished {
		c.mu.Unlock()
		release()
		return
	}
	c.release = append(c.release, release)
	c.mu.Unlock()
}

func (c *Completion) Finish() {
	if c == nil {
		return
	}
	c.mu.Lock()
	callbacks := c.release
	c.release, c.finished = nil, true
	c.mu.Unlock()
	for _, release := range callbacks {
		release()
	}
}
