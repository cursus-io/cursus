package topic

import (
	"context"
	"errors"
	"time"
)

// lockReplicaContext waits without creating a goroutine that could outlive its
// recovery owner. Already-started filesystem calls remain synchronous.
func lockReplicaContext(ctx context.Context, tryLock func() bool, unlock func()) (func(), error) {
	if ctx == nil {
		return nil, errors.New("replica lock requires context")
	}
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if tryLock() {
			return unlock, nil
		}
		if ticker == nil {
			ticker = time.NewTicker(time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// BeginReplicationMutationContext fences replacement for a cancellable request.
func (p *Partition) BeginReplicationMutationContext(ctx context.Context) (func(), error) {
	return lockReplicaContext(ctx, p.reconcileMu.TryRLock, p.reconcileMu.RUnlock)
}
