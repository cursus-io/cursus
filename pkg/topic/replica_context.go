package topic

import (
	"context"
	"errors"
)

// Wait in the lock's fair queue without a background owner or polling timer.
func lockReplicaContext(ctx context.Context, lock func(context.Context) error, unlock func()) (func(), error) {
	if ctx == nil {
		return nil, errors.New("replica lock requires context")
	}
	if err := lock(ctx); err != nil {
		return nil, err
	}
	return unlock, nil
}

// BeginReplicationMutationContext fences replacement for a cancellable request.
func (p *Partition) BeginReplicationMutationContext(ctx context.Context) (func(), error) {
	return lockReplicaContext(ctx, p.reconcileMu.RLockContext, p.reconcileMu.RUnlock)
}
