package topic

import (
	"context"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestReplicaRetainedPrefixUsesDurableStorageAndOriginalBounds(t *testing.T) {
	p, storage, _ := rollbackPartition(t)
	ctx := context.Background()
	actual, err := p.ReplicaRetainedPrefixContext(ctx, 0, 3)
	require.NoError(t, err)
	expected, err := storage.RetainedPrefixContext(ctx, 0, 3)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	_, err = p.ReplicaRetainedPrefixContext(ctx, 1, 3)
	require.Error(t, err, "a different retained anchor cannot certify the local log")
	_, err = p.ReplicaRetainedPrefixContext(ctx, 0, 4)
	require.Error(t, err, "a proof cannot manufacture an absent suffix")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = p.ReplicaRetainedPrefixContext(canceled, 0, 3)
	require.ErrorIs(t, err, context.Canceled)
	// An old provider may expose reads, but those reads are not proof authority.
	p.dh = struct{ types.StorageHandler }{storage}
	_, err = p.ReplicaRetainedPrefixContext(ctx, 0, 3)
	require.ErrorIs(t, err, ErrReplicaSourceUnsupported)
}

func TestReplicaRequestCancelsPartitionLockWaits(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	for _, gate := range []string{"reconciliation", "capture", "validation"} {
		t.Run(gate, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			var err error
			if gate == "reconciliation" {
				p.reconcileMu.Lock()
				defer p.reconcileMu.Unlock()
				var release func()
				release, err = p.BeginReplicationMutationContext(ctx)
				require.Nil(t, release)
			} else {
				p.mu.Lock()
				defer p.mu.Unlock()
				if gate == "capture" {
					_, err = p.CaptureReplicaRequestWitness(ctx, 3)
				} else {
					err = p.ValidateReplicaRequestWitness(ctx, nil, 3)
				}
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}
