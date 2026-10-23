package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/recovery"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBudgetedBootstrapCanceledCaptureKeepsWorkLease(t *testing.T) {
	h, s, request := replicaTransferFixture(t)
	limits, err := config.DefaultRecoveryConfig().Limits()
	require.NoError(t, err)
	s.budget, err = recovery.NewBudget(limits, nil)
	require.NoError(t, err)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	s.replicaCapture = func(ctx context.Context, _ *fsm.ReplicaBootstrapSourceTicket) (*fsm.ReplicaBootstrapCapture, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := h.BeginBootstrapTransfer(ctx, request); done <- err }()
	transferWait(t, entered)
	cancel()
	transferWait(t, canceled)
	require.Equal(t, 1, s.budget.Usage().Active)
	releaseOnce.Do(func() { close(release) })
	require.ErrorIs(t, <-done, context.Canceled)
	transferWaitEmpty(t, s)
	require.Zero(t, s.budget.Usage().Active)
	require.Zero(t, s.budget.Usage().Waiters)
}

func TestBudgetedBootstrapChunkRetainsLeaseThroughResponse(t *testing.T) {
	h, s, request := replicaTransferFixture(t)
	limits, err := config.DefaultRecoveryConfig().Limits()
	require.NoError(t, err)
	s.budget, err = recovery.NewBudget(limits, nil)
	require.NoError(t, err)
	p, err := h.TopicManager.GetTopic(request.Topic).GetPartition(0)
	require.NoError(t, err)
	require.NoError(t, p.ReplicaAppendWithMode([]types.Message{{Offset: 0, Payload: "recover"}}, true))
	require.NoError(t, p.ApplyReplicaHWM(1))
	transferMetadata(t, s, request, func(m *fsm.PartitionMetadata) { m.CommittedHWM = 1 })
	response, err := h.BeginBootstrapTransfer(context.Background(), request)
	require.NoError(t, err)
	ctx, completion := recovery.WithCompletion(context.Background())
	defer completion.Finish()
	chunk, err := h.ReadBootstrapTransferChunk(ctx, transferChunk(response))
	require.NoError(t, err)
	require.NotEmpty(t, chunk.Data)
	require.Equal(t, 1, s.budget.Usage().Active)
	completion.Finish()
	require.Zero(t, s.budget.Usage().Active)
	_, err = h.ReleaseBootstrapTransfer(ctx, transferRelease(request))
	require.NoError(t, err)
}

func TestBudgetedBootstrapRenewsIdleLeaseButNeverHardLifetime(t *testing.T) {
	now := time.Unix(1, 0)
	s := &bootstrapTransferSource{ttl: 5 * time.Minute}
	e := &bootstrapSourceAttempt{phase: bootstrapReady, expires: now.Add(s.ttl), hardExpires: now.Add(time.Hour), activity: make(chan struct{}, 1)}
	s.touchReadyLocked(e, now.Add(4*time.Minute))
	require.Equal(t, now.Add(9*time.Minute), e.expires)
	require.Len(t, e.activity, 1)
	// Notifications coalesce and storage remains bounded during a long transfer.
	for n := 8; n < 60; n += 4 {
		s.touchReadyLocked(e, now.Add(time.Duration(n)*time.Minute))
	}
	require.Equal(t, e.hardExpires, e.expires)
	s.touchReadyLocked(e, now.Add(time.Hour))
	require.Equal(t, e.hardExpires, e.expires, "expired owners cannot be resurrected")
	e.expires = now.Add(time.Minute)
	s.touchReadyLocked(e, now.Add(2*time.Minute))
	require.Equal(t, now.Add(time.Minute), e.expires, "idle expiry cannot be bypassed")
}
