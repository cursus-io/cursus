package topic

import (
	"context"
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRollbackPreparationRejectsChangedEvidence(t *testing.T) {
	for _, mode := range []string{"generation", "append", "hwm", "owner", "consumed", "proof"} {
		t.Run(mode, func(t *testing.T) {
			p, d, _ := rollbackPartition(t)
			ctx := context.Background()
			if mode == "hwm" {
				p.mu.Lock()
				p.HWM = 2
				p.mu.Unlock()
			}
			prepared, err := p.PrepareReplicaRollbackContext(ctx, 1, 3, nil, 3)
			require.NoError(t, err)
			var prefix *disk.LineagePrefix
			switch mode {
			case "generation":
				require.NoError(t, d.WriteDirect("orders", 0, types.Message{Offset: 0, Payload: "rewritten"}))
			case "append":
				require.NoError(t, p.ReplicaAppend([]types.Message{{Offset: 3, Payload: "later"}}))
			case "hwm":
				require.NoError(t, p.ApplyReplicaHWM(3))
			case "owner":
				other, _, _ := rollbackPartition(t)
				p = other
			case "consumed":
				// Rejected admission still consumes this single-use preparation.
				denied := errors.New("consumer admission denied")
				require.ErrorIs(t, p.ApplyPreparedReplicaRollbackChecked(ctx, prepared, 1, 3, nil, 3, func() error { return denied }), denied)
			case "proof":
				prefix = &disk.LineagePrefix{EndOffset: 1}
			}
			before := p.NextOffset()
			called := false
			err = p.ApplyPreparedReplicaRollbackChecked(ctx, prepared, 1, 3, prefix, 3, func() error { called = true; return nil })
			require.Error(t, err)
			require.False(t, called)
			require.Equal(t, before, p.NextOffset())
			require.NoError(t, p.RecoveryError())
		})
	}
}

func TestRollbackPreparationCancellationIsRetryable(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &cancelRollbackStorage{DiskHandler: d, phase: "scan", cancel: cancel}
	p.dh = s
	prepared, err := p.PrepareReplicaRollbackContext(ctx, 1, 3, nil, 3)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, prepared)
	require.Zero(t, s.truncations)
	require.Equal(t, uint64(3), p.NextOffset())
	require.NoError(t, p.RecoveryError())
	s.phase = ""
	prepared, err = p.PrepareReplicaRollbackContext(context.Background(), 1, 3, nil, 3)
	require.NoError(t, err)
	require.NoError(t, p.ApplyPreparedReplicaRollbackChecked(context.Background(), prepared, 1, 3, nil, 3, nil))
}

func TestRollbackPreparationRejectsRewriteDuringScan(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	p.dh = &rollbackScanStorage{d, func() error {
		return d.WriteDirect("orders", 0, types.Message{Offset: 0, Payload: "rewritten during preparation"})
	}}
	prepared, err := p.PrepareReplicaRollbackContext(context.Background(), 1, 3, nil, 3)
	require.ErrorIs(t, err, disk.ErrStaleContentGeneration)
	require.Nil(t, prepared)
	require.Equal(t, uint64(3), p.NextOffset())
	require.NoError(t, p.RecoveryError())
}

type preparedMutationStorage struct{ *disk.DiskHandler }

func (s *preparedMutationStorage) TruncateToIfGenerationContext(ctx context.Context, offset uint64, generation disk.ContentGeneration) error {
	if err := s.DiskHandler.TruncateToIfGenerationContext(ctx, offset, generation); err != nil {
		return err
	}
	// An extra destructive write must not be certified by the prepared maps.
	return s.DiskHandler.WriteDirect("orders", 0, types.Message{Offset: 0, Payload: "unexpected rewrite"})
}

func TestPreparedRollbackRejectsPostTruncateRewrite(t *testing.T) {
	p, d, _ := rollbackPartition(t)
	p.dh = &preparedMutationStorage{d}
	ctx := context.Background()
	prepared, err := p.PrepareReplicaRollbackContext(ctx, 1, 3, nil, 3)
	require.NoError(t, err)
	err = p.ApplyPreparedReplicaRollbackChecked(ctx, prepared, 1, 3, nil, 3, nil)
	require.ErrorIs(t, err, disk.ErrStaleContentGeneration)
	require.ErrorIs(t, err, ErrReplicaRollbackFailed)
	require.Error(t, p.RecoveryError())
}

func TestPreparedRollbackCheckpointFailureStaysUnavailable(t *testing.T) {
	p, _, _ := rollbackPartition(t)
	ctx := context.Background()
	prepared, err := p.PrepareReplicaRollbackContext(ctx, 1, 3, nil, 3)
	require.NoError(t, err)
	failure := errors.New("checkpoint directory failure")
	p.hwmCheckpointMu.Lock()
	p.producerCheckpointMu.Lock()
	p.checkpointDirectorySync = func(string) error { return failure }
	p.producerCheckpointMu.Unlock()
	p.hwmCheckpointMu.Unlock()
	err = p.ApplyPreparedReplicaRollbackChecked(ctx, prepared, 1, 3, nil, 3, nil)
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, err, ErrReplicaRollbackFailed)
	require.Error(t, p.RecoveryError())
}

func TestPreparedRollbackResolvesCurrentTransactionDecision(t *testing.T) {
	p, _, _ := rollbackPartitionRecords(t, []types.Message{
		{Payload: "open", TransactionalID: "txn", Epoch: 2, TransactionState: types.TransactionStateOpen},
		{Payload: "marker", TransactionalID: "txn", Epoch: 2, TransactionMarker: types.TransactionMarkerAbort, TransactionState: types.TransactionStateAborted},
		{Payload: "discard"},
	})
	p.SetTransactionDecisionResolver(&testTransactionDecisionResolver{state: types.TransactionStateAborted, known: true})
	ctx := context.Background()
	prepared, err := p.PrepareReplicaRollbackContext(ctx, 2, 3, nil, 3)
	require.NoError(t, err)
	// A decision changed after scanning; install must not retain the old answer.
	p.SetTransactionDecisionResolver(&testTransactionDecisionResolver{state: types.TransactionStateCommitted, known: true})
	require.NoError(t, p.ApplyPreparedReplicaRollbackChecked(ctx, prepared, 2, 3, nil, 3, nil))
	require.Len(t, p.txnOpenOffsets, 1)
	require.Equal(t, uint64(0), p.LastStableOffset())
}
