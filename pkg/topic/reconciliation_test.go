package topic

import (
	"errors"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

// Clearing an outcome must not bypass unknown HWM, a storage fault, snapshot
// recovery, a replaced marker, or a still-uncommitted retained record.
func TestCompleteReconciliationRequiresExactCommittedState(t *testing.T) {
	for _, kind := range []string{"success", "nil_marker", "replacement", "storage", "snapshot", "hwm", "leo", "unknown_hwm"} {
		t.Run(kind, func(t *testing.T) {
			storage := new(MockStorageHandler)
			storage.On("GetLatestOffset").Return(uint64(1)).Once()
			p := NewPartition(0, "events", storage, nil, config.DefaultConfig())
			p.MarkReconciliationPending(errors.New("unknown reply"))
			marker := p.ReconciliationError()
			switch kind {
			case "nil_marker":
				marker = nil
			case "replacement":
				p.MarkReconciliationPending(errors.New("new failure"))
			case "storage":
				p.setRecoveryError(errors.New("corrupt storage"))
			case "snapshot":
				p.snapshotRecovery = true
			case "hwm":
				p.HWM = 0
			case "leo":
				p.LEO.Store(2)
			case "unknown_hwm":
				p.hwmAuthoritative = false
			}
			err := p.CompleteReconciliation(marker, 1)
			if kind == "success" {
				require.NoError(t, err)
				require.NoError(t, p.RecoveryError())
			} else {
				require.Error(t, err)
				require.Error(t, p.RecoveryError())
			}
			storage.AssertExpectations(t)
		})
	}
}

// Genuine recovery failures are rejected before any record proof is read.
func TestValidateReconciliationRecordPreservesStorageFailure(t *testing.T) {
	storage := new(MockStorageHandler)
	storage.On("GetLatestOffset").Return(uint64(1)).Once()
	p := NewPartition(0, "events", storage, nil, config.DefaultConfig())
	p.MarkReconciliationPending(errors.New("unknown reply"))
	p.setRecoveryError(errors.New("corrupt transaction index"))
	require.Error(t, p.ValidateReconciliationRecord(p.ReconciliationError(), types.Message{Offset: 0}))
	require.ErrorContains(t, p.RecoveryError(), "corrupt transaction index")
	storage.AssertExpectations(t)
}
