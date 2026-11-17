package topic

import (
	"context"
	"strings"
	"testing"

	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

type rollbackContextKey struct{}

type cancelRollbackStorage struct {
	*disk.DiskHandler
	cancel                                    context.CancelFunc
	phase                                     string
	truncations, scans, records, payloadBytes int
	contextValue                              any
}

// TruncateToIfGenerationContext injects cancellation only after disk success.
func (s *cancelRollbackStorage) TruncateToIfGenerationContext(ctx context.Context, offset uint64, generation disk.ContentGeneration) error {
	s.truncations++
	if err := s.DiskHandler.TruncateToIfGenerationContext(ctx, offset, generation); err != nil {
		return err
	}
	if s.phase == "truncate" {
		s.cancel()
	}
	return nil
}

// ScanRetainedRange measures retained work and can cancel after reconstruction.
func (s *cancelRollbackStorage) ScanRetainedRange(ctx context.Context, start, end uint64, visit func(types.Message) error) error {
	s.scans++
	s.contextValue = ctx.Value(rollbackContextKey{})
	err := s.DiskHandler.ScanRetainedRange(ctx, start, end, func(msg types.Message) error {
		s.records++
		s.payloadBytes += len(msg.Payload)
		return visit(msg)
	})
	if s.phase == "scan" {
		s.cancel()
	}
	return err
}

// TestReplicaRollbackCompletesAfterAdmittedCancellation checks that cancellation
// cannot interrupt an admitted repair or leave inconsistent state on restart.
func TestReplicaRollbackCompletesAfterAdmittedCancellation(t *testing.T) {
	for _, policy := range []string{"canonical", "speculative", "prepared_canonical", "prepared_speculative"} {
		for _, phase := range []string{"truncate", "scan", "checkpoint"} {
			if strings.HasPrefix(policy, "prepared_") && phase == "scan" {
				continue // Preparation cancellation is pre-mutation and tested separately.
			}
			t.Run(policy+"/"+phase, func(t *testing.T) {
				p, d, cfg := rollbackPartitionRecords(t, []types.Message{
					{Payload: "retained", ProducerID: "producer", Epoch: 2, SeqNum: 1, TransactionalID: "txn", TransactionState: types.TransactionStateOpen},
					{Payload: "discarded", ProducerID: "producer", Epoch: 2, SeqNum: 2},
					{Payload: "abort", TransactionalID: "txn", Epoch: 2, TransactionMarker: types.TransactionMarkerAbort, TransactionState: types.TransactionStateAborted},
				})
				wantHWM := uint64(1)
				if strings.HasSuffix(policy, "speculative") {
					p.mu.Lock()
					p.HWM = 0
					p.mu.Unlock()
					wantHWM = 0
				}
				ctx, cancel := context.WithCancel(context.WithValue(context.Background(), rollbackContextKey{}, "accounting"))
				defer cancel()
				storage := &cancelRollbackStorage{DiskHandler: d, cancel: cancel, phase: phase}
				p.dh = storage
				var prepared *ReplicaRollbackPreparation
				if strings.HasPrefix(policy, "prepared_") {
					proofHWM := uint64(3)
					if policy == "prepared_speculative" {
						proofHWM = 0
					}
					var err error
					prepared, err = p.PrepareReplicaRollbackContext(ctx, 1, 3, nil, proofHWM)
					require.NoError(t, err)
				}
				if phase == "checkpoint" {
					p.hwmCheckpointMu.Lock()
					p.producerCheckpointMu.Lock()
					p.checkpointDirectorySync = func(path string) error { cancel(); return syncPartitionDirectory(path) }
					p.producerCheckpointMu.Unlock()
					p.hwmCheckpointMu.Unlock()
				}
				var err error
				switch policy {
				case "prepared_canonical":
					err = p.ApplyPreparedReplicaRollbackChecked(ctx, prepared, 1, 3, nil, 3, nil)
				case "prepared_speculative":
					err = p.TruncatePreparedReplicaTailContext(ctx, prepared, 1)
				case "canonical":
					err = p.ApplyReplicaRollbackChecked(ctx, 1, 3, nil, 3, nil)
				default:
					err = p.TruncateReplicaTailContext(ctx, 1)
				}
				require.NoError(t, err, "caller cancellation must not poison a successful disk repair")
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.NoError(t, p.RecoveryError())
				require.Equal(t, wantHWM, p.GetHWM())
				require.Equal(t, uint64(1), p.NextOffset())
				require.Equal(t, 1, storage.truncations)
				require.Equal(t, 1, storage.scans)
				require.Equal(t, 1, storage.records)
				require.Equal(t, len("retained"), storage.payloadBytes)
				require.Equal(t, "accounting", storage.contextValue)
				require.Empty(t, p.txnMarkers)
				require.Len(t, p.txnOpenOffsets, 1)
				require.Equal(t, uint64(1), p.producerStateIndex["producer"].Seq)
				p.Close()
				require.NoError(t, d.Close())
				reopened, err := disk.NewDiskHandler(cfg, "orders", 0)
				require.NoError(t, err)
				restarted := NewPartition(0, "orders", reopened, nil, cfg)
				t.Cleanup(func() { restarted.Close(); require.NoError(t, reopened.Close()) })
				require.NoError(t, restarted.RecoveryError())
				require.Equal(t, wantHWM, restarted.GetHWM())
				require.Equal(t, uint64(1), restarted.NextOffset())
				require.Equal(t, uint64(1), restarted.producerStateIndex["producer"].Seq)
				require.Len(t, restarted.txnOpenOffsets, 1)
				require.Empty(t, restarted.txnMarkers)
				require.NoError(t, restarted.ReplicaAppend([]types.Message{{Offset: 1, Payload: "canonical replacement"}}))
				require.NoError(t, restarted.ApplyReplicaHWM(2))
			})
		}
	}
}
