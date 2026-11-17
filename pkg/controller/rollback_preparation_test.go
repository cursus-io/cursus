package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type preparedRollbackStorage struct {
	*disk.DiskHandler
	beforeScan func() error
	scans      int
}

func TestRollbackPreparationIsNeverWireAuthority(t *testing.T) {
	_, _, p, batch := indexedApplyFixtureWithProvider(t, func(dm *disk.DiskManager) topic.HandlerProvider { return dm })
	prepared, err := p.PrepareReplicaRollbackContext(context.Background(), *batch.TruncateTo, batch.StartOffset, batch.MatchedPrefix, batch.CommittedHWM)
	require.NoError(t, err)
	batch.LocalRollbackPreparation = prepared
	encoded, err := json.Marshal(batch)
	require.NoError(t, err)
	var received fsm.ReplicaCatchupBatch
	require.NoError(t, json.Unmarshal(encoded, &received))
	require.Nil(t, received.LocalRollbackPreparation)
	require.NoError(t, fsm.ValidateReplicaCatchupBatchDigest(received))
}

func (s *preparedRollbackStorage) ScanRetainedRange(ctx context.Context, start, end uint64, visit func(types.Message) error) error {
	s.scans++
	if s.beforeScan != nil {
		if err := s.beforeScan(); err != nil {
			return err
		}
	}
	return s.DiskHandler.ScanRetainedRange(ctx, start, end, visit)
}

type preparedRollbackProvider struct {
	*disk.DiskManager
	storage *preparedRollbackStorage
}

func (p *preparedRollbackProvider) GetHandlerWithPolicy(name string, id int, policy string, hours int, bytes int64) (types.StorageHandler, error) {
	h, err := p.DiskManager.GetHandlerWithPolicy(name, id, policy, hours, bytes)
	if err != nil {
		return nil, err
	}
	p.storage = &preparedRollbackStorage{DiskHandler: h.(*disk.DiskHandler)}
	return p.storage, nil
}

// TestRollbackPreparationDoesNotBlockRaft proves the expensive retained scan is
// outside metadata exclusion and stale authority is rejected before truncation.
func TestRollbackPreparationDoesNotBlockRaft(t *testing.T) {
	for _, mode := range []string{"unrelated_commit", "leader_changed"} {
		t.Run(mode, func(t *testing.T) {
			var provider *preparedRollbackProvider
			h, state, p, batch := indexedApplyFixtureWithProvider(t, func(dm *disk.DiskManager) topic.HandlerProvider {
				provider = &preparedRollbackProvider{DiskManager: dm}
				return provider
			})
			provider.storage.scans = 0
			done := make(chan interface{}, 1)
			provider.storage.beforeScan = func() error {
				go func() {
					done <- state.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"observer","addr":"observer:9001","status":"active"}`)})
				}()
				select {
				case result := <-done:
					if result != nil {
						return fmt.Errorf("metadata apply: %v", result)
					}
				case <-time.After(time.Second):
					return fmt.Errorf("retained reconstruction blocked unrelated Raft apply")
				}
				if mode == "leader_changed" {
					meta := state.GetPartitionMetadata("orders-0")
					meta.LeaderEpoch++
					applyPartitionMetadata(t, state, "orders", 0, *meta)
				}
				return nil
			}
			err := h.ApplyReplicaCatchup(context.Background(), batch)
			if mode == "unrelated_commit" {
				require.NoError(t, err)
				require.Equal(t, uint64(1), p.NextOffset())
				require.Equal(t, 1, provider.storage.scans)
			} else {
				require.Error(t, err)
				require.Equal(t, uint64(2), p.NextOffset())
				require.NoError(t, p.RecoveryError())
			}
		})
	}
}
