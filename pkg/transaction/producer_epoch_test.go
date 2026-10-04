package transaction

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProducerEpochSurvivesCompleteRetention(t *testing.T) {
	m := NewManagerWithExpiration(time.Hour)
	_, first, err := m.InitProducer("retired")
	require.NoError(t, err)
	now := time.Now()
	require.Equal(t, 1, m.PruneExpired(now.Add(2*time.Hour)))
	require.Equal(t, 1, m.PruneExpired(now.Add(4*time.Hour)))
	state, next := m.ExportStateWithProducerEpoch()
	require.Empty(t, state)
	restored := NewManager()
	require.NoError(t, restored.ImportState(state))
	require.NoError(t, restored.RestoreProducerEpochWatermark(next))
	_, second, err := restored.InitProducer("retired")
	require.NoError(t, err)
	require.Greater(t, second, first)
	// Rollback/import of older state must never rewind the allocator.
	restored.Delete("retired")
	require.NoError(t, restored.ImportState(nil))
	require.NoError(t, restored.RestoreProducerEpochWatermark(0))
	_, third, err := restored.InitProducer("retired")
	require.NoError(t, err)
	require.Greater(t, third, second)
}

func TestProducerEpochConcurrentAllocationAndOutOfOrderReplication(t *testing.T) {
	m := NewManager()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			_, _, err := m.InitProducer(fmt.Sprintf("tx-%d", i))
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	state, next := m.ExportStateWithProducerEpoch()
	require.Equal(t, uint64(100), next)
	seen := make(map[int64]bool)
	for _, tx := range state {
		require.False(t, seen[tx.Epoch])
		seen[tx.Epoch] = true
	}
	// Different IDs can reach Raft in a different order from allocation.
	peer := NewManager()
	for _, tx := range state {
		require.NoError(t, peer.ApplyReplicatedSnapshot(tx))
	}
	require.NoError(t, peer.ImportState(nil))
	_, epoch, err := peer.InitProducer("tx-0")
	require.NoError(t, err)
	require.Equal(t, int64(100), epoch)
}

func TestProducerEpochExhaustionDoesNotWrapOrMutateTransaction(t *testing.T) {
	m := NewManager()
	require.NoError(t, m.RestoreProducerEpochWatermark(math.MaxInt64))
	_, epoch, err := m.InitProducer("last")
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), epoch)
	before := m.ExportState()
	_, _, err = m.InitProducer("last")
	require.ErrorContains(t, err, "exhausted")
	_, _, err = m.InitProducer("new")
	require.ErrorContains(t, err, "exhausted")
	require.Equal(t, before, m.ExportState())
	require.Error(t, m.RestoreProducerEpochWatermark(math.MaxUint64))
}
