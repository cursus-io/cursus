package transaction

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func transactionIDForShard(t *testing.T, shard, shardCount int) string {
	t.Helper()
	for i := 0; i < 100_000; i++ {
		id := fmt.Sprintf("shard-%d-txn-%d", shard, i)
		if CoordinatorShardForCount(id, shardCount) == shard {
			return id
		}
	}
	t.Fatalf("could not find transaction id for shard %d", shard)
	return ""
}

func beginProcessingTransaction(t *testing.T, manager *Manager, id string, deadline time.Time) (string, int64) {
	t.Helper()
	producer, epoch, err := manager.InitProducerWithMode(id, ModeProcessingV1)
	require.NoError(t, err)
	require.NoError(t, manager.BeginWithDeadline(id, producer, epoch, deadline))
	return producer, epoch
}

func TestPreparedTransactionsAreIndexedAndFilteredByShard(t *testing.T) {
	manager := NewManagerWithExpirationAndShards(time.Hour, 4)
	preparedID := transactionIDForShard(t, 1, 4)
	otherID := transactionIDForShard(t, 2, 4)
	producer, epoch := beginProcessingTransaction(t, manager, preparedID, time.Now().Add(time.Minute))
	otherProducer, otherEpoch := beginProcessingTransaction(t, manager, otherID, time.Now().Add(time.Minute))
	_, err := manager.PrepareCommit(preparedID, producer, epoch)
	require.NoError(t, err)
	_, err = manager.PrepareCommit(otherID, otherProducer, otherEpoch)
	require.NoError(t, err)

	got, more := manager.PreparedTransactions([]int{1}, 10)
	require.False(t, more)
	require.Len(t, got, 1)
	require.Equal(t, preparedID, got[0].ID)
	empty, emptyMore := manager.PreparedTransactions([]int{}, 10)
	require.False(t, emptyMore)
	require.Empty(t, empty)

	require.NoError(t, manager.Commit(preparedID))
	got, more = manager.PreparedTransactions([]int{1}, 10)
	require.False(t, more)
	require.Empty(t, got)
}

func TestTimedOutTransactionsUseDeadlineOrderAndBatchLimit(t *testing.T) {
	manager := NewManagerWithExpirationAndShards(time.Hour, 1)
	now := time.Now()
	for _, item := range []struct {
		id       string
		deadline time.Time
	}{{"late", now.Add(-time.Second)}, {"early", now.Add(-2 * time.Second)}, {"future", now.Add(time.Second)}} {
		beginProcessingTransaction(t, manager, item.id, item.deadline)
	}

	got, more := manager.TimedOutTransactions(nil, now, 1)
	require.True(t, more)
	require.Len(t, got, 1)
	require.Equal(t, "early", got[0].ID)

	got, more = manager.TimedOutTransactions(nil, now, 10)
	require.False(t, more)
	require.Len(t, got, 2)
	require.Equal(t, []string{"early", "late"}, []string{got[0].ID, got[1].ID})
}

func TestRecoveryScansRotateAcrossShards(t *testing.T) {
	manager := NewManagerWithExpirationAndShards(time.Hour, 2)
	now := time.Now()
	ids := make([]string, 2)
	for shard := range ids {
		ids[shard] = transactionIDForShard(t, shard, 2)
		producer, epoch := beginProcessingTransaction(t, manager, ids[shard], now.Add(-time.Second))
		_, err := manager.PrepareCommit(ids[shard], producer, epoch)
		require.NoError(t, err)
	}

	first, _ := manager.PreparedTransactions(nil, 1)
	second, _ := manager.PreparedTransactions(nil, 1)
	require.Equal(t, ids[0], first[0].ID)
	require.Equal(t, ids[1], second[0].ID)
}

func TestRuntimeSnapshotIncludesPrepareStates(t *testing.T) {
	manager := NewManagerWithExpirationAndShards(time.Hour, 1)
	producer, epoch := beginProcessingTransaction(t, manager, "prepared", time.Now().Add(time.Minute))
	_, err := manager.PrepareCommit("prepared", producer, epoch)
	require.NoError(t, err)
	time.Sleep(time.Millisecond)

	snapshot := manager.RuntimeSnapshot()
	require.Equal(t, 1, snapshot.ByState[StatePrepareCommit])
	require.Greater(t, snapshot.OldestActiveAgeSeconds, float64(0))
}

func TestImportStateRebuildsRecoveryIndexes(t *testing.T) {
	source := NewManagerWithExpirationAndShards(time.Hour, 3)
	id := transactionIDForShard(t, 2, 3)
	producer, epoch := beginProcessingTransaction(t, source, id, time.Now().Add(-time.Second))
	_, err := source.PrepareAbort(id, producer, epoch)
	require.NoError(t, err)

	restored := NewManagerWithExpirationAndShards(time.Hour, 3)
	require.NoError(t, restored.ImportState(source.ExportState()))
	prepared, more := restored.PreparedTransactions([]int{2}, 10)
	require.False(t, more)
	require.Len(t, prepared, 1)
	require.Equal(t, id, prepared[0].ID)
}

func TestIndependentManagerShardsSupportConcurrentMutation(t *testing.T) {
	manager := NewManagerWithExpirationAndShards(time.Hour, 8)
	var wg sync.WaitGroup
	errs := make(chan error, manager.ShardCount())
	for shard := 0; shard < manager.ShardCount(); shard++ {
		id := transactionIDForShard(t, shard, manager.ShardCount())
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			producer, epoch, err := manager.InitProducerWithMode(id, ModeProcessingV1)
			if err == nil {
				err = manager.BeginWithDeadline(id, producer, epoch, time.Now().Add(time.Minute))
			}
			errs <- err
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, manager.ExportState(), manager.ShardCount())
}

func BenchmarkPreparedTransactionsIndexed(b *testing.B) {
	manager := NewManagerWithExpirationAndShards(time.Hour, 50)
	for i := 0; i < 100_000; i++ {
		id := fmt.Sprintf("retained-%d", i)
		_, _, _ = manager.InitProducerWithMode(id, ModeProcessingV1)
	}
	id := ""
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("prepared-%d", i)
		if CoordinatorShardForCount(candidate, 50) == 0 {
			id = candidate
			break
		}
	}
	producer, epoch, _ := manager.InitProducerWithMode(id, ModeProcessingV1)
	_ = manager.BeginWithDeadline(id, producer, epoch, time.Now().Add(time.Minute))
	_, _ = manager.PrepareCommit(id, producer, epoch)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.PreparedTransactions([]int{0}, 256)
	}
}
