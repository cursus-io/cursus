package fsm

import (
	"fmt"
	"sort"

	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
)

func transactionCoordinatorShardKey(shard int) string {
	return fmt.Sprintf("__transaction_coordinator-%d", shard)
}

// GetTransactionCoordinator returns the durable owner and fencing epoch for a
// transactional ID's logical coordinator shard.
func (f *BrokerFSM) GetTransactionCoordinator(transactionalID string) (TransactionCoordinatorShard, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ownership, ok := f.transactionCoordinatorShards[transaction.CoordinatorShardForCount(transactionalID, f.effectiveTransactionCoordinatorShardCountLocked())]
	return ownership, ok && ownership.Owner != "" && ownership.Epoch > 0
}

// TransactionCoordinatorChanges is signalled after replicated broker
// membership changes move one or more transaction coordinator shards.
func (f *BrokerFSM) TransactionCoordinatorChanges() <-chan []int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.transactionCoordinatorChanges
}

func (f *BrokerFSM) TransactionCoordinatorShardCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.effectiveTransactionCoordinatorShardCountLocked()
}

func (f *BrokerFSM) ConfiguredTransactionCoordinatorShardCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.configuredTransactionCoordinatorShardCount
}

func (f *BrokerFSM) TransactionCoordinatorShardsOwnedBy(owner string) []int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	shards := make([]int, 0)
	for shard, ownership := range f.transactionCoordinatorShards {
		if ownership.Owner == owner {
			shards = append(shards, shard)
		}
	}
	sort.Ints(shards)
	return shards
}

func (f *BrokerFSM) effectiveTransactionCoordinatorShardCountLocked() int {
	if f.transactionCoordinatorShardCount > 0 {
		return f.transactionCoordinatorShardCount
	}
	return f.configuredTransactionCoordinatorShardCount
}

func (f *BrokerFSM) transactionCoordinatorEpochsLocked() map[int]int64 {
	epochs := make(map[int]int64, len(f.transactionCoordinatorShards))
	for shard, ownership := range f.transactionCoordinatorShards {
		epochs[shard] = ownership.Epoch
	}
	return epochs
}

// reconcileTransactionCoordinatorShardsLocked deterministically assigns the
// fixed logical shards from replicated broker membership. The caller must hold
// f.mu for writing. Every owner transition advances an epoch that fences work
// issued by the previous owner.
func (f *BrokerFSM) reconcileTransactionCoordinatorShardsLocked() []int {
	active := make([]string, 0, len(f.brokers))
	for id, broker := range f.brokers {
		if broker != nil && broker.Status == "active" {
			active = append(active, id)
		}
	}
	sort.Strings(active)

	var ring *util.ConsistentHashRing
	if len(active) > 0 {
		ring = util.NewConsistentHashRing(150, nil)
		ring.Add(active...)
	}

	changed := make([]int, 0)
	changedEpochs := make(map[int]int64)
	for shard := 0; shard < f.effectiveTransactionCoordinatorShardCountLocked(); shard++ {
		owner := ""
		if ring != nil {
			owner = ring.Get(transactionCoordinatorShardKey(shard))
		}
		current := f.transactionCoordinatorShards[shard]
		if current.Owner == owner && (owner == "" || current.Epoch > 0) {
			continue
		}
		epoch := current.Epoch
		if owner != current.Owner || epoch == 0 {
			epoch++
		}
		f.transactionCoordinatorShards[shard] = TransactionCoordinatorShard{Owner: owner, Epoch: epoch}
		changed = append(changed, shard)
		changedEpochs[shard] = epoch
	}
	if len(changed) > 0 && f.txn != nil {
		f.txn.ReconcileCoordinatorEpochs(changedEpochs, f.effectiveTransactionCoordinatorShardCountLocked())
	}
	return changed
}

func (f *BrokerFSM) notifyTransactionCoordinatorChange(changed []int) {
	if len(changed) == 0 {
		return
	}
	f.mu.RLock()
	changes := f.transactionCoordinatorChanges
	f.mu.RUnlock()
	if changes == nil {
		return
	}
	select {
	case changes <- append([]int(nil), changed...):
	default:
	}
}
