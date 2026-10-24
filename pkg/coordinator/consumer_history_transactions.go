package coordinator

import (
	"fmt"
	"sort"

	"github.com/cursus-io/cursus/pkg/types"
)

// A committed later intent is a durable epoch witness. Processing-v1 INIT holds
// the per-ID state lock through TXN_SYNC and forbids reinitialization before
// reservation release/offset materialization. SEND_OFFSETS holds that same lock.
// Thus a later physical committed intent can fence an older PROCESSING terminal
// epoch. A manager snapshot cannot: INIT mutates it BEFORE Raft acknowledgement.
type consumerHistoryTransactionFence struct {
	producer string
	epoch    int64
	offset   uint64
}

type historyTransactionKey struct {
	id    string
	epoch int64
}

type historyTransaction struct {
	intents []types.Message
	markers []types.Message
}

// PermitsTransactionRemoval is consumed only together with RemovedRecords by
// the checked storage installer. It never grants permission for a generic key
// compactor or a new removal list.
func (p *ConsumerHistoryCompactionPlan) PermitsTransactionRemoval(m types.Message) bool {
	fence, ok := p.transactions[m.TransactionalID]
	return ok && m.Epoch < fence.epoch && m.Offset < fence.offset && (m.TransactionMarker != types.TransactionMarkerNone || m.ProducerID == fence.producer)
}

func (p *ConsumerHistoryCompactionPlan) HasTransactionRemovals() bool {
	return len(p.transactions) != 0
}

func historyTransactionOf(m types.Message) historyTransactionKey {
	return historyTransactionKey{m.TransactionalID, m.Epoch}
}

// selectRetiredTransactions adds only complete old processing epochs. Controls
// are never removed ahead of intents in other segments. Keeping an intent as a
// producer/provenance witness also keeps its marker, so every intermediate
// single-segment publication remains independently recoverable.
func (p *ConsumerHistoryCompactionPlan) selectRetiredTransactions(transactions map[historyTransactionKey]*historyTransaction, keep map[uint64]bool) {
	anchors := make(map[string]consumerHistoryTransactionFence)
	for key, tx := range transactions {
		for _, m := range tx.intents {
			r, versioned, err := decodeConsumerMetadataRecord(m.Payload)
			if err != nil || !versioned || r.Version != ConsumerMetadataRecordVersionTransactionHistory || r.CoordinatorEpoch <= 0 ||
				m.TransactionState != types.TransactionStateOpen || m.Key != consumerMetadataRecordKey(r) {
				continue
			}
			old, exists := anchors[key.id]
			if !exists || m.Epoch > old.epoch || m.Epoch == old.epoch && m.Offset > old.offset {
				anchors[key.id] = consumerHistoryTransactionFence{m.ProducerID, m.Epoch, m.Offset}
			}
		}
	}
	for _, anchor := range anchors {
		keep[anchor.offset] = true
	}
	p.transactions = make(map[string]consumerHistoryTransactionFence)
	for key, tx := range transactions {
		fence, ok := anchors[key.id]
		if !ok || fence.producer == "" || fence.epoch <= key.epoch || len(tx.markers) == 0 {
			continue
		}
		marker := tx.markers[0]
		valid := true
		seen := make(map[int64]bool)
		for _, control := range tx.markers {
			if control.Offset >= fence.offset || control.ControlBatchCoordinatorEpoch <= 0 ||
				control.ProducerID != fmt.Sprintf("txn-history:%s:%s", key.id, control.TransactionMarker) ||
				control.TransactionMarker != marker.TransactionMarker || seen[control.ControlBatchCoordinatorEpoch] {
				valid = false
				break
			}
			seen[control.ControlBatchCoordinatorEpoch] = true
		}
		for _, intent := range tx.intents {
			r, versioned, err := decodeConsumerMetadataRecord(intent.Payload)
			if err != nil || !versioned || r.Version != ConsumerMetadataRecordVersionTransactionHistory || r.CoordinatorEpoch <= 0 ||
				r.CoordinatorEpoch > marker.ControlBatchCoordinatorEpoch || intent.ProducerID != fence.producer ||
				intent.TransactionState != types.TransactionStateOpen || intent.Offset >= marker.Offset || intent.Key != consumerMetadataRecordKey(r) {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		allRemoved := true
		used := false
		for _, intent := range tx.intents {
			if intent.Offset < p.first || intent.Offset >= p.end || keep[intent.Offset] {
				allRemoved = false
				continue
			}
			p.removed = append(p.removed, intent)
			used = true
		}
		if allRemoved {
			for _, control := range tx.markers {
				if control.Offset >= p.first && control.Offset < p.end && !keep[control.Offset] {
					p.removed = append(p.removed, control)
					used = true
				}
			}
		}
		if used {
			p.transactions[key.id] = fence
		}
	}
	sort.Slice(p.removed, func(i, j int) bool { return p.removed[i].Offset < p.removed[j].Offset })
}
