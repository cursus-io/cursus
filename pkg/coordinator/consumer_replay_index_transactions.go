package coordinator

import (
	"fmt"
	"sort"

	"github.com/cursus-io/cursus/pkg/types"
)

type consumerIndexTransactionResolver interface {
	ConsumerMetadataIndexTransactionVisible(int, types.Message) (bool, error)
}

func indexHistoryTransaction(m types.Message) bool {
	if !indexTransaction(m) {
		return false
	}
	if m.TransactionMarker != types.TransactionMarkerNone {
		return m.ProducerID == fmt.Sprintf("txn-history:%s:%s", m.TransactionalID, m.TransactionMarker) && validateCleanupTransactionControl(m) == nil
	}
	r, versioned, err := decodeConsumerMetadataRecord(m.Payload)
	return err == nil && versioned && r.Version == ConsumerMetadataRecordVersionTransactionHistory &&
		r.Type == ConsumerMetadataRecordTransactionalOffsetSnapshot && m.TransactionalID == r.TransactionalID &&
		m.ProducerID == r.ProducerID && m.Epoch == r.ProducerEpoch && m.Key == consumerMetadataRecordKey(r) &&
		(m.TransactionState == types.TransactionStateOpen || m.TransactionState == types.TransactionStateCommitted)
}

func sortReplayIndexRecords(records []consumerReplayIndexRecord) {
	sort.Slice(records, func(i, j int) bool {
		a, b := records[i], records[j]
		if a.Partition != b.Partition {
			return a.Partition < b.Partition
		}
		return a.Message.Offset < b.Message.Offset
	})
}

func (r *indexedMetadataReader) loadTransactions(saved *consumerReplayIndex) bool {
	last := make(map[int]uint64)
	for _, entry := range saved.Transactions {
		if entry.Partition < 0 || entry.Partition >= len(saved.Partitions) || !indexHistoryTransaction(entry.Message) {
			return false
		}
		b, m := saved.Partitions[entry.Partition], entry.Message
		if m.Offset < b.First || m.Offset >= b.End {
			return false
		}
		if previous, exists := last[entry.Partition]; exists && m.Offset <= previous {
			return false
		}
		last[entry.Partition] = m.Offset
	}
	var visible []consumerReplayIndexRecord
	for _, entry := range saved.Transactions {
		if entry.Message.TransactionMarker != types.TransactionMarkerNone {
			continue
		}
		r.transactionChecks++
		if resolver, ok := r.source.(consumerIndexTransactionResolver); ok {
			visibleNow, err := resolver.ConsumerMetadataIndexTransactionVisible(entry.Partition, entry.Message)
			if err != nil {
				return false
			}
			if visibleNow {
				visible = append(visible, entry)
			}
			continue
		}
		// The source owns the current fixed decision view. An immutable cached raw
		// intent cannot certify that a formerly open transaction is still invisible.
		messages, err := r.source.ReadCommittedConsumerMetadata(r.index.Topic, entry.Partition, entry.Message.Offset, 1)
		r.readRecords += len(messages)
		for _, m := range messages {
			r.readBytes += len(m.Key) + len(m.Payload)
		}
		if err != nil {
			return false
		}
		if len(messages) == 0 || messages[0].Offset != entry.Message.Offset {
			continue
		}
		m := messages[0]
		if m.Key != entry.Message.Key || m.Payload != entry.Message.Payload || m.ProducerID != entry.Message.ProducerID || m.Epoch != entry.Message.Epoch || !indexHistoryTransaction(m) {
			return false
		}
		visible = append(visible, consumerReplayIndexRecord{entry.Partition, m})
	}
	r.index.Version = saved.Version
	r.index.Transactions = append([]consumerReplayIndexRecord(nil), saved.Transactions...)
	r.visibleTransactions = visible
	r.cachedRecords = append(append([]consumerReplayIndexRecord(nil), saved.Records...), visible...)
	sortReplayIndexRecords(r.cachedRecords)
	return true
}
