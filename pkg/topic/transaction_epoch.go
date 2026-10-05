package topic

import "fmt"

// RetainedTransactionProducerEpoch scans retained raw records during standalone
// journal migration. Committed-only reads cannot recover epochs from aborted or
// unresolved transactions. A read failure must stop migration, not silently
// initialize an allocator below an epoch still used in the partition log.
// Call this during startup, before accepting writes or changing topic topology.
func (tm *TopicManager) RetainedTransactionProducerEpoch() (uint64, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	var next uint64
	for name, t := range tm.topics {
		t.mu.RLock()
		partitions := append([]*Partition(nil), t.Partitions...)
		t.mu.RUnlock()
		for _, p := range partitions {
			floor, err := p.retainedTransactionProducerEpoch()
			if err != nil {
				return 0, fmt.Errorf("recover producer epoch from %s[%d]: %w", name, p.id, err)
			}
			next = max(next, floor)
		}
	}
	return next, nil
}

func (p *Partition) retainedTransactionProducerEpoch() (uint64, error) {
	p.reconcileMu.RLock()
	defer p.reconcileMu.RUnlock()
	if p.dh == nil {
		return 0, fmt.Errorf("partition storage is unavailable")
	}
	var next uint64
	// Compaction can remove a producer's last raw record while its retained
	// deduplication checkpoint still fences that identity.
	p.producerStateMu.RLock()
	for _, entry := range p.producerStateIndex {
		if entry.Epoch >= 0 {
			next = max(next, uint64(entry.Epoch)+1)
		}
	}
	p.producerStateMu.RUnlock()
	end := p.dh.GetAbsoluteOffset()
	for cursor := p.dh.GetFirstOffset(); cursor < end; {
		messages, err := p.dh.ReadMessages(cursor, 1024)
		if err != nil {
			return 0, err
		}
		if len(messages) == 0 {
			// Compaction may remove the remainder of the physical range.
			break
		}
		previous := cursor
		for _, msg := range messages {
			if msg.Offset >= end {
				break
			}
			if msg.TransactionalID != "" {
				if msg.Epoch < 0 {
					return 0, fmt.Errorf("invalid transaction producer epoch %d", msg.Epoch)
				}
				next = max(next, uint64(msg.Epoch)+1)
			}
			cursor = max(cursor, msg.Offset+1)
		}
		if cursor <= previous {
			return 0, fmt.Errorf("partition epoch scan made no progress at offset %d", cursor)
		}
	}
	return next, nil
}
