package topic

import (
	"fmt"
	"sync"
)

// committedEventIDIndex is disposable: the committed retained log is the only
// source of truth. A new Partition rebuilds it lazily, without a format change
// or a new durability dependency. Entries cover exactly [first, next).
type committedEventIDIndex struct {
	mu         sync.Mutex
	ids        map[string]struct{}
	first      uint64
	next       uint64
	generation uint64
}

// CommittedEventIDExists checks identity in the stable retained prefix of an
// aggregate replay stream, which prohibits compaction and transactional writes.
// Ordinary appends cannot rewrite its indexed prefix. Callers must serialize
// check-and-append for a partition and retain their replication
// mutation fence until the write outcome is resolved. The cache mutex alone
// does not reserve an ID for a future append.
func (p *Partition) CommittedEventIDExists(eventID string) (exists bool, err error) {
	idx := &p.eventIDs
	idx.mu.Lock()
	defer idx.mu.Unlock()
	defer func() {
		if err != nil {
			// Never use a partially scanned or unreadable prefix as absence proof.
			idx.ids = nil
		}
	}()
	if err = p.RecoveryError(); err != nil {
		return false, err
	}
	if err = p.hwmReadinessError(); err != nil {
		return false, err
	}
	generation := p.eventIDGeneration.Load()
	first, limit := p.FirstOffset(), p.LastStableOffset()
	if first > limit {
		return false, fmt.Errorf("event ID retention floor %d exceeds stable tail %d", first, limit)
	}
	if idx.ids == nil || idx.generation != generation || idx.first != first || idx.next > limit {
		idx.ids = make(map[string]struct{})
		idx.first, idx.next, idx.generation = first, first, generation
	}
	for idx.next < limit {
		// Match the committed reader's storage batch size, avoiding repeated
		// decoding of overlapping 1024-record batches for 256-record pages.
		messages, _, next, readErr := p.ReadCommittedPage(idx.next, 1024, 0, true)
		if readErr != nil {
			return false, readErr
		}
		if next <= idx.next {
			return false, fmt.Errorf("event ID scan made no progress at offset %d", idx.next)
		}
		for _, msg := range messages {
			if msg.Offset >= limit {
				break
			}
			if msg.EventID != "" {
				idx.ids[msg.EventID] = struct{}{}
			}
		}
		idx.next = min(next, limit)
	}
	if p.eventIDGeneration.Load() != generation || p.FirstOffset() != first || p.LastStableOffset() < limit {
		return false, fmt.Errorf("committed event ID history changed during scan")
	}
	if err = p.RecoveryError(); err != nil {
		return false, err
	}
	if err = p.hwmReadinessError(); err != nil {
		return false, err
	}
	_, exists = idx.ids[eventID]
	return exists, nil
}
