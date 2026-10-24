package coordinator

import (
	"fmt"
	"sort"
)

// A sweep certifies a closed prefix once, then publishes its segments in physical
// order. It is available only for a subsequence-closed history: every target
// group has one physical source, each semantic key advances epoch/revision, and
// lifecycle versions do not change within an epoch. Keeping the final witness
// of every key then preserves every intermediate replay, not only the final one.
// Histories outside this conservative proof keep the single-segment path.
type ConsumerHistorySweep struct {
	plan    *ConsumerHistoryCompactionPlan
	next    uint64
	pending *ConsumerHistoryCompactionPlan
}

type historySubsequenceProof struct {
	valid   bool
	last    map[replayIndexKey]ConsumerMetadataRecord
	sources map[string]map[int]bool
}

func (s *historySubsequenceProof) observe(p, target int, r ConsumerMetadataRecord) {
	if r.Group == "" {
		return
	}
	// Version-7 intents are routed by transactional ID, independently from the
	// group's ordinary snapshots. They never supply materialized replay state.
	// Their separate retirement proof retains the final provenance/producer
	// witness and orders each intent before its control removal. Counting them
	// as a second ordinary source would force every real cross-owner transaction
	// back to a complete-history scan for each segment.
	if r.Type == ConsumerMetadataRecordTransactionalOffsetSnapshot && r.Version == ConsumerMetadataRecordVersionTransactionHistory {
		return
	}
	if s.sources[r.Group] == nil {
		s.sources[r.Group] = make(map[int]bool)
	}
	s.sources[r.Group][p] = true
	if p != target || r.Type == ConsumerMetadataRecordTransactionalOffsetSnapshot {
		return
	}
	key := replayIndexRecordKey(r)
	if old, exists := s.last[key]; exists {
		if r.Epoch < old.Epoch || r.Epoch == old.Epoch && (r.Revision < old.Revision || r.Type == ConsumerMetadataRecordLifecycleSnapshot && r.Version != old.Version) {
			s.valid = false
		}
	}
	s.last[key] = r
}

func (s *historySubsequenceProof) safe(target int) bool {
	if !s.valid {
		return false
	}
	for _, partitions := range s.sources {
		if partitions[target] && len(partitions) != 1 {
			return false
		}
	}
	return true
}

// Sweep converts a whole closed-prefix certificate to an ordered sweep only
// when its separately checked intermediate-state proof is available.
func (p *ConsumerHistoryCompactionPlan) Sweep() (*ConsumerHistorySweep, bool) {
	if !p.subsequenceSafe {
		return nil, false
	}
	return &ConsumerHistorySweep{plan: p, next: p.first}, true
}

// Segment returns the next non-overlapping atomic range. Call Published only
// after successful checked installation, or abandon the entire sweep on error.
func (s *ConsumerHistorySweep) Segment(first, end uint64) (*ConsumerHistoryCompactionPlan, error) {
	if s == nil || s.pending != nil || first != s.next || first >= end || end > s.plan.end {
		return nil, fmt.Errorf("consumer history sweep range is out of order")
	}
	p := *s.plan
	p.first, p.end = first, end
	p.bounds = append([]consumerReplayIndexPartition(nil), s.plan.bounds...)
	start := sort.Search(len(s.plan.removed), func(i int) bool { return s.plan.removed[i].Offset >= first })
	stop := sort.Search(len(s.plan.removed), func(i int) bool { return s.plan.removed[i].Offset >= end })
	p.removed = s.plan.removed[start:stop:stop]
	s.pending = &p
	return &p, nil
}

// Published rebases only our own successful rewrite's identity. The controller
// calls this inside the original installation callback, after validating all
// old identities and while replacement is still excluded. It is not a general
// certificate refresh: failed/external writes must discard the sweep.
func (s *ConsumerHistorySweep) Published(p *ConsumerHistoryCompactionPlan, identity string) error {
	if s == nil || p == nil || s.pending != p || identity == "" {
		return fmt.Errorf("invalid consumer history sweep publication")
	}
	s.plan.bounds[p.partition].Identity = identity
	s.next = p.end
	s.pending = nil
	return nil
}
