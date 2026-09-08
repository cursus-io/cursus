package transaction

import (
	"container/heap"
	"sort"
	"sync"
	"time"
)

type deadlineItem struct {
	id       string
	deadline time.Time
	index    int
}

type deadlineQueue []*deadlineItem

func (q deadlineQueue) Len() int { return len(q) }
func (q deadlineQueue) Less(i, j int) bool {
	if q[i].deadline.Equal(q[j].deadline) {
		return q[i].id < q[j].id
	}
	return q[i].deadline.Before(q[j].deadline)
}
func (q deadlineQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}
func (q *deadlineQueue) Push(value interface{}) {
	item := value.(*deadlineItem)
	item.index = len(*q)
	*q = append(*q, item)
}
func (q *deadlineQueue) Pop() interface{} {
	old := *q
	item := old[len(old)-1]
	old[len(old)-1] = nil
	item.index = -1
	*q = old[:len(old)-1]
	return item
}

type managerShard struct {
	mu           sync.Mutex
	txns         map[string]*Transaction
	prepared     map[string]struct{}
	nonTerminal  map[string]struct{}
	deadlines    deadlineQueue
	deadlineByID map[string]*deadlineItem
	expirations  deadlineQueue
	expiryByID   map[string]*deadlineItem
	expiration   time.Duration
}

func newManagerShard(expiration time.Duration) managerShard {
	return managerShard{
		txns:         make(map[string]*Transaction),
		prepared:     make(map[string]struct{}),
		nonTerminal:  make(map[string]struct{}),
		deadlineByID: make(map[string]*deadlineItem),
		expiryByID:   make(map[string]*deadlineItem),
		expiration:   expiration,
	}
}

func (s *managerShard) put(tx *Transaction) {
	if tx == nil || tx.ID == "" {
		return
	}
	s.txns[tx.ID] = tx
	s.reindex(tx)
}

func (s *managerShard) remove(id string) {
	delete(s.txns, id)
	delete(s.prepared, id)
	delete(s.nonTerminal, id)
	s.removeDeadline(id)
	s.removeExpiry(id)
}

func (s *managerShard) reindex(tx *Transaction) {
	delete(s.prepared, tx.ID)
	delete(s.nonTerminal, tx.ID)
	s.removeDeadline(tx.ID)
	s.removeExpiry(tx.ID)
	if tx.Expired {
		s.addExpiry(tx)
		return
	}
	switch tx.State {
	case StateCommitting, StatePrepareCommit, StatePrepareAbort:
		s.prepared[tx.ID] = struct{}{}
		s.nonTerminal[tx.ID] = struct{}{}
	case StateOpen:
		s.nonTerminal[tx.ID] = struct{}{}
		if tx.Mode == ModeProcessingV1 && !tx.Deadline.IsZero() {
			item := &deadlineItem{id: tx.ID, deadline: tx.Deadline}
			heap.Push(&s.deadlines, item)
			s.deadlineByID[tx.ID] = item
		}
	case StateCommitted, StateAborted:
		if tx.State == StateCommitted && tx.Mode == ModeProcessingV1 && len(tx.Offsets) > 0 && !tx.OffsetsMaterialized {
			s.prepared[tx.ID] = struct{}{}
			return
		}
		s.addExpiry(tx)
	}
}

func (s *managerShard) addExpiry(tx *Transaction) {
	if s.expiration <= 0 || tx.UpdatedAt.IsZero() {
		return
	}
	item := &deadlineItem{id: tx.ID, deadline: tx.UpdatedAt.Add(s.expiration)}
	heap.Push(&s.expirations, item)
	s.expiryByID[tx.ID] = item
}

func (s *managerShard) removeDeadline(id string) {
	if item := s.deadlineByID[id]; item != nil {
		heap.Remove(&s.deadlines, item.index)
		delete(s.deadlineByID, id)
	}
}

func (s *managerShard) removeExpiry(id string) {
	if item := s.expiryByID[id]; item != nil {
		heap.Remove(&s.expirations, item.index)
		delete(s.expiryByID, id)
	}
}

func (m *Manager) shardForID(id string) *managerShard {
	return &m.shards[CoordinatorShardForCount(id, len(m.shards))]
}

func (m *Manager) ShardCount() int {
	if m == nil {
		return 0
	}
	return len(m.shards)
}

func (m *Manager) selectedShardIDs(shardIDs []int) []int {
	if shardIDs == nil {
		ids := make([]int, len(m.shards))
		for i := range ids {
			ids[i] = i
		}
		return ids
	}
	seen := make(map[int]struct{}, len(shardIDs))
	ids := make([]int, 0, len(shardIDs))
	for _, id := range shardIDs {
		if id < 0 || id >= len(m.shards) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func (m *Manager) rotatingShardIDs(shardIDs []int, cursor *int) []int {
	ids := m.selectedShardIDs(shardIDs)
	if len(ids) < 2 {
		return ids
	}
	m.recoveryScanMu.Lock()
	start := *cursor % len(ids)
	*cursor = (*cursor + 1) % len(ids)
	m.recoveryScanMu.Unlock()
	rotated := make([]int, 0, len(ids))
	rotated = append(rotated, ids[start:]...)
	rotated = append(rotated, ids[:start]...)
	return rotated
}

func (m *Manager) PreparedTransactions(shardIDs []int, limit int) ([]*Transaction, bool) {
	if m == nil || limit <= 0 {
		return nil, false
	}
	out := make([]*Transaction, 0, limit)
	more := false
	for _, shardID := range m.rotatingShardIDs(shardIDs, &m.preparedShardCursor) {
		s := &m.shards[shardID]
		s.mu.Lock()
		for id := range s.prepared {
			if len(out) == limit {
				more = true
				break
			}
			if tx := s.txns[id]; tx != nil && !tx.Expired {
				out = append(out, clone(tx))
			}
		}
		s.mu.Unlock()
		if more {
			break
		}
	}
	return out, more
}

func (m *Manager) TimedOutTransactions(shardIDs []int, now time.Time, limit int) ([]*Transaction, bool) {
	if m == nil || limit <= 0 {
		return nil, false
	}
	out := make([]*Transaction, 0, limit)
	more := false
	for _, shardID := range m.rotatingShardIDs(shardIDs, &m.timeoutShardCursor) {
		s := &m.shards[shardID]
		s.mu.Lock()
		popped := make([]*deadlineItem, 0, limit+1)
		for len(s.deadlines) > 0 && !now.Before(s.deadlines[0].deadline) {
			item := heap.Pop(&s.deadlines).(*deadlineItem)
			popped = append(popped, item)
			if len(out) == limit {
				more = true
				break
			}
			if tx := s.txns[item.id]; tx != nil && tx.State == StateOpen && !tx.Expired {
				out = append(out, clone(tx))
			}
		}
		for _, item := range popped {
			heap.Push(&s.deadlines, item)
		}
		s.mu.Unlock()
		if more {
			break
		}
	}
	return out, more
}
