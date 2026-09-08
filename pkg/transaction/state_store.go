package transaction

import "fmt"

type ShardStateWriter interface {
	Persist(shardID int, snap *Snapshot) error
}

type ShardStateLoader interface {
	Load(shardIDs []int) (map[string]*Snapshot, error)
}

// ShardStateStore is the persistence boundary for transaction coordinator
// state. Implementations may share one physical log today and split shards
// across independently replicated logs without changing Manager callers.
type ShardStateStore interface {
	ShardStateWriter
	ShardStateLoader
}

type ShardStateWriterFunc func(shardID int, snap *Snapshot) error

func (fn ShardStateWriterFunc) Persist(shardID int, snap *Snapshot) error {
	return fn(shardID, snap)
}

// JournalShardStore adapts the backwards-compatible standalone journal to the
// shard-aware persistence boundary. The journal remains a single file.
type JournalShardStore struct {
	journal    *Journal
	shardCount int
}

func NewJournalShardStore(journal *Journal, shardCount int) (*JournalShardStore, error) {
	if journal == nil {
		return nil, fmt.Errorf("transaction journal is nil")
	}
	if shardCount <= 0 {
		return nil, fmt.Errorf("transaction shard count must be positive")
	}
	return &JournalShardStore{journal: journal, shardCount: shardCount}, nil
}

func (s *JournalShardStore) Persist(shardID int, snap *Snapshot) error {
	if snap == nil || snap.ID == "" {
		return fmt.Errorf("invalid transaction snapshot")
	}
	if shardID < 0 || shardID >= s.shardCount {
		return fmt.Errorf("transaction shard %d is outside [0,%d)", shardID, s.shardCount)
	}
	actual := CoordinatorShardForCount(snap.ID, s.shardCount)
	if actual != shardID {
		return fmt.Errorf("transaction %s belongs to shard %d, not %d", snap.ID, actual, shardID)
	}
	return s.journal.Append(snap)
}

func (s *JournalShardStore) Load(shardIDs []int) (map[string]*Snapshot, error) {
	state, err := s.journal.Load()
	if err != nil {
		return nil, err
	}
	if shardIDs == nil {
		return state, nil
	}
	selected := make(map[int]struct{}, len(shardIDs))
	for _, shardID := range shardIDs {
		if shardID < 0 || shardID >= s.shardCount {
			return nil, fmt.Errorf("transaction shard %d is outside [0,%d)", shardID, s.shardCount)
		}
		selected[shardID] = struct{}{}
	}
	filtered := make(map[string]*Snapshot)
	for id, snap := range state {
		if _, ok := selected[CoordinatorShardForCount(id, s.shardCount)]; ok {
			filtered[id] = snap
		}
	}
	return filtered, nil
}
