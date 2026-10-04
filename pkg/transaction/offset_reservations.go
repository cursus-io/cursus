package transaction

import (
	"fmt"
	"maps"
)

// BeginOffsetReservations freezes the staged payload before the first group
// coordinator request. A lost response must not allow the client to change the
// set of inputs that a delayed request may still reserve.
func (m *Manager) BeginOffsetReservations(id, producer string, epoch int64) (*Transaction, error) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := s.txns[id]
	if tx == nil {
		return nil, fmt.Errorf("transaction %s not found", id)
	}
	if err := validateOwner(tx, producer, epoch); err != nil {
		return nil, err
	}
	if tx.State != StateOpen || tx.Mode != ModeProcessingV1 || len(tx.Offsets) == 0 {
		return nil, fmt.Errorf("transaction %s cannot begin offset reservations", id)
	}
	if !tx.OffsetReservationsPending {
		tx.OffsetReservationsPending = true
		tx.Revision++
		s.reindex(tx)
	}
	return clone(tx), nil
}

// BuildOffsetReservationsResolvedSnapshot is deterministic so a lost append or
// Raft response can retry exactly the same checkpoint. The live manager remains
// recovery-pending until the caller durably applies the returned snapshot.
func (m *Manager) BuildOffsetReservationsResolvedSnapshot(id string) (*Snapshot, error) {
	s := m.shardForID(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := s.txns[id]
	if tx == nil || (tx.State != StateCommitted && tx.State != StateAborted) {
		return nil, fmt.Errorf("transaction %s has no terminal decision", id)
	}
	return offsetReservationsResolvedSnapshot(tx), nil
}

func offsetReservationsResolvedSnapshot(tx *Transaction) *Snapshot {
	result := snapshot(tx)
	if !tx.OffsetReservationsPending {
		return result
	}
	result.OffsetReservationsPending = false
	result.Offsets = nil
	result.OffsetsMaterialized = tx.State == StateCommitted
	result.Revision++
	return result
}

// IsOffsetReservationCleanupSnapshot authorizes only the deterministic cleanup
// of a locally applied terminal decision, or its exact retry. Ownership may
// change after commit without changing the epoch of already durable markers.
// The caller must separately fence the request against the current owner.
func (m *Manager) IsOffsetReservationCleanupSnapshot(incoming *Snapshot) bool {
	if incoming == nil || incoming.Mode != ModeProcessingV1 || incoming.OffsetReservationsPending || len(incoming.Offsets) != 0 ||
		(incoming.State != StateCommitted && incoming.State != StateAborted) || incoming.OffsetsMaterialized != (incoming.State == StateCommitted) {
		return false
	}
	s := m.shardForID(incoming.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	tx := s.txns[incoming.ID]
	if tx == nil || tx.Mode != ModeProcessingV1 || tx.Expired || tx.Ready || tx.State != incoming.State {
		return false
	}
	expected := offsetReservationsResolvedSnapshot(tx)
	return snapshotsEqual(transactionFromSnapshot(expected), incoming) &&
		maps.Equal(expected.SequenceByPartition, incoming.SequenceByPartition) &&
		maps.Equal(expected.RequestAssignments, incoming.RequestAssignments)
}
