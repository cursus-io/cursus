package transaction

import "math"

// CaptureTransactionDecisionGeneration captures process-local provenance for
// both transaction decision resolvers. Tokens belong to this Manager instance
// and must not be persisted or used with another manager. A false result means
// certification is unavailable. Payload and bookkeeping changes preserve tokens.
// This acquires every shard lock in order; do not call while holding a shard lock.
func (m *Manager) CaptureTransactionDecisionGeneration() (uint64, bool) {
	if m == nil {
		return 0, false
	}
	m.lockAllShards()
	defer m.unlockAllShards()
	generation := m.decisionGeneration.Load()
	return generation, generation != math.MaxUint64
}

// CheckTransactionDecisionGeneration reports whether a captured token still
// describes this manager's decision state. It waits for in-flight shard mutations
// to finish, but does not prevent mutations after it returns. Do not call while
// holding a shard lock. Generation exhaustion permanently fails closed.
func (m *Manager) CheckTransactionDecisionGeneration(generation uint64) bool {
	current, ok := m.CaptureTransactionDecisionGeneration()
	return ok && current == generation
}

// advanceDecisionGenerationLocked requires the affected shard lock (or all
// shard locks for state replacement). Writers on different shards can overlap.
func (m *Manager) advanceDecisionGenerationLocked() {
	for current := m.decisionGeneration.Load(); current != math.MaxUint64; current = m.decisionGeneration.Load() {
		if m.decisionGeneration.CompareAndSwap(current, current+1) {
			return
		}
	}
}

type decisionTuple struct {
	exists           bool
	epoch            int64
	expired          bool
	state            State
	coordinatorEpoch int64
}

func transactionDecisionTuple(tx *Transaction) decisionTuple {
	if tx == nil {
		return decisionTuple{}
	}
	return makeDecisionTuple(tx.Epoch, tx.Expired, tx.State, tx.Mode, tx.CoordinatorEpoch)
}

func snapshotDecisionTuple(snap *Snapshot) decisionTuple {
	if snap == nil {
		return decisionTuple{}
	}
	return makeDecisionTuple(snap.Epoch, snap.Expired, snap.State, snap.Mode, snap.CoordinatorEpoch)
}

func makeDecisionTuple(epoch int64, expired bool, state State, mode Mode, coordinatorEpoch int64) decisionTuple {
	if mode != ModeProcessingV1 {
		coordinatorEpoch = 0
	}
	return decisionTuple{exists: true, epoch: epoch, expired: expired, state: state, coordinatorEpoch: coordinatorEpoch}
}

// Deferred calls must run before the corresponding shard unlock, including
// error returns after expiration has already changed the live transaction.
func (m *Manager) recordDecisionChangeLocked(s *managerShard, id string, before decisionTuple) {
	if before != transactionDecisionTuple(s.txns[id]) {
		m.advanceDecisionGenerationLocked()
	}
}

// decisionStateMatchesLocked compares only resolver dependencies without cloning
// payloads. All shard locks must be held. Import and topic-reference pruning use
// the same replacement path, so bookkeeping-only restores retain certification.
func (m *Manager) decisionStateMatchesLocked(state map[string]*Snapshot) bool {
	for i := range m.shards {
		for id, tx := range m.shards[i].txns {
			if transactionDecisionTuple(tx) != snapshotDecisionTuple(state[id]) {
				return false
			}
		}
	}
	for id, snap := range state {
		if snapshotDecisionTuple(snap) != transactionDecisionTuple(m.shardForID(id).txns[id]) {
			return false
		}
	}
	return true
}
