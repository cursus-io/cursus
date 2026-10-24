package coordinator

import "sync"

// coordinatorStateMutex separates short state access from the exclusion needed
// by an in-flight cooperative offset append. State mutations acquire commits
// before state; ordinary readers only acquire state. Cooperative commits retain
// commits.RLock across I/O, releasing state.RLock so a validated unchanged
// heartbeat can update volatile clocks. Lifecycle publication still waits for
// those commits, including retained grants during a pending handoff.
//
// Only unchanged heartbeats and FSM restore may lock state directly. Heartbeats
// cannot change membership; restore fences detached commit completion by import
// generation and group identity, without waiting for an append needing Raft.
type coordinatorStateMutex struct {
	commits sync.RWMutex
	state   sync.RWMutex
}

// Lock excludes cooperative appends before acquiring exclusive state access.
func (m *coordinatorStateMutex) Lock() {
	m.commits.Lock()
	m.state.Lock()
}

// Unlock releases state access before allowing another cooperative append.
func (m *coordinatorStateMutex) Unlock() {
	m.state.Unlock()
	m.commits.Unlock()
}

// RLock protects state reads without excluding in-flight cooperative appends.
func (m *coordinatorStateMutex) RLock() { m.state.RLock() }

// RUnlock releases state read access; it does not release a commit fence.
func (m *coordinatorStateMutex) RUnlock() { m.state.RUnlock() }

// TryRLock acquires state read access only if immediately available.
func (m *coordinatorStateMutex) TryRLock() bool { return m.state.TryRLock() }

// TryLock acquires both mutation fences without waiting, releasing commit
// exclusion if the state lock is unavailable.
func (m *coordinatorStateMutex) TryLock() bool {
	if !m.commits.TryLock() {
		return false
	}
	if !m.state.TryLock() {
		m.commits.Unlock()
		return false
	}
	return true
}
