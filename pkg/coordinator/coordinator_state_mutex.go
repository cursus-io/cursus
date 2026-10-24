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

func (m *coordinatorStateMutex) Lock() {
	m.commits.Lock()
	m.state.Lock()
}

func (m *coordinatorStateMutex) Unlock() {
	m.state.Unlock()
	m.commits.Unlock()
}

func (m *coordinatorStateMutex) RLock()         { m.state.RLock() }
func (m *coordinatorStateMutex) RUnlock()       { m.state.RUnlock() }
func (m *coordinatorStateMutex) TryRLock() bool { return m.state.TryRLock() }

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
