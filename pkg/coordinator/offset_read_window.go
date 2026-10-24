package coordinator

import "sync"

// offsetReadWindow borrows the last acknowledged offsets only while an ordinary
// cooperative append holds group.mu. All mutations, including reservation
// creation, still wait for group.mu. Closing the window drains its readers
// before the offsets can change. It is neither a persistent cache nor a view of
// the candidate snapshot being appended.
type offsetReadWindow struct {
	mu      sync.RWMutex
	active  bool
	topic   string
	offsets map[int]uint64
}

// Caller holds group.mu and the cooperative membership fence throughout.
func (c *Coordinator) writeOffsetSnapshotWithReadWindow(group *GroupMetadata, groupName, topic string, revision uint64, items []OffsetItem) error {
	if group.Cooperative == nil || len(group.OffsetReservations) != 0 {
		return c.writeOffsetSnapshot(groupName, topic, group.RegistrationEpoch, revision, items)
	}
	w := &group.offsetReadWindow
	w.mu.Lock()
	w.topic, w.offsets, w.active = topic, group.Offsets[topic], true
	w.mu.Unlock()
	defer func() {
		// Wait for readers (including their transaction resolver lookup) before
		// publishing offsets or allowing a reservation/lifecycle mutation.
		w.mu.Lock()
		w.active, w.offsets = false, nil
		w.mu.Unlock()
	}()
	return c.writeOffsetSnapshot(groupName, topic, group.RegistrationEpoch, revision, items)
}
