package coordinator

import "fmt"

// reserveOffsetRevision requires group.mu. An append error does not prove that
// the record was absent, so an attempted revision cannot identify a new payload.
// Acknowledged offsets and revisions are published separately after success.
func reserveOffsetRevision(group *GroupMetadata, topic string) (uint64, error) {
	group.offsetRevisionMu.Lock()
	defer group.offsetRevisionMu.Unlock()
	revision := max(group.OffsetRevisions[topic], group.offsetWriteRevisions[topic])
	if revision == ^uint64(0) {
		return 0, fmt.Errorf("offset revision overflow")
	}
	if group.offsetWriteRevisions == nil {
		group.offsetWriteRevisions = make(map[string]uint64)
	}
	group.offsetWriteRevisions[topic] = revision + 1
	return revision + 1, nil
}

// preserveOffsetWriteRevisions requires exclusive ownership of next and the
// coordinator map lock. It preserves allocation history, never unacknowledged
// offsets, when replay or a snapshot replaces a live group in the same process.
func preserveOffsetWriteRevisions(previous, next *GroupMetadata) {
	if previous == nil || next == nil || previous == next {
		return
	}
	// Standalone writes may release the coordinator map lock while retaining
	// group.mu through a writer callback. Do not wait for that callback while
	// replacing the map; only the short allocation critical section is needed.
	previous.offsetRevisionMu.Lock()
	defer previous.offsetRevisionMu.Unlock()
	if previous.RegistrationEpoch != next.RegistrationEpoch {
		return
	}
	for topic, revision := range previous.offsetWriteRevisions {
		if next.offsetWriteRevisions == nil {
			next.offsetWriteRevisions = make(map[string]uint64)
		}
		next.offsetWriteRevisions[topic] = max(next.offsetWriteRevisions[topic], revision)
	}
}
