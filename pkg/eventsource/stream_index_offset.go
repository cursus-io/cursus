package eventsource

import (
	"fmt"
	"sort"
)

type offsetVersion struct{ offset, version, maximum uint64 }
type indexedStreamRecord struct {
	key   string
	entry StreamIndexEntry
}

// rememberEntryLocked keeps offset order independently of public Append order.
// The two ordered caches add O(n) metadata; ordinary increasing-offset appends
// update only the tail, while arbitrary public Append order may shift a suffix.
func (si *StreamIndex) rememberEntryLocked(key string, e StreamIndexEntry) {
	if si.offsetVersions == nil {
		si.offsetVersions = make(map[string][]offsetVersion)
	}
	points := si.offsetVersions[key]
	at := sort.Search(len(points), func(i int) bool { return points[i].offset >= e.Offset })
	points = append(points, offsetVersion{})
	copy(points[at+1:], points[at:])
	points[at] = offsetVersion{offset: e.Offset, version: e.AggregateVersion}
	for i := at; i < len(points); i++ {
		points[i].maximum = points[i].version
		if i > 0 {
			points[i].maximum = max(points[i].maximum, points[i-1].maximum)
		}
	}
	si.offsetVersions[key] = points
	at = sort.Search(len(si.orderedRecords), func(i int) bool { return si.orderedRecords[i].entry.Offset >= e.Offset })
	si.orderedRecords = append(si.orderedRecords, indexedStreamRecord{})
	copy(si.orderedRecords[at+1:], si.orderedRecords[at:])
	si.orderedRecords[at] = indexedStreamRecord{key, e}
	if si.certificate != nil && e.Offset < si.certificate.end {
		si.certificate = nil
	}
}

// VersionBefore includes only entries below the exclusive offset and the
// separately retained durable checkpoint. Snapshot callers must first certify
// these entries against the fixed raw/visible log cut.
func (si *StreamIndex) VersionBefore(key string, exclusiveOffset uint64) (uint64, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	if err := si.checkLocked(); err != nil {
		return 0, err
	}
	return si.versionBeforeLocked(key, exclusiveOffset), nil
}
func (si *StreamIndex) versionBeforeLocked(key string, end uint64) uint64 {
	points := si.offsetVersions[key]
	at := sort.Search(len(points), func(i int) bool { return points[i].offset >= end })
	v := uint64(0)
	if end >= si.seedAnchors[key] {
		v = si.seeds[key]
	}
	if at > 0 {
		v = max(v, points[at-1].maximum)
	}
	return v
}
func (si *StreamIndex) recordsBetween(start, end uint64) ([]indexedStreamRecord, error) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	if err := si.checkLocked(); err != nil {
		return nil, err
	}
	a := sort.Search(len(si.orderedRecords), func(i int) bool { return si.orderedRecords[i].entry.Offset >= start })
	b := sort.Search(len(si.orderedRecords), func(i int) bool { return si.orderedRecords[i].entry.Offset >= end })
	if b-a > 256 {
		return nil, fmt.Errorf("snapshot suffix index exceeds raw page record bound")
	}
	return append([]indexedStreamRecord(nil), si.orderedRecords[a:b]...), nil
}

// resetForSnapshot transfers ownership of the old ordered tail before reset.
// No complete ahead-tail copy is allocated; rebuild consumes one entry at a time.
func (si *StreamIndex) resetForSnapshot(end uint64) ([]indexedStreamRecord, error) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if err := si.checkLocked(); err != nil {
		return nil, err
	}
	at := sort.Search(len(si.orderedRecords), func(i int) bool { return si.orderedRecords[i].entry.Offset >= end })
	ahead := si.orderedRecords[at:]
	if err := si.resetLocked(); err != nil {
		return nil, err
	}
	return ahead, nil
}
