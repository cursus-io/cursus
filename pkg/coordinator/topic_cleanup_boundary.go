package coordinator

import "fmt"

// TopicCleanupBoundary is a Raft-ordered cut of the consumer metadata log at
// truncate commit. Epochs on opposite sides of this structure are NOT comparable.
// Empty Ends means the historical cut is unknown, never an empty log.
type TopicCleanupBoundary struct {
	TopicInstance    string
	TopicEpoch       uint64
	MetadataInstance string
	MetadataEpoch    uint64
	Ends             []uint64
}

func (b *TopicCleanupBoundary) Clone() *TopicCleanupBoundary {
	if b == nil {
		return nil
	}
	c := *b
	c.Ends = append([]uint64(nil), b.Ends...)
	return &c
}

func (b *TopicCleanupBoundary) validateView(partitions []ConsumerCleanupPartition) error {
	if b == nil || len(b.Ends) == 0 {
		return nil
	}
	if b.TopicEpoch == 0 || b.MetadataEpoch == 0 || len(b.Ends) != len(partitions) {
		return fmt.Errorf("truncate consumer metadata boundary is invalid")
	}
	for i, p := range partitions {
		if p.TopicInstance != b.MetadataInstance || p.TopicEpoch != b.MetadataEpoch || p.Start != 0 || p.End < b.Ends[i] {
			return fmt.Errorf("truncate consumer metadata boundary history unavailable")
		}
	}
	return nil
}

func (scan *topicCleanupScan) preserves(target *cleanupTarget) bool {
	if scan.boundary == nil || len(scan.boundary.Ends) == 0 || target == nil {
		return false
	}
	e := target.evidence
	return target.singleTopic && e.topic == scan.status.Topic && e.kind == ConsumerMetadataRecordRegistration && e.epoch == target.epoch &&
		e.registrationAfter && e.epoch > e.beforeLive && e.liveEpoch <= e.epoch &&
		e.deletedEpoch > e.beforeLive && e.deletedEpoch < e.epoch
}

func (scan *topicCleanupScan) preservedTarget(name string) *cleanupTarget {
	if scan == nil {
		return nil
	}
	if target := scan.completed[name]; target != nil {
		return target
	}
	return scan.targets[name]
}

// Completed batches retain only proven post-truncate registrations. Active
// membership changes do not change that registration's position in the log.
func (c *Coordinator) pendingCleanupReferences(refs []TopicGroupReference, scan *topicCleanupScan, boundary *TopicCleanupBoundary) []TopicGroupReference {
	if scan == nil || boundary == nil {
		return refs
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]TopicGroupReference, 0, len(refs))
	for _, ref := range refs {
		target := scan.completed[ref.Name]
		group := c.groups[ref.Name]
		preserve := false
		if target != nil && group == target.group && group != nil && !c.lifecyclePending[ref.Name] {
			group.mu.RLock()
			preserve = group.RegistrationEpoch == target.epoch
			group.mu.RUnlock()
		}
		if !preserve {
			result = append(result, ref)
		}
	}
	return result
}

func (scan *topicCleanupScan) mayDelete(target *cleanupTarget) bool {
	if scan.boundary == nil {
		return true
	} // Legacy delete API, not truncate.
	return len(scan.boundary.Ends) != 0 && target != nil &&
		target.epoch != 0 && target.epoch <= target.evidence.beforeLive && !scan.preserves(target)
}
