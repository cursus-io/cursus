package coordinator

import (
	"sort"
	"time"
)

// Historical GROUP_SYNC records encode membership changes, not the resulting
// assignment. Replaying them must use their original range algorithm so that
// later fenced offset records see exactly the original ownership. Modern
// lifecycle records persist the complete assignment and do not run this path.
func (mode lifecycleWriteMode) rebalance(group *GroupMetadata) {
	if mode == publishLifecycle {
		rebalanceGroup(group)
		return
	}
	members := make([]string, 0, len(group.Members))
	for id := range group.Members {
		members = append(members, id)
	}
	sort.Strings(members)
	group.LastActivity = time.Now()
	group.LastRebalance = group.LastActivity
	if len(members) == 0 {
		return
	}
	count := len(group.Partitions)
	if len(group.TopicPartitions) > 0 {
		count = len(group.TopicPartitions)
	}
	start := 0
	for i, id := range members {
		size := count / len(members)
		if i < count%len(members) {
			size++
		}
		member := group.Members[id]
		if len(group.TopicPartitions) > 0 {
			member.Assignments = nil
			member.TopicAssignments = append([]TopicPartition(nil), group.TopicPartitions[start:start+size]...)
		} else {
			member.Assignments = append([]int(nil), group.Partitions[start:start+size]...)
		}
		start += size
	}
}
