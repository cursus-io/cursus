package coordinator

import (
	"fmt"
	"testing"
)

// Compare assignor work and movement only. This does not model application
// replay or imply uninterrupted processing: the eager SDK path is unchanged.
func BenchmarkStickyAssignmentJoin(b *testing.B) {
	for _, count := range []int{120, 12000} {
		partitions := makePartitions(count)
		members := []string{"a", "b", "c", "d"}
		previous := map[string][]int{"a": partitions[:count/3], "b": partitions[count/3 : count*2/3], "c": partitions[count*2/3:]}
		for _, sticky := range []bool{false, true} {
			b.Run(fmt.Sprintf("partitions=%d/sticky=%t", count, sticky), func(b *testing.B) {
				b.ReportAllocs()
				moved := 0
				for b.Loop() {
					group := &GroupMetadata{Partitions: partitions, Members: make(map[string]*MemberMetadata)}
					for _, id := range members {
						group.Members[id] = &MemberMetadata{Assignments: previous[id]}
					}
					var assigned map[string][]int
					if sticky {
						assigned = stickyAssignments(partitions, members, previous)
					} else {
						replayLifecycle.rebalance(group)
						assigned = make(map[string][]int)
						for id, member := range group.Members {
							assigned[id] = member.Assignments
						}
					}
					moved = 0
					for i, id := range members {
						for _, p := range assigned[id] {
							if p/(count/3) != i {
								moved++
							}
						}
					}
				}
				b.ReportMetric(float64(moved), "partitions-moved/op")
			})
		}
	}
}
