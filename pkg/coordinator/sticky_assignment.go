package coordinator

import "slices"

// stickyAssignments balances a shared group subscription while retaining the
// maximum number of valid previous owners. partitions must be unique and in
// canonical order; members must be sorted. It never mutates its inputs.
// This computes assignment only, not permission to bypass generation fencing.
func stickyAssignments[P comparable](partitions []P, members []string, previous map[string][]P) map[string][]P {
	result := make(map[string][]P, len(members))
	if len(members) == 0 {
		return result
	}
	eligible := make(map[P]bool, len(partitions))
	for _, p := range partitions {
		eligible[p] = true
	}
	owners := make(map[P]string, len(partitions))
	counts := make(map[string]int, len(members))
	for _, id := range members {
		for _, p := range previous[id] {
			if _, claimed := owners[p]; eligible[p] && !claimed {
				owners[p] = id
				counts[id]++
			}
		}
	}

	// Giving an extra slot to a member already above the floor saves one move.
	// Stable ordering breaks ties by member ID, including a fresh assignment.
	priority := slices.Clone(members)
	slices.SortStableFunc(priority, func(a, b string) int {
		return counts[b] - counts[a]
	})
	floor, remainder := len(partitions)/len(members), len(partitions)%len(members)
	target := make(map[string]int, len(members))
	for i, id := range priority {
		target[id] = floor
		if i < remainder {
			target[id]++
		}
	}

	unassigned := make([]P, 0)
	for _, p := range partitions {
		id, owned := owners[p]
		if owned && len(result[id]) < target[id] {
			result[id] = append(result[id], p)
		} else {
			unassigned = append(unassigned, p)
		}
	}
	next := 0
	for _, id := range members {
		missing := target[id] - len(result[id])
		result[id] = append(result[id], unassigned[next:next+missing]...)
		next += missing
	}
	// Rebuild in canonical partition order so replay and a no-op rebalance
	// produce identical slices, even when newly assigned partitions sort first.
	for id, assigned := range result {
		for _, p := range assigned {
			owners[p] = id
		}
		result[id] = assigned[:0]
	}
	for _, p := range partitions {
		id := owners[p]
		result[id] = append(result[id], p)
	}
	return result
}
