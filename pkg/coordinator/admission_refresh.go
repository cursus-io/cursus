package coordinator

import "slices"

// sameConsumerSession compares lifecycle identity, excluding offsets and
// reservations, which may advance independently. Call with the installed group
// locked and the replayed group still private to the publication operation.
func sameConsumerSession(installed, replayed *GroupMetadata) bool {
	if installed.RegistrationEpoch != replayed.RegistrationEpoch ||
		installed.RegistrationInferred != replayed.RegistrationInferred ||
		installed.Generation != replayed.Generation ||
		!sameCooperativeAssignment(installed.Cooperative, replayed.Cooperative) ||
		installed.TopicName != replayed.TopicName ||
		installed.TopicPattern != replayed.TopicPattern ||
		!installed.LastRebalance.Equal(replayed.LastRebalance) ||
		!slices.Equal(installed.Topics, replayed.Topics) ||
		!sameSessionItems(installed.Partitions, replayed.Partitions) ||
		!sameSessionItems(installed.TopicPartitions, replayed.TopicPartitions) ||
		len(installed.Members) != len(replayed.Members) {
		return false
	}
	for id, member := range installed.Members {
		other, ok := replayed.Members[id]
		if !ok || member == nil || other == nil || member.ID != other.ID ||
			!sameSessionItems(member.Assignments, other.Assignments) ||
			!sameSessionItems(member.TopicAssignments, other.TopicAssignments) {
			return false
		}
	}
	return true
}

// Snapshot import permits partition/assignment permutations, while durable
// replay canonicalizes them. Compare membership without mutating either state;
// retain multiplicity checks so malformed duplicates cannot hide a difference.
func sameSessionItems[T comparable](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}
	if slices.Equal(left, right) {
		return true
	}
	counts := make(map[T]int, len(left))
	for _, item := range left {
		counts[item]++
	}
	for _, item := range right {
		if counts[item] == 0 {
			return false
		}
		counts[item]--
	}
	return true
}
