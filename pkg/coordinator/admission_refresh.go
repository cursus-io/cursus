package coordinator

import "slices"

// sameConsumerSession compares lifecycle identity, excluding offsets and
// reservations, which may advance independently. Call with the installed group
// locked and the replayed group still private to the publication operation.
func sameConsumerSession(installed, replayed *GroupMetadata) bool {
	if installed.RegistrationEpoch != replayed.RegistrationEpoch ||
		installed.RegistrationInferred != replayed.RegistrationInferred ||
		installed.Generation != replayed.Generation ||
		installed.TopicName != replayed.TopicName ||
		installed.TopicPattern != replayed.TopicPattern ||
		!installed.LastRebalance.Equal(replayed.LastRebalance) ||
		!slices.Equal(installed.Topics, replayed.Topics) ||
		!slices.Equal(installed.Partitions, replayed.Partitions) ||
		!slices.Equal(installed.TopicPartitions, replayed.TopicPartitions) ||
		len(installed.Members) != len(replayed.Members) {
		return false
	}
	for id, member := range installed.Members {
		other, ok := replayed.Members[id]
		if !ok || member == nil || other == nil || member.ID != other.ID ||
			!slices.Equal(member.Assignments, other.Assignments) ||
			!slices.Equal(member.TopicAssignments, other.TopicAssignments) {
			return false
		}
	}
	return true
}
