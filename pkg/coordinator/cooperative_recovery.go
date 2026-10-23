package coordinator

import (
	"errors"
	"fmt"
)

var errCooperativeProtocolRequired = errors.New("ERROR: cooperative_protocol_required")

func cooperativeRevision(group *GroupMetadata) uint64 {
	if group == nil || group.Cooperative == nil {
		return 0
	}
	return group.Cooperative.Revision
}

func validateCooperativeSnapshot(snapshot *GroupStateSnapshot) error {
	state := snapshot.Cooperative
	if state == nil {
		return nil
	}
	if err := state.Validate(); err != nil {
		return err
	}
	if snapshot.LastActivity.IsZero() {
		return fmt.Errorf("cooperative ownership is missing last activity")
	}
	if snapshot.RegistrationInferred || state.RegistrationEpoch != snapshot.RegistrationEpoch || snapshot.Generation < 0 || state.Revision <= uint64(snapshot.Generation) {
		return fmt.Errorf("cooperative ownership has inconsistent registration or revision")
	}
	declared := make(map[TopicPartition]bool)
	isSubscription := len(snapshot.Topics) != 0 || snapshot.TopicPattern != "" || len(snapshot.TopicPartitions) != 0
	if isSubscription {
		for _, p := range snapshot.TopicPartitions {
			declared[p] = true
		}
	} else {
		for _, p := range snapshot.Partitions {
			declared[TopicPartition{Topic: snapshot.TopicName, Partition: p}] = true
		}
	}
	if len(declared) != len(state.Partitions) || len(snapshot.Members) != len(state.Members) {
		return fmt.Errorf("cooperative ownership differs from group topology or membership")
	}
	for _, p := range state.Partitions {
		if !declared[TopicPartition{Topic: p.Topic, Partition: p.Partition}] {
			return fmt.Errorf("cooperative ownership references an undeclared partition")
		}
	}
	legacy := make(map[string][]int)
	subscriptions := make(map[string][]TopicPartition)
	for _, grant := range state.Current {
		if isSubscription {
			subscriptions[grant.Member] = append(subscriptions[grant.Member], TopicPartition{Topic: grant.Partition.Topic, Partition: grant.Partition.Partition})
		} else {
			legacy[grant.Member] = append(legacy[grant.Member], grant.Partition.Partition)
		}
	}
	for _, member := range state.Members {
		assignments, exists := snapshot.Members[member.ID]
		if !exists || !sameSessionItems(assignments, legacy[member.ID]) || !sameSessionItems(snapshot.TopicAssignments[member.ID], subscriptions[member.ID]) {
			return fmt.Errorf("cooperative current ownership differs from member assignment")
		}
	}
	return nil
}

func validateCooperativeLifecycle(epoch uint64, lifecycle *GroupLifecycleSnapshot) error {
	snapshot := &GroupStateSnapshot{
		RegistrationEpoch: epoch, Generation: lifecycle.Generation, Cooperative: lifecycle.Cooperative,
		TopicName: lifecycle.TopicName, Topics: lifecycle.Topics, TopicPattern: lifecycle.TopicPattern,
		TopicPartitions: lifecycle.TopicPartitions, Partitions: lifecycle.Partitions,
		LastActivity: lifecycle.LastActivity,
		Members:      make(map[string][]int), TopicAssignments: make(map[string][]TopicPartition),
	}
	for _, member := range lifecycle.Members {
		snapshot.Members[member.ID] = member.Assignments
		snapshot.TopicAssignments[member.ID] = member.TopicAssignments
	}
	return validateCooperativeSnapshot(snapshot)
}

func sameCooperativeAssignment(a, b *CooperativeAssignment) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.RegistrationEpoch == b.RegistrationEpoch && a.Revision == b.Revision && a.TargetRevision == b.TargetRevision &&
		sameSessionItems(a.Partitions, b.Partitions) && sameSessionItems(a.Members, b.Members) &&
		sameSessionItems(a.Target, b.Target) && sameSessionItems(a.Current, b.Current)
}

// Version-6 records must advance past the eager generation and cannot be
// downgraded within a registration. Apply this independent of scan order.
func validateCooperativeRecordOrder(a, b ConsumerMetadataRecord) error {
	if a.Type != ConsumerMetadataRecordLifecycleSnapshot || b.Type != ConsumerMetadataRecordLifecycleSnapshot {
		return nil
	}
	aCooperative := a.Version == ConsumerMetadataRecordVersionCooperative
	bCooperative := b.Version == ConsumerMetadataRecordVersionCooperative
	if aCooperative == bCooperative {
		return nil
	}
	cooperative, eager := a, b
	if !aCooperative {
		cooperative, eager = b, a
	}
	if eager.Revision >= cooperative.Revision || cooperative.Lifecycle == nil || cooperative.Lifecycle.Generation < 0 || eager.Revision > uint64(cooperative.Lifecycle.Generation) {
		return fmt.Errorf("eager lifecycle overlaps cooperative ownership for group %q", a.Group)
	}
	return nil
}
