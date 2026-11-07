package coordinator

import (
	"fmt"
	"slices"
	"time"
)

// ValidateCooperativeSubscription checks the wire v1 subscription without
// exporting offsets or waiting for their in-flight durable append.
func (c *Coordinator) ValidateCooperativeSubscription(groupName, topic string) error {
	release, err := c.admitConsumerOperation()
	if err != nil {
		return err
	}
	defer release()
	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]
	if group == nil {
		return fmt.Errorf("ERROR: group_not_found")
	}
	if group.TopicName != topic || len(group.Topics) != 0 || group.TopicPattern != "" {
		return fmt.Errorf("ERROR: invalid_cooperative_payload")
	}
	return nil
}

// refreshCooperativeHeartbeat updates only volatile liveness for an unchanged
// durable assignment. A surviving session may refresh during a pending lifecycle
// append, but receives no grants or revocation acknowledgement until publication.
func (c *Coordinator) refreshCooperativeHeartbeat(groupName, member string, registration, session uint64, partitions []CooperativePartition, hasAcks bool) (CooperativeMembership, bool, error) {
	c.mu.state.Lock()
	defer c.mu.state.Unlock()
	group := c.groups[groupName]
	if group == nil || group.Cooperative == nil {
		return CooperativeMembership{}, false, nil
	}
	state := group.Cooperative
	if member == "" || registration != group.RegistrationEpoch || state.session(member) != session || group.Members[member] == nil {
		return CooperativeMembership{}, true, fmt.Errorf("ERROR: cooperative_fenced")
	}
	if c.lifecyclePending[groupName] {
		pending := c.cooperativePending[groupName]
		if pending != nil && pending.RegistrationEpoch == registration && pending.session(member) == session {
			now := time.Now()
			group.Members[member].LastHeartbeat = now
			group.LastActivity = now
		}
		// A removal already selected for publication cannot be undone by a late
		// heartbeat. An ambiguous append has no candidate and cannot refresh.
		return CooperativeMembership{}, true, fmt.Errorf("ERROR: coordinator_not_available")
	}
	if hasAcks {
		return CooperativeMembership{}, false, nil
	}
	if !slices.Equal(state.Partitions, partitions) {
		return CooperativeMembership{}, false, nil
	}
	// With no blocked targets this is the largest possible transition. Only
	// when even that changes nothing can we skip reading reservation state.
	next, err := state.Acknowledge(member, session, nil, nil)
	if err != nil || !sameCooperativeAssignment(state, next) {
		return CooperativeMembership{}, false, nil
	}
	now := time.Now()
	group.Members[member].LastHeartbeat = now
	group.LastActivity = now
	return cooperativeMembership(state, member), true, nil
}
