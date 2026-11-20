package sdk

import "sort"

// ConsumerMembership is an immutable snapshot of the consumer's active group
// ownership. Ready is true only while at least one partition is assigned by a
// completed JOIN_GROUP/SYNC_GROUP cycle.
type ConsumerMembership struct {
	Ready                bool
	Generation           int64
	AssignmentGeneration uint64
	MemberID             string
	Partitions           []int
}

// Membership returns a concurrency-safe copy of the active membership.
func (c *Consumer) Membership() ConsumerMembership {
	if c == nil {
		return ConsumerMembership{}
	}
	c.membershipMu.RLock()
	snapshot := c.membership
	snapshot.Partitions = append([]int(nil), c.membership.Partitions...)
	c.membershipMu.RUnlock()
	return snapshot
}

func (c *Consumer) publishMembership(generation int64, memberID string, assignmentGeneration uint64, partitions []int) {
	if c == nil {
		return
	}
	assigned := append([]int(nil), partitions...)
	sort.Ints(assigned)
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.membershipMu.Lock()
	defer c.membershipMu.Unlock()
	if c.State() != ConsumerStateRunning || c.assignmentGeneration.Load() != assignmentGeneration {
		return
	}
	if generation <= 0 || memberID == "" || assignmentGeneration == 0 || len(assigned) == 0 {
		c.membership = ConsumerMembership{}
		return
	}
	c.membership = ConsumerMembership{
		Ready:                true,
		Generation:           generation,
		AssignmentGeneration: assignmentGeneration,
		MemberID:             memberID,
		Partitions:           assigned,
	}
}

func (c *Consumer) clearMembership() {
	if c == nil {
		return
	}
	c.membershipMu.Lock()
	c.membership = ConsumerMembership{}
	c.membershipMu.Unlock()
}

func (c *Consumer) clearMembershipIfCurrent(generation int64, memberID string, assignmentGeneration uint64) {
	if c == nil {
		return
	}
	c.membershipMu.Lock()
	defer c.membershipMu.Unlock()
	current := c.membership
	if current.Generation != generation || current.MemberID != memberID ||
		current.AssignmentGeneration != assignmentGeneration {
		return
	}
	c.membership = ConsumerMembership{}
}
