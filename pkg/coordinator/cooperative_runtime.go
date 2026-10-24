package coordinator

import (
	"fmt"
	"github.com/cursus-io/cursus/pkg/protocol"
	"slices"
	"time"
)

type CooperativeMembership = protocol.CooperativeMembership

func cooperativeMembership(s *CooperativeAssignment, member string) CooperativeMembership {
	result := CooperativeMembership{RegistrationEpoch: s.RegistrationEpoch, Revision: s.Revision, Member: member, Session: s.session(member)}
	for _, grant := range s.Current {
		if grant.Member == member {
			result.Grants = append(result.Grants, grant)
		}
	}
	return result
}

// ReconcileCooperative serializes membership, topology and reservation barriers.
// A zero registration/session denotes an explicit join; other calls never rejoin
// a fenced session implicitly. partitions must come from authoritative metadata.
func (c *Coordinator) ReconcileCooperative(groupName, member string, registration, session uint64, partitions []CooperativePartition, acks []CooperativeRevocation, leave bool) (CooperativeMembership, error) {
	release, err := c.admitConsumerOperation()
	if err != nil {
		return CooperativeMembership{}, err
	}
	defer release()
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.mu.Lock()
	group := c.groups[groupName]
	if group == nil {
		c.mu.Unlock()
		return CooperativeMembership{}, fmt.Errorf("ERROR: group_not_found")
	}
	if member == "" || c.lifecyclePending[groupName] {
		c.mu.Unlock()
		return CooperativeMembership{}, fmt.Errorf("ERROR: coordinator_not_available")
	}
	candidate := cloneGroupLifecycle(group)
	state := candidate.Cooperative
	joining := registration == 0 && session == 0 && !leave && len(acks) == 0
	if state == nil {
		if !joining || len(group.Members) != 0 || group.RegistrationInferred {
			c.mu.Unlock()
			return CooperativeMembership{}, fmt.Errorf("ERROR: cooperative_protocol_required")
		}
		if group.Generation < 0 {
			c.mu.Unlock()
			return CooperativeMembership{}, fmt.Errorf("ERROR: invalid_generation")
		}
		state, err = NewCooperativeAssignment(group.RegistrationEpoch, partitions)
		if err == nil {
			state.Revision = uint64(group.Generation) + 1
			state.TargetRevision = state.Revision
		}
	} else if !joining && (registration != group.RegistrationEpoch || session == 0 || state.session(member) != session) {
		c.mu.Unlock()
		return CooperativeMembership{}, fmt.Errorf("ERROR: cooperative_fenced")
	}
	if err != nil {
		c.mu.Unlock()
		return CooperativeMembership{}, err
	}
	group.mu.RLock()
	blocked := cooperativeBlockedPartitions(group, partitions)
	group.mu.RUnlock()
	if !slices.Equal(state.Partitions, partitions) {
		state, err = state.SetPartitions(partitions, blocked)
	}
	if err == nil {
		switch {
		case joining:
			if state.session(member) == 0 {
				state, err = state.Join(member, blocked)
			}
		case leave:
			state, err = state.Remove([]CooperativeMember{{ID: member, Session: session}}, blocked)
		default:
			state, err = state.Acknowledge(member, session, acks, blocked)
		}
	}
	if err != nil {
		c.mu.Unlock()
		return CooperativeMembership{}, err
	}
	now := time.Now()
	if group.Cooperative != nil && sameCooperativeAssignment(group.Cooperative, state) {
		if m := group.Members[member]; m != nil {
			m.LastHeartbeat = now
			group.LastActivity = now
		}
		result := cooperativeMembership(state, member)
		c.mu.Unlock()
		return result, nil
	}
	installCooperativeCandidate(candidate, state, now)
	if m := candidate.Members[member]; m != nil {
		m.LastHeartbeat = now
	}
	if c.cooperativePending == nil {
		c.cooperativePending = make(map[string]*CooperativeAssignment)
	}
	c.cooperativePending[groupName] = state
	c.lifecyclePending[groupName] = true
	c.mu.Unlock()
	if err := c.persistAndApplyLifecycle(groupName, group, candidate, publishLifecycle); err != nil {
		return CooperativeMembership{}, err
	}
	return cooperativeMembership(state, member), nil
}

func cooperativeBlockedPartitions(group *GroupMetadata, partitions []CooperativePartition) []CooperativePartition {
	blocked := make(map[TopicPartition]bool)
	for _, reservation := range group.OffsetReservations {
		for _, offset := range reservation.Offsets {
			blocked[TopicPartition{Topic: offset.Topic, Partition: offset.Partition}] = true
		}
	}
	var result []CooperativePartition
	for _, p := range partitions {
		if blocked[TopicPartition{Topic: p.Topic, Partition: p.Partition}] {
			result = append(result, p)
		}
	}
	return result
}

func installCooperativeCandidate(candidate *GroupMetadata, state *CooperativeAssignment, now time.Time) {
	candidate.Cooperative = state
	members := make(map[string]*MemberMetadata, len(state.Members))
	for _, m := range state.Members {
		heartbeat := now
		if previous := candidate.Members[m.ID]; previous != nil {
			heartbeat = previous.LastHeartbeat
		}
		members[m.ID] = &MemberMetadata{ID: m.ID, LastHeartbeat: heartbeat}
	}
	for _, grant := range state.Current {
		m := members[grant.Member]
		if len(candidate.Topics) > 0 || candidate.TopicPattern != "" {
			m.TopicAssignments = append(m.TopicAssignments, TopicPartition{Topic: grant.Partition.Topic, Partition: grant.Partition.Partition})
		} else {
			m.Assignments = append(m.Assignments, grant.Partition.Partition)
		}
	}
	candidate.Members = members
	candidate.LastActivity = now
	candidate.LastRebalance = now
}

// expireCooperative rechecks session clocks after acquiring lifecycleMu. An old
// timeout scan cannot evict a member whose heartbeat arrived in the meantime.
func (c *Coordinator) expireCooperative(groupName string, timeout time.Duration) error {
	release, err := c.admitConsumerOperation()
	if err != nil {
		return err
	}
	defer release()
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.mu.Lock()
	group := c.groups[groupName]
	if group == nil || group.Cooperative == nil || c.lifecyclePending[groupName] {
		c.mu.Unlock()
		return nil
	}
	var expired []CooperativeMember
	now := time.Now()
	for _, member := range group.Cooperative.Members {
		if m := group.Members[member.ID]; m != nil && now.Sub(m.LastHeartbeat) > timeout {
			expired = append(expired, member)
		}
	}
	if len(expired) == 0 {
		c.mu.Unlock()
		return nil
	}
	group.mu.RLock()
	blocked := cooperativeBlockedPartitions(group, group.Cooperative.Partitions)
	group.mu.RUnlock()
	state, err := group.Cooperative.Remove(expired, blocked)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	candidate := cloneGroupLifecycle(group)
	installCooperativeCandidate(candidate, state, now)
	if c.cooperativePending == nil {
		c.cooperativePending = make(map[string]*CooperativeAssignment)
	}
	c.cooperativePending[groupName] = state
	c.lifecyclePending[groupName] = true
	c.mu.Unlock()
	return c.persistAndApplyLifecycle(groupName, group, candidate, publishLifecycle)
}

// ValidateCooperativeRead permits legacy stateless reads only for eager groups.
func (c *Coordinator) ValidateCooperativeRead(groupName string, registration uint64, grant *CooperativeGrant) error {
	release, err := c.admitConsumerOperation()
	if err != nil {
		return err
	}
	defer release()
	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]
	if group == nil || group.Cooperative == nil {
		if grant != nil {
			return fmt.Errorf("ERROR: cooperative_fenced")
		}
		return nil
	}
	if grant == nil {
		return errCooperativeProtocolRequired
	}
	if err := c.validateCooperativePendingLocked(groupName, registration, *grant); err != nil {
		return err
	}
	if err := group.Cooperative.ValidateGrant(registration, *grant); err != nil {
		return fmt.Errorf("ERROR: cooperative_fenced")
	}
	return nil
}

func (c *Coordinator) CommitCooperative(groupName string, registration uint64, grant CooperativeGrant, offset uint64) error {
	return c.CommitCooperativeBatch(groupName, []protocol.CooperativeToken{{RegistrationEpoch: registration, Grant: grant}}, []OffsetItem{{Partition: grant.Partition.Partition, Offset: offset}})
}

// CommitCooperativeBatch validates every original grant before publishing one
// durable topic snapshot. No offset is published if validation or append fails.
func (c *Coordinator) CommitCooperativeBatch(groupName string, tokens []protocol.CooperativeToken, offsets []OffsetItem) error {
	if len(tokens) == 0 || len(tokens) != len(offsets) || len(tokens) > protocol.MaxCooperativeCommitPartitions {
		return fmt.Errorf("ERROR: invalid_cooperative_payload")
	}
	release, err := c.admitConsumerOperation()
	if err != nil {
		return err
	}
	defer release()
	c.mu.RLock()
	defer c.mu.RUnlock()
	group := c.groups[groupName]
	if group == nil || group.Cooperative == nil {
		return fmt.Errorf("ERROR: cooperative_fenced")
	}
	first := tokens[0]
	seen := make(map[int]bool, len(tokens))
	for i, token := range tokens {
		grant := token.Grant
		if token.RegistrationEpoch != first.RegistrationEpoch || grant.Partition.Topic != first.Grant.Partition.Topic || grant.Member != first.Grant.Member || grant.Session != first.Grant.Session || grant.Partition.Partition != offsets[i].Partition || seen[offsets[i].Partition] {
			return fmt.Errorf("ERROR: invalid_cooperative_payload")
		}
		seen[offsets[i].Partition] = true
		if err := c.validateCooperativePendingLocked(groupName, token.RegistrationEpoch, grant); err != nil {
			return err
		}
		if err := group.Cooperative.ValidateGrant(token.RegistrationEpoch, grant); err != nil {
			return fmt.Errorf("ERROR: cooperative_fenced")
		}
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	return c.commitAuthorizedOffsetsBulkForGroupLocked(group, groupName, first.Grant.Partition.Topic, offsets)
}

// IsCooperativeGroup is a synchronized mode query for legacy admission.
func (c *Coordinator) IsCooperativeGroup(group string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	g := c.groups[group]
	return g != nil && g.Cooperative != nil
}

// Retained grants remain usable while a different partition's transition is
// being persisted. A failed/ambiguous append has no candidate and fences all.
func (c *Coordinator) validateCooperativePendingLocked(group string, registration uint64, grant CooperativeGrant) error {
	if !c.lifecyclePending[group] {
		return nil
	}
	pending := c.cooperativePending[group]
	if pending == nil || pending.ValidateGrant(registration, grant) != nil {
		return fmt.Errorf("ERROR: coordinator_not_available")
	}
	return nil
}
