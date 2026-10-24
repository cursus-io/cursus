package coordinator

import (
	"fmt"
	"github.com/cursus-io/cursus/pkg/protocol"
	"math"
	"slices"
)

type CooperativePartition = protocol.CooperativePartition
type CooperativeMember = protocol.CooperativeMember
type CooperativeGrant = protocol.CooperativeGrant
type CooperativeRevocation = protocol.CooperativeRevocation
type CooperativeTarget = protocol.CooperativeTarget

// CooperativeAssignment is a candidate state machine, not an installed lease.
// Persist a returned candidate before publishing its grants or fencing tokens.
// Methods never mutate the input, and callers serialize transitions with the
// authoritative transaction-reservation state supplied as blocked partitions.
type CooperativeAssignment struct {
	RegistrationEpoch uint64                 `json:"registration_epoch"`
	Revision          uint64                 `json:"revision"`
	TargetRevision    uint64                 `json:"target_revision"`
	Partitions        []CooperativePartition `json:"partitions"`
	Members           []CooperativeMember    `json:"members"`
	Target            []CooperativeTarget    `json:"target"`
	Current           []CooperativeGrant     `json:"current"`
}

func NewCooperativeAssignment(epoch uint64, partitions []CooperativePartition) (*CooperativeAssignment, error) {
	s := &CooperativeAssignment{RegistrationEpoch: epoch, Revision: 1, TargetRevision: 1, Partitions: slices.Clone(partitions)}
	s.canonicalize()
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *CooperativeAssignment) Clone() *CooperativeAssignment {
	if s == nil {
		return nil
	}
	clone := *s
	clone.Partitions = slices.Clone(s.Partitions)
	clone.Members = slices.Clone(s.Members)
	clone.Target = slices.Clone(s.Target)
	clone.Current = slices.Clone(s.Current)
	return &clone
}

func (s *CooperativeAssignment) next() (*CooperativeAssignment, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if s.Revision == math.MaxUint64 {
		return nil, fmt.Errorf("cooperative assignment revision exhausted")
	}
	next := s.Clone()
	next.Revision++
	return next, nil
}

func (s *CooperativeAssignment) Join(member string, blocked []CooperativePartition) (*CooperativeAssignment, error) {
	next, err := s.next()
	if err != nil {
		return nil, err
	}
	if member == "" || s.session(member) != 0 {
		return nil, fmt.Errorf("cooperative member must be nonempty and new")
	}
	next.Members = append(next.Members, CooperativeMember{ID: member, Session: next.Revision})
	next.plan(blocked)
	return next.checked()
}

// Remove fences exact sessions, including a batch that expires together.
// A stale session rejects the entire batch, rather than removing a newer member.
func (s *CooperativeAssignment) Remove(members []CooperativeMember, blocked []CooperativePartition) (*CooperativeAssignment, error) {
	next, err := s.next()
	if err != nil {
		return nil, err
	}
	remove := make(map[string]bool, len(members))
	for _, member := range members {
		if member.Session == 0 || s.session(member.ID) != member.Session || remove[member.ID] {
			return nil, fmt.Errorf("invalid or fenced cooperative member %q", member.ID)
		}
		remove[member.ID] = true
	}
	if len(remove) == 0 {
		return s.Clone(), nil
	}
	next.Members = slices.DeleteFunc(next.Members, func(m CooperativeMember) bool { return remove[m.ID] })
	next.Current = slices.DeleteFunc(next.Current, func(g CooperativeGrant) bool { return remove[g.Member] })
	next.plan(blocked)
	return next.checked()
}

// SetPartitions is applied only at an authoritative topic lifecycle fence.
// Removing/recreating a topic invalidates its old grant before any new grant is
// published. The caller must not derive this input from a stale metadata view.
func (s *CooperativeAssignment) SetPartitions(partitions []CooperativePartition, blocked []CooperativePartition) (*CooperativeAssignment, error) {
	next, err := s.next()
	if err != nil {
		return nil, err
	}
	next.Partitions = slices.Clone(partitions)
	if err := validateCooperativePartitions(next.Partitions); err != nil {
		return nil, err
	}
	allowed := cooperativePartitionSet(partitions)
	// Lifecycle counters cannot go backwards for the same topic while it is
	// present. Removal/recreation must still be fenced by authoritative storage.
	oldEpochs := make(map[string]uint64)
	for _, p := range s.Partitions {
		oldEpochs[p.Topic] = p.LifecycleEpoch
	}
	for _, p := range partitions {
		if p.LifecycleEpoch < oldEpochs[p.Topic] {
			return nil, fmt.Errorf("cooperative topic lifecycle regressed")
		}
	}
	next.Current = slices.DeleteFunc(next.Current, func(g CooperativeGrant) bool { return !allowed[g.Partition] })
	next.plan(blocked)
	return next.checked()
}

// Acknowledge accepts only the original durable revocation key. Old/duplicate
// acknowledgements cannot remove a replacement grant, including a grant to the
// same member. The caller acknowledges only after its partition work quiesces.
func (s *CooperativeAssignment) Acknowledge(member string, session uint64, acks []CooperativeRevocation, blocked []CooperativePartition) (*CooperativeAssignment, error) {
	next, err := s.next()
	if err != nil {
		return nil, err
	}
	if session == 0 || s.session(member) != session {
		return nil, fmt.Errorf("fenced cooperative member %q", member)
	}
	seen := make(map[CooperativePartition]bool, len(acks))
	releases := make(map[CooperativeRevocation]bool, len(acks))
	for _, ack := range acks {
		if !validCooperativePartition(ack.Partition) || ack.GrantEpoch == 0 || ack.GrantEpoch >= ack.RevokeRevision || ack.RevokeRevision > s.Revision || seen[ack.Partition] {
			return nil, fmt.Errorf("invalid cooperative revocation acknowledgement")
		}
		seen[ack.Partition] = true
		releases[ack] = true
	}
	next.Current = slices.DeleteFunc(next.Current, func(g CooperativeGrant) bool {
		return g.Member == member && g.Session == session && releases[CooperativeRevocation{Partition: g.Partition, GrantEpoch: g.Epoch, RevokeRevision: g.RevokeRevision}]
	})
	next.grantAvailable(blocked)
	if slices.Equal(next.Current, s.Current) {
		return s.Clone(), nil
	}
	return next.checked()
}

// Advance grants newly available targets after reservation resolution. It must
// use the same serialized reservation view as every other candidate transition.
func (s *CooperativeAssignment) Advance(blocked []CooperativePartition) (*CooperativeAssignment, error) {
	next, err := s.next()
	if err != nil {
		return nil, err
	}
	next.grantAvailable(blocked)
	if slices.Equal(next.Current, s.Current) {
		return s.Clone(), nil
	}
	return next.checked()
}

// ValidateGrant checks all identities, even while a revocation is pending. A
// pending revocation still belongs to the old owner until the durable release.
func (s *CooperativeAssignment) ValidateGrant(registration uint64, grant CooperativeGrant) error {
	if s == nil || registration == 0 || registration != s.RegistrationEpoch || grant.Session == 0 || s.session(grant.Member) != grant.Session {
		return fmt.Errorf("fenced cooperative ownership")
	}
	for _, current := range s.Current {
		if current.Partition == grant.Partition && current.Member == grant.Member && current.Session == grant.Session && current.Epoch == grant.Epoch {
			return nil
		}
	}
	return fmt.Errorf("fenced cooperative ownership")
}

func (s *CooperativeAssignment) session(member string) uint64 {
	for _, m := range s.Members {
		if m.ID == member {
			return m.Session
		}
	}
	return 0
}

func cooperativePartitionSet(partitions []CooperativePartition) map[CooperativePartition]bool {
	set := make(map[CooperativePartition]bool, len(partitions))
	for _, p := range partitions {
		set[p] = true
	}
	return set
}

func (s *CooperativeAssignment) plan(blocked []CooperativePartition) {
	s.canonicalize()
	members := make([]string, 0, len(s.Members))
	for _, member := range s.Members {
		members = append(members, member.ID)
	}
	previous := make(map[string][]CooperativePartition)
	for _, target := range s.Target {
		previous[target.Member] = append(previous[target.Member], target.Partition)
	}
	assigned := stickyAssignments(s.Partitions, members, previous)
	s.Target = nil
	s.TargetRevision = s.Revision
	wanted := make(map[CooperativePartition]string, len(s.Partitions))
	for _, member := range members {
		for _, p := range assigned[member] {
			s.Target = append(s.Target, CooperativeTarget{Partition: p, Member: member})
			wanted[p] = member
		}
	}
	for i := range s.Current {
		g := &s.Current[i]
		if g.RevokeRevision == 0 && wanted[g.Partition] != g.Member {
			g.RevokeRevision = s.Revision
		}
	}
	s.grantAvailable(blocked)
	s.canonicalize()
}

func (s *CooperativeAssignment) grantAvailable(blocked []CooperativePartition) {
	unavailable := cooperativePartitionSet(blocked)
	for _, grant := range s.Current {
		unavailable[grant.Partition] = true
	}
	for _, target := range s.Target {
		if !unavailable[target.Partition] {
			s.Current = append(s.Current, CooperativeGrant{Partition: target.Partition, Member: target.Member, Session: s.session(target.Member), Epoch: s.Revision})
		}
	}
	s.canonicalize()
}

func (s *CooperativeAssignment) checked() (*CooperativeAssignment, error) {
	s.canonicalize()
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}
