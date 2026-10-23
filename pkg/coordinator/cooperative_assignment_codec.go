package coordinator

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"slices"
)

func validCooperativePartition(p CooperativePartition) bool {
	return p.Topic != "" && p.Partition >= 0 && p.LifecycleEpoch != 0
}

func validateCooperativePartitions(partitions []CooperativePartition) error {
	seen := make(map[CooperativePartition]bool, len(partitions))
	epochs := make(map[string]uint64)
	for _, p := range partitions {
		if !validCooperativePartition(p) || seen[p] || (epochs[p.Topic] != 0 && epochs[p.Topic] != p.LifecycleEpoch) {
			return fmt.Errorf("invalid or duplicate cooperative partition")
		}
		seen[p], epochs[p.Topic] = true, p.LifecycleEpoch
	}
	return nil
}

// Validate checks persisted state without repairing it or granting ownership.
func (s *CooperativeAssignment) Validate() error {
	if s == nil || s.RegistrationEpoch == 0 || s.Revision == 0 || s.TargetRevision == 0 || s.TargetRevision > s.Revision {
		return fmt.Errorf("invalid cooperative assignment identity")
	}
	if err := validateCooperativePartitions(s.Partitions); err != nil {
		return err
	}
	partitions := cooperativePartitionSet(s.Partitions)
	members := make(map[string]uint64, len(s.Members))
	sessions := make(map[uint64]bool, len(s.Members))
	for _, m := range s.Members {
		if m.ID == "" || m.Session < 2 || m.Session > s.TargetRevision || members[m.ID] != 0 || sessions[m.Session] {
			return fmt.Errorf("invalid or duplicate cooperative member")
		}
		members[m.ID], sessions[m.Session] = m.Session, true
	}
	targets := make(map[CooperativePartition]string, len(s.Target))
	counts := make(map[string]int, len(members))
	for _, t := range s.Target {
		if !partitions[t.Partition] || members[t.Member] == 0 || targets[t.Partition] != "" {
			return fmt.Errorf("invalid or duplicate cooperative target")
		}
		targets[t.Partition] = t.Member
		counts[t.Member]++
	}
	if len(members) == 0 {
		if len(s.Target) != 0 || len(s.Current) != 0 {
			return fmt.Errorf("cooperative ownership without members")
		}
	} else {
		if len(targets) != len(partitions) {
			return fmt.Errorf("incomplete cooperative target")
		}
		floor := len(partitions) / len(members)
		ceiling := (len(partitions) + len(members) - 1) / len(members)
		for member := range members {
			if counts[member] < floor || counts[member] > ceiling {
				return fmt.Errorf("unbalanced cooperative target")
			}
		}
	}
	current := make(map[CooperativePartition]bool, len(s.Current))
	for _, g := range s.Current {
		if !partitions[g.Partition] || current[g.Partition] || g.Session == 0 || members[g.Member] != g.Session || g.Epoch < g.Session || g.Epoch > s.Revision {
			return fmt.Errorf("invalid or duplicate cooperative grant")
		}
		if g.RevokeRevision == 0 {
			if targets[g.Partition] != g.Member {
				return fmt.Errorf("cooperative grant differs from target without revocation")
			}
		} else if g.RevokeRevision <= g.Epoch || g.RevokeRevision > s.TargetRevision {
			return fmt.Errorf("invalid cooperative revocation revision")
		}
		current[g.Partition] = true
	}
	return nil
}

func compareCooperativePartition(a, b CooperativePartition) int {
	if n := cmp.Compare(a.Topic, b.Topic); n != 0 {
		return n
	}
	if n := cmp.Compare(a.LifecycleEpoch, b.LifecycleEpoch); n != 0 {
		return n
	}
	return cmp.Compare(a.Partition, b.Partition)
}

func (s *CooperativeAssignment) canonicalize() {
	slices.SortFunc(s.Partitions, compareCooperativePartition)
	slices.SortFunc(s.Members, func(a, b CooperativeMember) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(s.Target, func(a, b CooperativeTarget) int { return compareCooperativePartition(a.Partition, b.Partition) })
	slices.SortFunc(s.Current, func(a, b CooperativeGrant) int { return compareCooperativePartition(a.Partition, b.Partition) })
	if len(s.Partitions) == 0 {
		s.Partitions = nil
	}
	if len(s.Members) == 0 {
		s.Members = nil
	}
	if len(s.Target) == 0 {
		s.Target = nil
	}
	if len(s.Current) == 0 {
		s.Current = nil
	}
}

// EncodeCooperativeAssignment returns deterministic recovery bytes. It does not
// make the state durable or establish an authorization boundary on its own.
func EncodeCooperativeAssignment(s *CooperativeAssignment) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	canonical := s.Clone()
	canonical.canonicalize()
	return json.Marshal(canonical)
}

func DecodeCooperativeAssignment(data []byte) (*CooperativeAssignment, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var s CooperativeAssignment
	if err := decoder.Decode(&s); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("trailing cooperative assignment data")
	}
	return s.checked()
}
