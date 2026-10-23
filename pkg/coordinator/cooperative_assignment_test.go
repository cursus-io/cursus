package coordinator

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func cooperativeFixture(t testing.TB, count int) *CooperativeAssignment {
	t.Helper()
	partitions := make([]CooperativePartition, count)
	for i := range partitions {
		partitions[i] = CooperativePartition{Topic: "orders", Partition: i, LifecycleEpoch: 7}
	}
	s, err := NewCooperativeAssignment(19, partitions)
	require.NoError(t, err)
	return s
}

func cooperativeJoin(t testing.TB, s *CooperativeAssignment, member string) *CooperativeAssignment {
	t.Helper()
	next, err := s.Join(member, nil)
	require.NoError(t, err)
	return next
}

func cooperativeAcks(s *CooperativeAssignment, member string) []CooperativeRevocation {
	var acks []CooperativeRevocation
	for _, g := range s.Current {
		if g.Member == member && g.RevokeRevision != 0 {
			acks = append(acks, CooperativeRevocation{Partition: g.Partition, GrantEpoch: g.Epoch, RevokeRevision: g.RevokeRevision})
		}
	}
	return acks
}

func TestCooperativeReleaseBeforeGrant(t *testing.T) {
	before := cooperativeJoin(t, cooperativeFixture(t, 6), "a")
	joining := cooperativeJoin(t, before, "b")
	require.Len(t, joining.Target, 6)
	require.Len(t, joining.Current, 6)
	acks := cooperativeAcks(joining, "a")
	require.Len(t, acks, 3)
	for _, grant := range before.Current {
		require.NoError(t, joining.ValidateGrant(19, grant), "old owner remains valid until release")
	}
	for _, grant := range joining.Current {
		require.Equal(t, "a", grant.Member, "target owner cannot receive an unreleased partition")
	}
	current, err := joining.Acknowledge("a", joining.session("a"), acks, nil)
	require.NoError(t, err)
	require.Empty(t, cooperativeAcks(current, "a"))
	for i, grant := range current.Current {
		if grant.Member == "a" {
			require.Equal(t, before.Current[i], grant, "retained tokens must not change")
		} else {
			require.Equal(t, "b", grant.Member)
			require.Error(t, current.ValidateGrant(19, before.Current[i]))
			require.NoError(t, current.ValidateGrant(19, grant))
		}
	}
	duplicate, err := current.Acknowledge("a", current.session("a"), acks, nil)
	require.NoError(t, err)
	require.Equal(t, current, duplicate)
	require.Equal(t, uint64(2), before.Revision, "candidate computation must not mutate installed state")
	require.Empty(t, cooperativeAcks(before, "a"))
}

func TestCooperativeTargetReversalKeepsOriginalRevocation(t *testing.T) {
	s := cooperativeJoin(t, cooperativeFixture(t, 4), "a")
	s = cooperativeJoin(t, s, "b")
	acks := cooperativeAcks(s, "a")
	old := s.Clone()
	s, err := s.Remove([]CooperativeMember{{ID: "b", Session: s.session("b")}}, nil)
	require.NoError(t, err)
	require.Equal(t, acks, cooperativeAcks(s, "a"))
	for _, target := range s.Target {
		require.Equal(t, "a", target.Member)
	}
	s, err = s.Acknowledge("a", s.session("a"), acks, nil)
	require.NoError(t, err)
	for _, grant := range old.Current {
		if grant.RevokeRevision != 0 {
			require.Error(t, s.ValidateGrant(19, grant), "same-owner return must issue a new grant")
		}
	}
	duplicate, err := s.Acknowledge("a", s.session("a"), acks, nil)
	require.NoError(t, err)
	require.Equal(t, s, duplicate)
}

func TestCooperativeSessionAndTopicFencing(t *testing.T) {
	s := cooperativeJoin(t, cooperativeFixture(t, 2), "a")
	old := s.Clone()
	s, err := s.Remove([]CooperativeMember{{ID: "a", Session: s.session("a")}}, nil)
	require.NoError(t, err)
	s = cooperativeJoin(t, s, "a")
	require.Greater(t, s.session("a"), old.session("a"))
	_, err = s.Remove([]CooperativeMember{{ID: "a", Session: old.session("a")}}, nil)
	require.Error(t, err)
	_, err = s.Acknowledge("a", old.session("a"), nil, nil)
	require.Error(t, err)
	for _, g := range old.Current {
		require.Error(t, s.ValidateGrant(19, g))
	}
	beforeTopic := s.Clone()
	partitions := slices.Clone(s.Partitions)
	for i := range partitions {
		partitions[i].LifecycleEpoch++
	}
	s, err = s.SetPartitions(partitions, nil)
	require.NoError(t, err)
	for _, g := range beforeTopic.Current {
		require.Error(t, s.ValidateGrant(19, g))
	}
	for _, g := range s.Current {
		require.Error(t, s.ValidateGrant(20, g), "group registration must fence too")
		require.NoError(t, s.ValidateGrant(19, g))
	}
	_, err = s.SetPartitions(beforeTopic.Partitions, nil)
	require.Error(t, err)
}

func TestCooperativeReservationBlocksOnlyNewGrants(t *testing.T) {
	s := cooperativeJoin(t, cooperativeFixture(t, 4), "a")
	s = cooperativeJoin(t, s, "b")
	acks := cooperativeAcks(s, "a")
	blocked := []CooperativePartition{acks[0].Partition}
	for _, grant := range s.Current {
		require.NoError(t, s.ValidateGrant(19, grant))
	}
	s, err := s.Acknowledge("a", s.session("a"), acks, blocked)
	require.NoError(t, err)
	require.Len(t, s.Current, 3)
	for _, grant := range s.Current {
		require.NotEqual(t, blocked[0], grant.Partition)
	}
	unchanged, err := s.Advance(blocked)
	require.NoError(t, err)
	require.Equal(t, s, unchanged)
	s, err = s.Advance(nil)
	require.NoError(t, err)
	require.Len(t, s.Current, 4)
	for _, grant := range s.Current {
		if grant.Partition == blocked[0] {
			require.Equal(t, "b", grant.Member)
			require.Equal(t, s.Revision, grant.Epoch)
		}
	}
}

func TestCooperativeInvalidBatchesAreAtomic(t *testing.T) {
	s := cooperativeJoin(t, cooperativeFixture(t, 6), "a")
	s = cooperativeJoin(t, s, "b")
	before := s.Clone()
	acks := cooperativeAcks(s, "a")
	_, err := s.Acknowledge("a", s.session("a"), append(acks, acks[0]), nil)
	require.Error(t, err)
	_, err = s.Remove([]CooperativeMember{{ID: "a", Session: s.session("a")}, {ID: "b", Session: 999}}, nil)
	require.Error(t, err)
	require.Equal(t, before, s)
	next, err := s.Remove(s.Members, nil)
	require.NoError(t, err)
	require.Empty(t, next.Current)
	require.Empty(t, next.Target)
	require.Empty(t, next.Members)
	next.Partitions[0].Topic = "different"
	require.Equal(t, before, s)
	s.Revision = math.MaxUint64
	_, err = s.Join("c", nil)
	require.ErrorContains(t, err, "exhausted")
}

func TestCooperativeRecoveryTrace(t *testing.T) {
	s := cooperativeFixture(t, 120)
	totalBytes, records, recoveredGrants, recoveredRevocations := 0, 0, 0, 0
	recoverState := func() {
		t.Helper()
		data, err := EncodeCooperativeAssignment(s)
		require.NoError(t, err)
		restored, err := DecodeCooperativeAssignment(data)
		require.NoError(t, err)
		require.Equal(t, s, restored)
		totalBytes += len(data)
		records++
		recoveredGrants += len(restored.Current)
		for _, g := range restored.Current {
			if g.RevokeRevision != 0 {
				recoveredRevocations++
			}
		}
		s = restored
	}
	recoverState()
	for _, member := range []string{"a", "b", "c", "d"} {
		s = cooperativeJoin(t, s, member)
		recoverState()
		for _, owner := range s.Members {
			acks := cooperativeAcks(s, owner.ID)
			if len(acks) == 0 {
				continue
			}
			var err error
			s, err = s.Acknowledge(owner.ID, owner.Session, acks, nil)
			require.NoError(t, err)
			recoverState()
		}
	}
	require.Len(t, s.Current, 120)
	for _, g := range s.Current {
		require.Zero(t, g.RevokeRevision)
	}
	t.Logf("recovery records=%d bytes=%d grants=%d pending_revocations=%d", records, totalBytes, recoveredGrants, recoveredRevocations)
}

func TestCooperativeRecoveryValidation(t *testing.T) {
	valid := cooperativeJoin(t, cooperativeJoin(t, cooperativeFixture(t, 6), "a"), "b")
	cases := map[string]func(*CooperativeAssignment){
		"registration":        func(s *CooperativeAssignment) { s.RegistrationEpoch = 0 },
		"revision":            func(s *CooperativeAssignment) { s.TargetRevision = s.Revision + 1 },
		"partition duplicate": func(s *CooperativeAssignment) { s.Partitions = append(s.Partitions, s.Partitions[0]) },
		"topic epochs":        func(s *CooperativeAssignment) { s.Partitions[0].LifecycleEpoch++ },
		"member duplicate":    func(s *CooperativeAssignment) { s.Members = append(s.Members, s.Members[0]) },
		"session duplicate":   func(s *CooperativeAssignment) { s.Members[1].Session = s.Members[0].Session },
		"target missing":      func(s *CooperativeAssignment) { s.Target = s.Target[1:] },
		"target duplicate":    func(s *CooperativeAssignment) { s.Target[1] = s.Target[0] },
		"target unbalanced": func(s *CooperativeAssignment) {
			for i := range s.Target {
				s.Target[i].Member = "a"
			}
		},
		"grant duplicate":    func(s *CooperativeAssignment) { s.Current = append(s.Current, s.Current[0]) },
		"grant session":      func(s *CooperativeAssignment) { s.Current[0].Session++ },
		"grant epoch":        func(s *CooperativeAssignment) { s.Current[0].Epoch = s.Revision + 1 },
		"revocation epoch":   func(s *CooperativeAssignment) { s.Current[0].RevokeRevision = s.Current[0].Epoch },
		"revocation missing": func(s *CooperativeAssignment) { s.Current[len(s.Current)-1].RevokeRevision = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := valid.Clone()
			mutate(s)
			require.Error(t, s.Validate())
			_, err := EncodeCooperativeAssignment(s)
			require.Error(t, err)
		})
	}
	encoded, err := EncodeCooperativeAssignment(valid)
	require.NoError(t, err)
	reordered := valid.Clone()
	slices.Reverse(reordered.Partitions)
	slices.Reverse(reordered.Members)
	slices.Reverse(reordered.Current)
	slices.Reverse(reordered.Target)
	reencoded, err := EncodeCooperativeAssignment(reordered)
	require.NoError(t, err)
	require.Equal(t, encoded, reencoded)
	for _, bad := range [][]byte{[]byte("null"), append(slices.Clone(encoded), []byte(" {}")...), bytes.Replace(encoded, []byte("\"revision\":"), []byte("\"unknown\":1,\"revision\":"), 1)} {
		_, err := DecodeCooperativeAssignment(bad)
		require.Error(t, err)
	}
}

func BenchmarkCooperativeRecovery(b *testing.B) {
	for _, n := range []int{120, 12000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s := cooperativeJoin(b, cooperativeFixture(b, n), "a")
			s = cooperativeJoin(b, s, "b")
			data, err := EncodeCooperativeAssignment(s)
			require.NoError(b, err)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, err := DecodeCooperativeAssignment(data)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(data)), "recovery-bytes")
		})
	}
}

func TestCooperativeChurnDoesNotResurrectOwnership(t *testing.T) {
	for seed := uint64(1); seed <= 12; seed++ {
		rng := rand.New(rand.NewPCG(seed, 29))
		s := cooperativeFixture(t, 17)
		var fenced []CooperativeGrant
		for step := range 200 {
			before := s.Clone()
			var err error
			var removed string
			var acknowledged []CooperativeRevocation
			blocked := []CooperativePartition{s.Partitions[rng.IntN(len(s.Partitions))]}
			switch rng.IntN(4) {
			case 0:
				id := fmt.Sprintf("member-%d", rng.IntN(6))
				if s.session(id) != 0 {
					continue
				}
				s, err = s.Join(id, blocked)
			case 1:
				if len(s.Members) == 0 {
					continue
				}
				member := s.Members[rng.IntN(len(s.Members))]
				removed = member.ID
				s, err = s.Remove([]CooperativeMember{member}, blocked)
			case 2:
				if len(s.Members) == 0 {
					continue
				}
				member := s.Members[rng.IntN(len(s.Members))]
				acknowledged = cooperativeAcks(s, member.ID)
				s, err = s.Acknowledge(member.ID, member.Session, acknowledged, blocked)
			case 3:
				s, err = s.Advance(nil)
			}
			require.NoError(t, err, "seed=%d step=%d", seed, step)
			for _, grant := range before.Current {
				if s.ValidateGrant(19, grant) == nil {
					continue
				}
				ack := CooperativeRevocation{Partition: grant.Partition, GrantEpoch: grant.Epoch, RevokeRevision: grant.RevokeRevision}
				require.True(t, grant.Member == removed || slices.Contains(acknowledged, ack), "unreleased ownership disappeared: seed=%d step=%d grant=%+v", seed, step, grant)
				fenced = append(fenced, grant)
			}
			for _, grant := range fenced {
				require.Error(t, s.ValidateGrant(19, grant), "an old token became valid again")
			}
			data, encodeErr := EncodeCooperativeAssignment(s)
			require.NoError(t, encodeErr)
			s, err = DecodeCooperativeAssignment(data)
			require.NoError(t, err)
		}
		for _, member := range s.Members {
			var err error
			s, err = s.Acknowledge(member.ID, member.Session, cooperativeAcks(s, member.ID), nil)
			require.NoError(t, err)
		}
		var err error
		s, err = s.Advance(nil)
		require.NoError(t, err)
		require.Len(t, s.Current, len(s.Target), "settled ownership must converge")
	}
}
