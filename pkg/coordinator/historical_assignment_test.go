package coordinator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHistoricalMembershipKeepsRangeAssignmentAndCommitAuthority(t *testing.T) {
	for _, multi := range []bool{false, true} {
		c := newAuthorityTestCoordinator(t)
		topic := "events"
		if multi {
			require.NoError(t, c.RegisterGroupSubscriptionFromFSM("g", []string{"events", "other"}, "", map[string]int{"events": 3, "other": 3}))
			topic = ""
		}
		for _, id := range []string{"a", "b", "c"} {
			require.NoError(t, c.JoinGroupFromFSM("g", id, topic, 6))
		}
		commit := func(member string, generation, partition int, offset uint64) error {
			return c.ApplyFencedTopicOffsetUpdateFromFSM("g", member, generation, c.GetGroup("g").RegistrationEpoch,
				map[string][]OffsetItem{"events": {{Partition: partition, Offset: offset}}})
		}
		// The original range algorithm grants events:2 to b. Sticky would
		// retain b's events:3 in a single-topic group and grant events:2 to c.
		require.NoError(t, commit("b", 3, 2, 5))
		before := c.ExportState()["g"]
		require.NoError(t, c.JoinGroupFromFSM("g", "b", topic, 6))
		after := c.ExportState()["g"]
		require.Equal(t, before.Members, after.Members)
		require.Equal(t, before.TopicAssignments, after.TopicAssignments)
		require.NoError(t, c.RemoveConsumerForGenerationFromFSM("g", "a", 4))
		// Range gives the first half to b after a leaves. In multi-topic mode
		// this is every events partition; the topic identity must be preserved.
		require.NoError(t, commit("b", 5, 0, 7))
		require.NoError(t, c.ExpireConsumersFromFSM("g", 5, []string{"b"}))
		require.NoError(t, commit("c", 6, 0, 8))
		_, envelope, err := c.ExportStateWithRecovery()
		require.NoError(t, err)
		require.True(t, envelope.KnownComplete)
		require.Equal(t, uint64(8), envelope.RaftCompatibilityState["g"].Offsets["events"][0])
	}
}
