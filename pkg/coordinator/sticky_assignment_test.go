package coordinator

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStickyAssignmentMinimizesMovement(t *testing.T) {
	// Enumerate all valid ownership histories (including unowned partitions).
	// The maximum retained count is the sum of floor-capped old counts plus
	// one for each available extra slot whose member previously exceeded it.
	for m := 1; m <= 4; m++ {
		members := []string{"a", "b", "c", "d"}[:m]
		for n := 0; n <= 6; n++ {
			partitions := makePartitions(n)
			cases := 1
			for range n {
				cases *= m + 1
			}
			for history := 0; history < cases; history++ {
				previous := make(map[string][]int)
				owners := make(map[int]string)
				value := history
				for _, p := range partitions {
					owner := value % (m + 1)
					value /= m + 1
					if owner < m {
						id := members[owner]
						previous[id] = append(previous[id], p)
						owners[p] = id
					}
				}
				actual := stickyAssignments(partitions, members, previous)
				floor, extra := n/m, n%m
				maxRetained, aboveFloor, retained := 0, 0, 0
				seen := make(map[int]bool)
				for _, id := range members {
					maxRetained += min(len(previous[id]), floor)
					if len(previous[id]) > floor {
						aboveFloor++
					}
					if len(actual[id]) < floor || len(actual[id]) > (n+m-1)/m {
						t.Fatalf("unbalanced: n=%d m=%d old=%v new=%v", n, m, previous, actual)
					}
					if !slices.IsSorted(actual[id]) {
						t.Fatalf("non-canonical assignment: %v", actual)
					}
					for _, p := range actual[id] {
						if seen[p] || p < 0 || p >= n {
							t.Fatalf("duplicate or invalid partition: %v", actual)
						}
						seen[p] = true
						if owners[p] == id {
							retained++
						}
					}
				}
				if len(seen) != n || retained != maxRetained+min(extra, aboveFloor) {
					t.Fatalf("avoidable movement/loss: n=%d m=%d old=%v new=%v retained=%d", n, m, previous, actual, retained)
				}
				stable := stickyAssignments(partitions, members, actual)
				for _, id := range members {
					if !slices.Equal(stable[id], actual[id]) {
						t.Fatalf("no-op moved partitions: %v -> %v", actual, stable)
					}
				}
			}
		}
	}
}

func TestStickyAssignmentSanitizesHistoryWithoutMutatingInputs(t *testing.T) {
	group := &GroupMetadata{Partitions: []int{3, 1, 2, 0, 2}, Members: map[string]*MemberMetadata{
		"a": {Assignments: []int{2, 2, 99, 0}},
		"b": {Assignments: []int{2, 3, 1}},
	}}
	rebalanceGroup(group)
	require.Equal(t, []int{0, 2}, group.Members["a"].Assignments)
	require.Equal(t, []int{1, 3}, group.Members["b"].Assignments)
	require.Equal(t, []int{3, 1, 2, 0, 2}, group.Partitions)
	group.Members["a"].Assignments[0] = 42
	require.Equal(t, []int{3, 1, 2, 0, 2}, group.Partitions)
}

func TestStickyTopicAssignmentsPreserveTopicIdentity(t *testing.T) {
	a0, a1 := TopicPartition{"a", 0}, TopicPartition{"a", 1}
	b0, b1 := TopicPartition{"b", 0}, TopicPartition{"b", 1}
	group := &GroupMetadata{Topics: []string{"a", "b"}, TopicPartitions: []TopicPartition{b1, a1, b0, a0, b0}, Members: map[string]*MemberMetadata{
		"a": {Assignments: []int{9}, TopicAssignments: []TopicPartition{b0, a0}},
		"b": {TopicAssignments: []TopicPartition{b1, a1}},
	}}
	rebalanceGroup(group)
	require.Equal(t, []TopicPartition{a0, b0}, group.Members["a"].TopicAssignments)
	require.Equal(t, []TopicPartition{a1, b1}, group.Members["b"].TopicAssignments)
	require.Empty(t, group.Members["a"].Assignments)
	// Removing a topic cannot leave its ownership behind or fall back to old
	// single-topic partition IDs when the subscription becomes empty.
	group.TopicPartitions = []TopicPartition{b0, b1}
	rebalanceGroup(group)
	require.Equal(t, []TopicPartition{b0}, group.Members["a"].TopicAssignments)
	require.Equal(t, []TopicPartition{b1}, group.Members["b"].TopicAssignments)
	group.TopicPartitions = nil
	group.Partitions = []int{9}
	rebalanceGroup(group)
	for _, member := range group.Members {
		require.Empty(t, member.Assignments)
		require.Empty(t, member.TopicAssignments)
	}
}

func TestStickyMembershipPersistsRejoinsAndFencesOldGeneration(t *testing.T) {
	for _, multi := range []bool{false, true} {
		t.Run(fmt.Sprintf("multi=%t", multi), func(t *testing.T) {
			cfg := config.DefaultConfig()
			c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
			t.Cleanup(c.Stop)
			var messages []types.Message
			replayBytes := 0
			c.SetOffsetRecordWriter(func(record ConsumerMetadataRecord) error {
				payload, _, err := EncodeConsumerMetadataRecord(record)
				if err != nil {
					return err
				}
				replayBytes += len(payload)
				messages = append(messages, encodedMetadataMessage(t, record, uint64(len(messages))))
				return nil
			})
			if multi {
				require.NoError(t, c.RegisterGroupSubscription("g", []string{"events", "other"}, "", map[string]int{"events": 3, "other": 3}))
			} else {
				require.NoError(t, c.RegisterGroup("events", "g", 6))
			}
			for _, id := range []string{"b", "c", "a"} {
				_, err := c.AddConsumer("g", id)
				require.NoError(t, err)
			}
			before := c.ExportState()["g"]
			_, err := c.AddConsumer("g", "b")
			require.NoError(t, err)
			after := c.ExportState()["g"]
			require.Equal(t, before.Generation+1, after.Generation)
			require.Equal(t, before.Members, after.Members)
			require.Equal(t, before.TopicAssignments, after.TopicAssignments)
			require.Contains(t, c.ValidateMemberGeneration("g", "b", before.Generation), "GEN_MISMATCH")
			require.Empty(t, c.ValidateMemberGeneration("g", "b", after.Generation))
			partitions := c.GetMemberTopicAssignments("g", "b")
			require.NotEmpty(t, partitions)
			p := partitions[0]
			require.ErrorContains(t, c.ValidateAndCommit("g", p.Topic, p.Partition, 10, before.Generation, "b"), "GEN_MISMATCH")

			// A failed durable write must not publish the candidate assignment.
			c.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return fmt.Errorf("append unavailable") })
			_, err = c.AddConsumer("g", "d")
			require.ErrorContains(t, err, "append unavailable")
			require.Equal(t, after, c.ExportState()["g"])
			restored := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
			t.Cleanup(restored.Stop)
			require.NoError(t, restored.ImportState(c.ExportState()))
			require.Equal(t, after.Members, restored.ExportState()["g"].Members)
			require.Equal(t, after.TopicAssignments, restored.ExportState()["g"].TopicAssignments)
			require.Contains(t, restored.ValidateMemberGeneration("g", "b", before.Generation), "GEN_MISMATCH")
			recovered, err := NewCoordinatorWithRecovery(context.Background(), cfg, &metadataReplayHandler{messages: map[int][]types.Message{0: messages}})
			require.NoError(t, err)
			t.Cleanup(recovered.Stop)
			replayed := recovered.ExportState()["g"]
			require.Equal(t, after.Generation, replayed.Generation)
			require.Equal(t, after.Members, replayed.Members)
			require.Equal(t, after.TopicAssignments, replayed.TopicAssignments)
			require.Contains(t, recovered.ValidateMemberGeneration("g", "b", before.Generation), "GEN_MISMATCH")
			recovered.SetOffsetRecordWriter(func(ConsumerMetadataRecord) error { return nil })
			_, err = recovered.AddConsumer("g", "b")
			require.NoError(t, err)
			require.Equal(t, after.Members, recovered.ExportState()["g"].Members)
			require.Equal(t, after.TopicAssignments, recovered.ExportState()["g"].TopicAssignments)
			t.Logf("recovery input: records=%d encoded_payload_bytes=%d; assignments preserved after replay and rejoin", len(messages), replayBytes)
		})
	}
}

func TestStickyLeaveAndExpirationRetainSurvivingOwners(t *testing.T) {
	for _, expire := range []bool{false, true} {
		c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
		t.Cleanup(c.Stop)
		require.NoError(t, c.RegisterGroup("events", "g", 12))
		for _, id := range []string{"a", "b", "c", "d"} {
			_, err := c.AddConsumer("g", id)
			require.NoError(t, err)
		}
		before := c.ExportState()["g"]
		if expire {
			require.NoError(t, c.ExpireConsumers("g", before.Generation, []string{"b", "c"}))
		} else {
			require.NoError(t, c.RemoveConsumerForGeneration("g", "b", before.Generation))
		}
		after := c.ExportState()["g"]
		for id, current := range after.Members {
			for _, p := range before.Members[id] {
				require.Contains(t, current, p, "surviving owner %s lost partition %d", id, p)
			}
		}
		require.Equal(t, before.Generation+1, after.Generation)
	}
}
