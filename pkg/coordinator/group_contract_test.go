package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResumeConsumerPreservesGenerationAndAssignments(t *testing.T) {
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	require.NoError(t, c.RegisterGroup("events", "workers", 4))

	assignments, err := c.AddConsumer("workers", "member-1")
	require.NoError(t, err)
	generation := c.GetGeneration("workers")
	require.Positive(t, generation)

	resumed, err := c.ResumeConsumer("workers", "member-1", generation)
	require.NoError(t, err)
	assert.Equal(t, assignments, resumed)
	assert.Equal(t, generation, c.GetGeneration("workers"))

	_, err = c.ResumeConsumer("workers", "member-1", generation+1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ERROR: GEN_MISMATCH")
	assert.Equal(t, generation, c.GetGeneration("workers"))
}

func TestExpireConsumersAdvancesGenerationOnceAndTransfersOwnership(t *testing.T) {
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	require.NoError(t, c.RegisterGroup("events", "workers", 4))
	_, err := c.AddConsumer("workers", "member-1")
	require.NoError(t, err)
	_, err = c.AddConsumer("workers", "member-2")
	require.NoError(t, err)
	generation := c.GetGeneration("workers")

	require.NoError(t, c.ExpireConsumers("workers", generation, []string{"member-1"}))
	assert.Equal(t, generation+1, c.GetGeneration("workers"))

	status, err := c.GetGroupStatus("workers")
	require.NoError(t, err)
	require.Len(t, status.Members, 1)
	assert.Equal(t, "member-2", status.Members[0].MemberID)
	assert.ElementsMatch(t, []int{0, 1, 2, 3}, status.Members[0].Assignments)

	err = c.ExpireConsumers("workers", generation, []string{"member-1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ERROR: GEN_MISMATCH")
	assert.Equal(t, generation+1, c.GetGeneration("workers"))
}

func TestGroupSnapshotRestoresPartitionsBeforeFirstOffsetCommit(t *testing.T) {
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	require.NoError(t, c.RegisterGroup("events", "workers", 4))
	assignments, err := c.AddConsumer("workers", "member-1")
	require.NoError(t, err)
	require.Len(t, assignments, 4)

	restored := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	restored.ImportState(c.ExportState())

	status, err := restored.GetGroupStatus("workers")
	require.NoError(t, err)
	assert.Equal(t, 4, status.PartitionCount)
	require.Len(t, status.Members, 1)
	assert.ElementsMatch(t, []int{0, 1, 2, 3}, status.Members[0].Assignments)

	_, err = restored.AddConsumer("workers", "member-2")
	require.NoError(t, err)
	status, err = restored.GetGroupStatus("workers")
	require.NoError(t, err)
	for _, member := range status.Members {
		assert.Len(t, member.Assignments, 2)
	}
}

func TestApplyFencedOffsetUpdateRejectsStaleAndPartialMutation(t *testing.T) {
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	require.NoError(t, c.RegisterGroup("events", "workers", 2))
	_, err := c.AddConsumer("workers", "member-1")
	require.NoError(t, err)
	generation := c.GetGeneration("workers")

	err = c.ApplyFencedOffsetUpdateFromFSM(
		"workers",
		"events",
		"member-1",
		generation+1,
		[]OffsetItem{{Partition: 0, Offset: 5}},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ERROR: GEN_MISMATCH")
	_, ok := c.GetOffset("workers", "events", 0)
	assert.False(t, ok)

	require.NoError(t, c.ApplyFencedOffsetUpdateFromFSM(
		"workers",
		"events",
		"member-1",
		generation,
		[]OffsetItem{{Partition: 0, Offset: 5}, {Partition: 1, Offset: 10}},
	))

	err = c.ApplyFencedOffsetUpdateFromFSM(
		"workers",
		"events",
		"member-1",
		generation,
		[]OffsetItem{{Partition: 0, Offset: 6}, {Partition: 1, Offset: 9}},
	)
	require.ErrorContains(t, err, "offset regression")
	offset, ok := c.GetOffset("workers", "events", 0)
	require.True(t, ok)
	assert.Equal(t, uint64(5), offset)
	offset, ok = c.GetOffset("workers", "events", 1)
	require.True(t, ok)
	assert.Equal(t, uint64(10), offset)
}

func TestDistributedTimeoutRequiresOwnershipGraceAndReplicatedMutation(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	require.NoError(t, c.RegisterGroup("events", "workers", 2))
	_, err := c.AddConsumer("workers", "member-1")
	require.NoError(t, err)

	timeout := time.Second
	c.mu.Lock()
	c.groups["workers"].Members["member-1"].LastHeartbeat = time.Now().Add(-2 * timeout)
	c.mu.Unlock()

	owner := false
	var (
		calls      int
		generation int
		members    []string
	)
	c.SetGroupSessionCallbacks(
		func(string) bool { return owner },
		func(_ string, gotGeneration int, gotMembers []string) error {
			calls++
			generation = gotGeneration
			members = append([]string(nil), gotMembers...)
			return nil
		},
	)

	c.checkSingleGroupTimeout("workers", timeout)
	assert.Zero(t, calls)

	owner = true
	c.checkSingleGroupTimeout("workers", timeout)
	assert.Zero(t, calls, "new coordinator ownership must wait one full session timeout")

	c.mu.Lock()
	c.ownershipSince["workers"] = time.Now().Add(-2 * timeout)
	wantGeneration := c.groups["workers"].Generation
	c.mu.Unlock()

	c.checkSingleGroupTimeout("workers", timeout)
	assert.Equal(t, 1, calls)
	assert.Equal(t, wantGeneration, generation)
	assert.Equal(t, []string{"member-1"}, members)

	status, err := c.GetGroupStatus("workers")
	require.NoError(t, err)
	assert.Equal(t, 1, status.MemberCount, "distributed timeout mutation must be applied through the replicated callback")
}

type partitionedOffsetReader struct {
	records      map[int][]types.Message
	lastOffset   map[int]uint64
	offsetWasSet map[int]bool
}

func (r *partitionedOffsetReader) ReadTopicPartition(_ string, partitionID int, offset uint64, max int) ([]types.Message, error) {
	if r.lastOffset == nil {
		r.lastOffset = make(map[int]uint64)
		r.offsetWasSet = make(map[int]bool)
	}
	if r.offsetWasSet[partitionID] && r.lastOffset[partitionID] == offset {
		return nil, fmt.Errorf("repeated offset request: partition=%d offset=%d", partitionID, offset)
	}
	r.lastOffset[partitionID] = offset
	r.offsetWasSet[partitionID] = true
	records := r.records[partitionID]
	if offset >= uint64(len(records)) {
		return nil, nil
	}
	end := int(offset) + max
	if end > len(records) {
		end = len(records)
	}
	result := make([]types.Message, end-int(offset))
	copy(result, records[int(offset):end])
	return result, nil
}

func TestLoadOffsetsFromLogIsIndependentOfInternalPartitionOrder(t *testing.T) {
	oldBulk, err := json.Marshal(BulkOffsetMsg{
		Group: "workers",
		Topic: "events",
		Offsets: []OffsetItem{
			{Partition: 0, Offset: 10},
			{Partition: 1, Offset: 7},
		},
		Timestamp: time.Now().Add(-time.Minute),
	})
	require.NoError(t, err)
	newSingle, err := json.Marshal(OffsetCommitMessage{
		Group:     "workers",
		Topic:     "events",
		Partition: 0,
		Offset:    12,
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	reader := &partitionedOffsetReader{records: map[int][]types.Message{
		0: {{Offset: 0, Payload: string(newSingle)}},
		1: {{Offset: 0, Payload: string(oldBulk)}},
	}}
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	require.NoError(t, c.LoadOffsetsFromLog(reader))

	offset, ok := c.GetOffset("workers", "events", 0)
	require.True(t, ok)
	assert.Equal(t, uint64(12), offset)
	offset, ok = c.GetOffset("workers", "events", 1)
	require.True(t, ok)
	assert.Equal(t, uint64(7), offset)
}

func TestLoadOffsetsFromLogAdvancesPastInvalidFullBatch(t *testing.T) {
	records := make([]types.Message, 1025)
	for i := 0; i < 1024; i++ {
		records[i] = types.Message{Offset: uint64(i), Payload: "invalid"}
	}
	valid, err := json.Marshal(OffsetCommitMessage{
		Group:     "workers",
		Topic:     "events",
		Partition: 2,
		Offset:    19,
		Timestamp: time.Now(),
	})
	require.NoError(t, err)
	records[1024] = types.Message{Offset: 1024, Payload: string(valid)}

	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	require.NoError(t, c.LoadOffsetsFromLog(&partitionedOffsetReader{
		records: map[int][]types.Message{0: records},
	}))
	offset, ok := c.GetOffset("workers", "events", 2)
	require.True(t, ok)
	assert.Equal(t, uint64(19), offset)
}

func TestCommitOffsetsBulkRejectsDuplicatePartitionAtomically(t *testing.T) {
	c := NewCoordinator(context.Background(), config.DefaultConfig(), &DummyPublisher{})
	require.NoError(t, c.RegisterGroup("events", "workers", 1))
	require.NoError(t, c.CommitOffset("workers", "events", 0, 5))

	err := c.CommitOffsetsBulk("workers", "events", []OffsetItem{
		{Partition: 0, Offset: 6},
		{Partition: 0, Offset: 4},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ERROR: duplicate_partition")
	offset, ok := c.GetOffset("workers", "events", 0)
	require.True(t, ok)
	assert.Equal(t, uint64(5), offset)
}

type discoveringOffsetLog struct {
	*partitionedOffsetReader
	DummyPublisher
	partitionCount int
}

func (l *discoveringOffsetLog) ExistingPartitionCount(string) (int, error) {
	return l.partitionCount, nil
}

func TestCoordinatorDiscoversExpandedOffsetTopicBeforeReplay(t *testing.T) {
	payload, err := json.Marshal(OffsetCommitMessage{
		Group:     "workers",
		Topic:     "events",
		Partition: 3,
		Offset:    27,
		Timestamp: time.Now(),
	})
	require.NoError(t, err)
	log := &discoveringOffsetLog{
		partitionedOffsetReader: &partitionedOffsetReader{records: map[int][]types.Message{
			5: {{Offset: 0, Payload: string(payload)}},
		}},
		partitionCount: 6,
	}

	c := NewCoordinator(context.Background(), config.DefaultConfig(), log)
	assert.Equal(t, 6, c.offsetTopicPartitionCount)
	offset, ok := c.GetOffset("workers", "events", 3)
	require.True(t, ok)
	assert.Equal(t, uint64(27), offset)
}
