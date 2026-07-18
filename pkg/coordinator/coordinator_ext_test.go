package coordinator

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoordinator_StatusAndList(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})

	_ = c.RegisterGroup("topic1", "group1", 4)
	_, _ = c.AddConsumer("group1", "c1")
	c.Rebalance("group1")

	t.Run("ListGroups", func(t *testing.T) {
		groups := c.ListGroups()
		assert.Contains(t, groups, "group1")
	})

	t.Run("GetGroupStatus", func(t *testing.T) {
		status, err := c.GetGroupStatus("group1")
		assert.NoError(t, err)
		assert.Equal(t, "group1", status.GroupName)
		assert.Equal(t, "topic1", status.TopicName)
		assert.Equal(t, "Stable", status.State)
		assert.Equal(t, 1, status.MemberCount)
		assert.Equal(t, 4, status.PartitionCount)
		if assert.NotEmpty(t, status.Members) {
			assert.Equal(t, 4, len(status.Members[0].Assignments))
		}
	})

	t.Run("GetGroupStatus - Not Found", func(t *testing.T) {
		_, err := c.GetGroupStatus("no-group")
		assert.Error(t, err)
	})
}

func TestCoordinator_Offsets(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})

	_ = c.RegisterGroup("topic1", "group1", 2)

	t.Run("FetchOffset - Empty", func(t *testing.T) {
		off, ok := c.GetOffset("group1", "topic1", 0)
		assert.False(t, ok)
		assert.Equal(t, uint64(0), off)
	})

	t.Run("CommitOffset", func(t *testing.T) {
		err := c.CommitOffset("group1", "topic1", 0, 100)
		assert.NoError(t, err)

		off, ok := c.GetOffset("group1", "topic1", 0)
		assert.True(t, ok)
		assert.Equal(t, uint64(100), off)
	})

	t.Run("CommitOffsetsBulk", func(t *testing.T) {
		offsets := []OffsetItem{
			{Partition: 0, Offset: 300},
			{Partition: 1, Offset: 400},
		}
		err := c.CommitOffsetsBulk("group1", "topic1", offsets)
		assert.NoError(t, err)

		off0, _ := c.GetOffset("group1", "topic1", 0)
		off1, _ := c.GetOffset("group1", "topic1", 1)
		assert.Equal(t, uint64(300), off0)
		assert.Equal(t, uint64(400), off1)
	})

	t.Run("ApplyOffsetUpdateFromFSM", func(t *testing.T) {
		offsets := []OffsetItem{
			{Partition: 0, Offset: 500},
		}
		err := c.ApplyOffsetUpdateFromFSM("group1", "topic1", offsets)
		assert.NoError(t, err)

		off, _ := c.GetOffset("group1", "topic1", 0)
		assert.Equal(t, uint64(500), off)
	})

	t.Run("RejectLowerOffset", func(t *testing.T) {
		err := c.CommitOffset("group1", "topic1", 0, 499)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "offset regression")

		off, _ := c.GetOffset("group1", "topic1", 0)
		assert.Equal(t, uint64(500), off)
	})
}

func TestCoordinator_DurableOffsetReplayAndIsolation(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ConsumerSessionTimeoutMS = 30000
	cfg.ConsumerHeartbeatCheckMS = 5000

	offsetLog := newPersistentOffsetLog()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := NewCoordinator(ctx, cfg, offsetLog)
	require.NoError(t, c.RegisterGroup("topic1", "groupA", 2))
	require.NoError(t, c.RegisterGroup("topic1", "groupB", 2))

	require.NoError(t, c.CommitOffset("groupA", "topic1", 0, 3))
	require.NoError(t, c.CommitOffset("groupA", "topic1", 1, 8))
	require.NoError(t, c.CommitOffset("groupB", "topic1", 0, 1))
	assert.ErrorContains(t, c.CommitOffset("groupA", "topic1", 0, 2), "offset regression")

	restarted := NewCoordinator(ctx, cfg, offsetLog)

	offset, ok := restarted.GetOffset("groupA", "topic1", 0)
	require.True(t, ok)
	assert.Equal(t, uint64(3), offset)

	offset, ok = restarted.GetOffset("groupA", "topic1", 1)
	require.True(t, ok)
	assert.Equal(t, uint64(8), offset)

	offset, ok = restarted.GetOffset("groupB", "topic1", 0)
	require.True(t, ok)
	assert.Equal(t, uint64(1), offset)

	assert.ErrorContains(t, restarted.CommitOffset("groupA", "topic1", 0, 2), "offset regression")
	offset, _ = restarted.GetOffset("groupA", "topic1", 0)
	assert.Equal(t, uint64(3), offset)

	require.NoError(t, restarted.RegisterGroup("topic1", "groupA", 2))
	_, err := restarted.AddConsumer("groupA", "member-1")
	require.NoError(t, err)
}

type persistentOffsetLog struct {
	partitions map[int][]types.Message
}

func newPersistentOffsetLog() *persistentOffsetLog {
	return &persistentOffsetLog{partitions: make(map[int][]types.Message)}
}

func (p *persistentOffsetLog) CreateTopic(string, int, bool, bool) error {
	return nil
}

func (p *persistentOffsetLog) Publish(topic string, msg *types.Message) error {
	return p.PublishWithAck(topic, msg)
}

func (p *persistentOffsetLog) PublishWithAck(_ string, msg *types.Message) error {
	cp := *msg
	cp.Offset = uint64(len(p.partitions[0]))
	p.partitions[0] = append(p.partitions[0], cp)
	return nil
}

func (p *persistentOffsetLog) ReadTopicPartition(_ string, partitionID int, offset uint64, max int) ([]types.Message, error) {
	records := p.partitions[partitionID]
	if int(offset) >= len(records) {
		return nil, nil
	}
	end := int(offset) + max
	if end > len(records) {
		end = len(records)
	}
	start := int(offset)
	out := make([]types.Message, end-start)
	copy(out, records[start:end])
	return out, nil
}

func TestCoordinatorValidateAndCommitReturnsWireErrors(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	require.NoError(t, c.RegisterGroup("topic1", "group1", 2))
	assignments, err := c.AddConsumer("group1", "member-1")
	require.NoError(t, err)
	require.NotEmpty(t, assignments)
	generation := c.GetGeneration("group1")

	require.NoError(t, c.ValidateAndCommit("group1", "topic1", assignments[0], 10, generation, "member-1"))
	offset, ok := c.GetOffset("group1", "topic1", assignments[0])
	require.True(t, ok)
	assert.Equal(t, uint64(10), offset)

	err = c.ValidateAndCommit("group1", "topic1", assignments[0], 11, generation+1, "member-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ERROR: GEN_MISMATCH")

	err = c.ValidateAndCommit("group1", "topic1", assignments[0], 11, generation, "missing-member")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ERROR: member_not_found")

	unownedPartition := 1
	if assignments[0] == 1 {
		unownedPartition = 0
	}
	require.NoError(t, c.RegisterGroup("single-topic", "single-group", 2))
	_, err = c.AddConsumer("single-group", "single-member")
	require.NoError(t, err)
	singleGeneration := c.GetGeneration("single-group")
	c.mu.Lock()
	c.groups["single-group"].Members["single-member"].Assignments = []int{assignments[0]}
	c.mu.Unlock()
	err = c.ValidateAndCommit("single-group", "single-topic", unownedPartition, 1, singleGeneration, "single-member")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ERROR: NOT_OWNER")
}
func TestCoordinator_MemberAssignments(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(context.Background(), cfg, &DummyPublisher{})

	_ = c.RegisterGroup("t1", "g1", 2)
	_, _ = c.AddConsumer("g1", "m1")
	c.Rebalance("g1")

	asgn := c.GetMemberAssignments("g1", "m1")
	assert.ElementsMatch(t, []int{0, 1}, asgn)

	assert.Nil(t, c.GetMemberAssignments("no-group", "m1"))
	assert.Empty(t, c.GetMemberAssignments("g1", "no-member"))
}
