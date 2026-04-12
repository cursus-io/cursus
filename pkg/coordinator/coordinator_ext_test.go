package coordinator

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestCoordinator_StatusAndList(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(cfg, &DummyPublisher{})

	c.RegisterGroup("topic1", "group1", 4)
	c.AddConsumer("group1", "c1")
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
		assert.Equal(t, 4, len(status.Members[0].Assignments))
	})

	t.Run("GetGroupStatus - Not Found", func(t *testing.T) {
		_, err := c.GetGroupStatus("no-group")
		assert.Error(t, err)
	})
}

func TestCoordinator_Offsets(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(cfg, &DummyPublisher{})

	c.RegisterGroup("topic1", "group1", 2)
	
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
}

func TestCoordinator_MemberAssignments(t *testing.T) {
	cfg := &config.Config{}
	c := NewCoordinator(cfg, &DummyPublisher{})

	c.RegisterGroup("t1", "g1", 2)
	c.AddConsumer("g1", "m1")
	c.Rebalance("g1")

	asgn := c.GetMemberAssignments("g1", "m1")
	assert.ElementsMatch(t, []int{0, 1}, asgn)

	assert.Nil(t, c.GetMemberAssignments("no-group", "m1"))
	assert.Empty(t, c.GetMemberAssignments("g1", "no-member"))
}
