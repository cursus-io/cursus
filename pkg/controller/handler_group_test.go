package controller_test

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
)

type DummyPublisher struct{}

func (d *DummyPublisher) Publish(topic string, msg *types.Message) error {
	return nil
}
func (d *DummyPublisher) CreateTopic(topic string, partitionCount int, idempotent bool) error {
	return nil
}

func TestCommandHandler_GroupOps(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	tm.CreateTopic("topic1", 4, false)

	coord := coordinator.NewCoordinator(cfg, &DummyPublisher{})
	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("", 0)

	t.Run("REGISTER_GROUP", func(t *testing.T) {
		resp := ch.HandleCommand("REGISTER_GROUP topic=topic1 group=g1", ctx)
		assert.Contains(t, resp, "✅ Group 'g1' registered")
	})

	t.Run("JOIN_GROUP", func(t *testing.T) {
		resp := ch.HandleCommand("JOIN_GROUP topic=topic1 group=g1 member=m1", ctx)
		assert.Contains(t, resp, "OK generation=")
		assert.Contains(t, resp, "member=m1-")
		assert.NotEmpty(t, ctx.MemberID)
	})

	t.Run("SYNC_GROUP", func(t *testing.T) {
		resp := ch.HandleCommand("SYNC_GROUP topic=topic1 group=g1 member="+ctx.MemberID, ctx)
		assert.Contains(t, resp, "OK assignments=")
	})

	t.Run("HEARTBEAT", func(t *testing.T) {
		resp := ch.HandleCommand("HEARTBEAT topic=topic1 group=g1 member="+ctx.MemberID, ctx)
		assert.Equal(t, "OK", resp)
	})

	t.Run("GROUP_STATUS", func(t *testing.T) {
		resp := ch.HandleCommand("GROUP_STATUS group=g1", ctx)
		assert.Contains(t, resp, `"group_name":"g1"`)
	})

	t.Run("handleBatchCommit", func(t *testing.T) {
		cmd := "BATCH_COMMIT topic=topic1 group=g1 generation=1 member=" + ctx.MemberID + " 0:150,1:250"
		resp := ch.HandleCommand(cmd, ctx)
		assert.Contains(t, resp, "OK batched=2")
		
		off0, _ := coord.GetOffset("g1", "topic1", 0)
		assert.Equal(t, uint64(150), off0)
	})

	t.Run("LEAVE_GROUP", func(t *testing.T) {
		resp := ch.HandleCommand("LEAVE_GROUP topic=topic1 group=g1 member="+ctx.MemberID, ctx)
		assert.Contains(t, resp, "✅ Left group 'g1'")
	})
}

func TestCommandHandler_OffsetOps(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	tm.CreateTopic("topic1", 4, false)

	coord := coordinator.NewCoordinator(cfg, &DummyPublisher{})
	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("g1", 0)

	coord.RegisterGroup("topic1", "g1", 4)

	t.Run("COMMIT_OFFSET", func(t *testing.T) {
		resp := ch.HandleCommand("COMMIT_OFFSET topic=topic1 partition=0 group=g1 offset=123", ctx)
		assert.Equal(t, "OK", resp)
	})

	t.Run("FETCH_OFFSET", func(t *testing.T) {
		resp := ch.HandleCommand("FETCH_OFFSET topic=topic1 partition=0 group=g1", ctx)
		assert.Equal(t, "123", resp)
	})

	t.Run("FETCH_OFFSET - Not Found", func(t *testing.T) {
		resp := ch.HandleCommand("FETCH_OFFSET topic=topic1 partition=1 group=g1", ctx)
		assert.Equal(t, "0", resp)
	})
}
