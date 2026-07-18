package controller_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/controller"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type DummyPublisher struct{}

func (d *DummyPublisher) Publish(topic string, msg *types.Message) error {
	return nil
}
func (d *DummyPublisher) CreateTopic(topic string, partitionCount int, idempotent bool, eventSourcing bool) error {
	return nil
}

func TestCommandHandler_GroupOps(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	_ = tm.CreateTopic("topic1", 4, false, false)

	coord := coordinator.NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("", 0)

	t.Run("REGISTER_GROUP", func(t *testing.T) {
		resp := ch.HandleCommand("REGISTER_GROUP topic=topic1 group=g1", ctx)
		assert.Contains(t, resp, "OK group=g1 topic=topic1")
		assert.Contains(t, resp, "registered=true")
	})

	t.Run("JOIN_GROUP", func(t *testing.T) {
		resp := ch.HandleCommand("JOIN_GROUP topic=topic1 group=g1 member=m1", ctx)
		assert.Contains(t, resp, "OK generation=")
		assert.Contains(t, resp, "member=m1-")
		assert.NotEmpty(t, ctx.MemberID)
	})

	t.Run("SYNC_GROUP", func(t *testing.T) {
		cmd := fmt.Sprintf("SYNC_GROUP topic=topic1 group=g1 member=%s generation=%d", ctx.MemberID, ctx.Generation)
		resp := ch.HandleCommand(cmd, ctx)
		assert.Contains(t, resp, "OK generation=")
	})

	t.Run("HEARTBEAT", func(t *testing.T) {
		cmd := fmt.Sprintf("HEARTBEAT topic=topic1 group=g1 member=%s generation=%d", ctx.MemberID, ctx.Generation)
		resp := ch.HandleCommand(cmd, ctx)
		assert.Contains(t, resp, "OK member=")
	})

	t.Run("GROUP_STATUS", func(t *testing.T) {
		resp := ch.HandleCommand("GROUP_STATUS group=g1", ctx)
		assert.Contains(t, resp, `"group_name":"g1"`)
	})

	t.Run("handleBatchCommit", func(t *testing.T) {
		cmd := fmt.Sprintf("BATCH_COMMIT topic=topic1 group=g1 generation=%d member=%s P0:150,P1:250", ctx.Generation, ctx.MemberID)
		resp := ch.HandleCommand(cmd, ctx)
		assert.Contains(t, resp, "OK batched=2")

		off0, _ := coord.GetOffset("g1", "topic1", 0)
		assert.Equal(t, uint64(150), off0)
	})

	t.Run("LEAVE_GROUP", func(t *testing.T) {
		cmd := fmt.Sprintf("LEAVE_GROUP topic=topic1 group=g1 member=%s generation=%d", ctx.MemberID, ctx.Generation)
		resp := ch.HandleCommand(cmd, ctx)
		assert.Contains(t, resp, "left=true")
	})
}

func TestCommandHandler_OffsetOps(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	_ = tm.CreateTopic("topic1", 4, false, false)

	coord := coordinator.NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	ctx := controller.NewClientContext("g1", 0)

	_ = coord.RegisterGroup("topic1", "g1", 4)
	assignments, err := coord.AddConsumer("g1", "m1")
	require.NoError(t, err)
	require.Contains(t, assignments, 0)
	generation := coord.GetGeneration("g1")

	t.Run("COMMIT_OFFSET", func(t *testing.T) {
		resp := ch.HandleCommand(fmt.Sprintf("COMMIT_OFFSET topic=topic1 partition=0 group=g1 offset=123 member=m1 generation=%d", generation), ctx)
		assert.Equal(t, "OK", resp)
	})
	t.Run("ownership-only validation ignores offset regression", func(t *testing.T) {
		resp := ch.HandleCommand(fmt.Sprintf("COMMIT_OFFSET topic=topic1 partition=0 group=g1 offset=1 member=m1 generation=%d validate_only=true ownership_only=true", generation), ctx)
		assert.Equal(t, "OK validated=true", resp)
		offset, found := coord.GetOffset("g1", "topic1", 0)
		require.True(t, found)
		assert.Equal(t, uint64(123), offset)
	})

	t.Run("FETCH_OFFSET", func(t *testing.T) {
		resp := ch.HandleCommand("FETCH_OFFSET topic=topic1 partition=0 group=g1", ctx)
		assert.Equal(t, "OK offset=123", resp)
	})

	t.Run("FETCH_OFFSET - Not Found", func(t *testing.T) {
		resp := ch.HandleCommand("FETCH_OFFSET topic=topic1 partition=1 group=g1", ctx)
		assert.Equal(t, "OK offset=0", resp)
	})
}

func TestCommandHandler_GroupCommandsRejectTopicMismatchAndPartialBatch(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	require.NoError(t, tm.CreateTopic("topic1", 2, false, false))
	require.NoError(t, tm.CreateTopic("topic2", 2, false, false))

	coord := coordinator.NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	t.Cleanup(func() { require.NoError(t, ch.Close()) })
	ctx := controller.NewClientContext("g1", 0)

	require.NoError(t, coord.RegisterGroup("topic1", "g1", 2))
	assignments, err := coord.AddConsumer("g1", "m1")
	require.NoError(t, err)
	require.Contains(t, assignments, 0)
	generation := coord.GetGeneration("g1")
	require.NoError(t, coord.CommitOffset("g1", "topic1", 0, 123))

	commands := []string{
		"JOIN_GROUP topic=topic2 group=g1 member=intruder",
		fmt.Sprintf("SYNC_GROUP topic=topic2 group=g1 member=m1 generation=%d", generation),
		fmt.Sprintf("HEARTBEAT topic=topic2 group=g1 member=m1 generation=%d", generation),
		fmt.Sprintf("LEAVE_GROUP topic=topic2 group=g1 member=m1 generation=%d", generation),
	}
	for _, command := range commands {
		resp := ch.HandleCommand(command, ctx)
		assert.Contains(t, resp, "ERROR: topic_not_assigned_to_group", command)
	}

	status, err := coord.GetGroupStatus("g1")
	require.NoError(t, err)
	assert.Equal(t, 1, status.MemberCount)
	assert.Equal(t, generation, status.Generation)

	resp := ch.HandleCommand(
		fmt.Sprintf("BATCH_COMMIT topic=topic1 group=g1 generation=%d member=m1 P0:150,invalid", generation),
		ctx,
	)
	assert.Contains(t, resp, "ERROR: invalid_batch_commit_entry")
	offset, found := coord.GetOffset("g1", "topic1", 0)
	require.True(t, found)
	assert.Equal(t, uint64(123), offset)

	resp = ch.HandleCommand(
		fmt.Sprintf("COMMIT_OFFSET topic=topic1 partition=-1 group=g1 offset=150 member=m1 generation=%d", generation),
		ctx,
	)
	assert.Equal(t, "ERROR: invalid_partition", resp)

	resp = ch.HandleCommand(
		"BATCH_COMMIT topic=topic1 group=g1 member=m1 P0:150",
		ctx,
	)
	assert.Equal(t, "ERROR: missing_generation command=BATCH_COMMIT", resp)
}
func TestCommandHandler_GroupResumeAndGenerationFencing(t *testing.T) {
	cfg := config.DefaultConfig()
	hp := &mockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	require.NoError(t, tm.CreateTopic("resume-topic", 4, false, false))

	coord := coordinator.NewCoordinator(context.Background(), cfg, &DummyPublisher{})
	ch := controller.NewCommandHandler(tm, cfg, coord, nil, nil)
	t.Cleanup(func() { require.NoError(t, ch.Close()) })
	ctx := controller.NewClientContext("", 0)

	registerResp := ch.HandleCommand("REGISTER_GROUP topic=resume-topic group=resume-group", ctx)
	require.Contains(t, registerResp, "registered=true")
	joinResp := ch.HandleCommand("JOIN_GROUP topic=resume-topic group=resume-group member=member", ctx)
	require.Contains(t, joinResp, "OK generation=")

	member := ctx.MemberID
	generation := ctx.Generation
	resumeResp := ch.HandleCommand(
		fmt.Sprintf("JOIN_GROUP topic=resume-topic group=resume-group member=%s generation=%d", member, generation),
		ctx,
	)
	assert.Contains(t, resumeResp, "resumed=true")
	assert.Equal(t, member, ctx.MemberID)
	assert.Equal(t, generation, ctx.Generation)

	status, err := coord.GetGroupStatus("resume-group")
	require.NoError(t, err)
	assert.Equal(t, 1, status.MemberCount)
	assert.Equal(t, generation, status.Generation)

	otherCtx := controller.NewClientContext("", 0)
	otherJoin := ch.HandleCommand("JOIN_GROUP topic=resume-topic group=resume-group member=other", otherCtx)
	require.Contains(t, otherJoin, "OK generation=")
	require.Greater(t, otherCtx.Generation, generation)

	staleHeartbeat := ch.HandleCommand(
		fmt.Sprintf("HEARTBEAT topic=resume-topic group=resume-group member=%s generation=%d", member, generation),
		ctx,
	)
	assert.Contains(t, staleHeartbeat, "ERROR: GEN_MISMATCH")

	staleSync := ch.HandleCommand(
		fmt.Sprintf("SYNC_GROUP topic=resume-topic group=resume-group member=%s generation=%d", member, generation),
		ctx,
	)
	assert.Contains(t, staleSync, "ERROR: GEN_MISMATCH")

	staleCommit := ch.HandleCommand(
		fmt.Sprintf("COMMIT_OFFSET topic=resume-topic partition=0 group=resume-group offset=7 member=%s generation=%d", member, generation),
		ctx,
	)
	assert.Contains(t, staleCommit, "ERROR: GEN_MISMATCH")
	_, committed := coord.GetOffset("resume-group", "resume-topic", 0)
	assert.False(t, committed)

	staleLeave := ch.HandleCommand(
		fmt.Sprintf("LEAVE_GROUP topic=resume-topic group=resume-group member=%s generation=%d", member, generation),
		ctx,
	)
	assert.Contains(t, staleLeave, "ERROR: GEN_MISMATCH")
	status, err = coord.GetGroupStatus("resume-group")
	require.NoError(t, err)
	assert.Equal(t, 2, status.MemberCount)
}
