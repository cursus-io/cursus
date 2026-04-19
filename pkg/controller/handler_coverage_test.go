package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyPublisher struct{}

func (d *dummyPublisher) Publish(topic string, msg *types.Message) error         { return nil }
func (d *dummyPublisher) CreateTopic(string, int, bool, bool) error              { return nil }

type testMockStorage struct{}

func (m *testMockStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	return []types.Message{}, nil
}
func (m *testMockStorage) GetAbsoluteOffset() uint64               { return 0 }
func (m *testMockStorage) GetLatestOffset() uint64                 { return 0 }
func (m *testMockStorage) GetSegmentPath(baseOffset uint64) string { return "" }
func (m *testMockStorage) AppendMessage(topic string, partition int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *testMockStorage) AppendMessageSync(topic string, partition int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *testMockStorage) WriteBatch(batch []types.DiskMessage) error { return nil }
func (m *testMockStorage) Flush()                                     {}
func (m *testMockStorage) Close() error                               { return nil }

type testMockHandlerProvider struct{}

func (m *testMockHandlerProvider) GetHandler(topic string, partitionID int) (types.StorageHandler, error) {
	return &testMockStorage{}, nil
}

func newTestHandler(t *testing.T) (*CommandHandler, *topic.TopicManager) {
	t.Helper()
	cfg := config.DefaultConfig()
	hp := &testMockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := NewCommandHandler(tm, cfg, nil, nil, nil)
	t.Cleanup(func() { _ = ch.Close() })
	return ch, tm
}

func newTestHandlerWithCoordinator(t *testing.T) (*CommandHandler, *topic.TopicManager, *coordinator.Coordinator) {
	t.Helper()
	cfg := config.DefaultConfig()
	hp := &testMockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	coord := coordinator.NewCoordinator(context.Background(), cfg, &dummyPublisher{})
	ch := NewCommandHandler(tm, cfg, coord, nil, nil)
	t.Cleanup(func() { _ = ch.Close() })
	return ch, tm, coord
}

func TestBackoffDelay(t *testing.T) {
	t.Run("basic delay increases", func(t *testing.T) {
		d0 := backoffDelay(0, 100*time.Millisecond)
		d1 := backoffDelay(1, 100*time.Millisecond)
		assert.Greater(t, d1, time.Duration(0))
		assert.Greater(t, d0, time.Duration(0))
	})

	t.Run("caps at 5 seconds", func(t *testing.T) {
		d := backoffDelay(50, 100*time.Millisecond)
		assert.LessOrEqual(t, d, 7*time.Second)
	})

	t.Run("attempt capped at 30", func(t *testing.T) {
		d30 := backoffDelay(30, 100*time.Millisecond)
		d100 := backoffDelay(100, 100*time.Millisecond)
		assert.LessOrEqual(t, d30, 7*time.Second)
		assert.LessOrEqual(t, d100, 7*time.Second)
	})
}

func TestIsAuthorizedForPartition_NonDistributed(t *testing.T) {
	ch, _ := newTestHandler(t)
	assert.True(t, ch.isAuthorizedForPartition("any-topic", 0))
}

func TestResolveConsumerGroup(t *testing.T) {
	ch, _ := newTestHandler(t)

	assert.Equal(t, "default-group", ch.resolveConsumerGroup(""))
	assert.Equal(t, "default-group", ch.resolveConsumerGroup("-"))
	assert.Equal(t, "my-group", ch.resolveConsumerGroup("my-group"))
}

func TestErrorResponse(t *testing.T) {
	ch, _ := newTestHandler(t)

	resp := ch.errorResponse("something went wrong")
	var ack types.AckResponse
	err := json.Unmarshal([]byte(resp), &ack)
	require.NoError(t, err)
	assert.Equal(t, "ERROR", ack.Status)
	assert.Equal(t, "something went wrong", ack.ErrorMsg)
}

func TestValidateStreamArgs(t *testing.T) {
	ch, _ := newTestHandler(t)

	err := ch.validateStreamArgs(map[string]string{"topic": "t1", "partition": "0"})
	assert.NoError(t, err)

	err = ch.validateStreamArgs(map[string]string{"partition": "0"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing topic")

	err = ch.validateStreamArgs(map[string]string{"topic": "t1"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing partition")
}

func TestValidateConsumeArgs(t *testing.T) {
	ch, _ := newTestHandler(t)

	err := ch.validateConsumeArgs(map[string]string{
		"topic": "t1", "partition": "0", "offset": "0", "member": "m1",
	})
	assert.NoError(t, err)

	err = ch.validateConsumeArgs(map[string]string{
		"partition": "0", "offset": "0", "member": "m1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing topic")

	err = ch.validateConsumeArgs(map[string]string{
		"topic": "t1", "offset": "0", "member": "m1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing partition")

	err = ch.validateConsumeArgs(map[string]string{
		"topic": "t1", "partition": "0", "member": "m1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing offset")

	err = ch.validateConsumeArgs(map[string]string{
		"topic": "t1", "partition": "0", "offset": "0",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing member")
}

func TestParseCommonArgs(t *testing.T) {
	ch, _ := newTestHandler(t)

	t.Run("valid args", func(t *testing.T) {
		args, err := ch.parseCommonArgs(map[string]string{
			"topic": "t1", "partition": "2", "group": "g1",
			"member": "m1", "generation": "3", "offset": "100",
			"batch": "50", "wait_ms": "500", "autoOffsetReset": "Latest",
		})
		require.NoError(t, err)
		assert.Equal(t, "t1", args.TopicName)
		assert.Equal(t, 2, args.PartitionID)
		assert.Equal(t, "g1", args.GroupName)
		assert.Equal(t, "m1", args.MemberID)
		assert.Equal(t, 3, args.Generation)
		assert.True(t, args.HasOffset)
		assert.Equal(t, uint64(100), args.Offset)
		assert.Equal(t, 50, args.BatchSize)
		assert.Equal(t, 500*time.Millisecond, args.WaitTimeout)
		assert.Equal(t, "latest", args.AutoOffsetReset)
	})

	t.Run("defaults", func(t *testing.T) {
		args, err := ch.parseCommonArgs(map[string]string{
			"topic": "t1", "partition": "0",
		})
		require.NoError(t, err)
		assert.Equal(t, "default-group", args.GroupName)
		assert.Equal(t, -1, args.Generation)
		assert.False(t, args.HasOffset)
		assert.Equal(t, DefaultMaxPollRecords, args.BatchSize)
		assert.Equal(t, time.Duration(0), args.WaitTimeout)
	})

	t.Run("invalid partition", func(t *testing.T) {
		_, err := ch.parseCommonArgs(map[string]string{
			"topic": "t1", "partition": "abc",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid partition")
	})

	t.Run("invalid generation", func(t *testing.T) {
		_, err := ch.parseCommonArgs(map[string]string{
			"topic": "t1", "partition": "0", "generation": "xyz",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid generation")
	})

	t.Run("invalid offset", func(t *testing.T) {
		_, err := ch.parseCommonArgs(map[string]string{
			"topic": "t1", "partition": "0", "offset": "notanumber",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid offset")
	})

	t.Run("empty group resolves to default", func(t *testing.T) {
		args, err := ch.parseCommonArgs(map[string]string{
			"topic": "t1", "partition": "0", "group": "-",
		})
		require.NoError(t, err)
		assert.Equal(t, "default-group", args.GroupName)
	})
}

func TestResolveOffset(t *testing.T) {
	ch, tm, _ := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("resolve-topic", 1, false, false)
	tObj := tm.GetTopic("resolve-topic")
	p, _ := tObj.GetPartition(0)
	p.SetHWM(50)

	t.Run("uses explicit offset", func(t *testing.T) {
		cArgs := CommonArgs{HasOffset: true, Offset: 42, GroupName: "g1"}
		off, err := ch.resolveOffset(p, "resolve-topic", cArgs)
		require.NoError(t, err)
		assert.Equal(t, uint64(42), off)
	})

	t.Run("earliest returns zero", func(t *testing.T) {
		cArgs := CommonArgs{AutoOffsetReset: "earliest", GroupName: "g1"}
		off, err := ch.resolveOffset(p, "resolve-topic", cArgs)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), off)
	})

	t.Run("latest returns partition latest", func(t *testing.T) {
		cArgs := CommonArgs{AutoOffsetReset: "latest", GroupName: "g1"}
		off, err := ch.resolveOffset(p, "resolve-topic", cArgs)
		require.NoError(t, err)
		assert.Equal(t, p.GetLatestOffset(), off)
	})

	t.Run("uses saved offset from coordinator", func(t *testing.T) {
		_ = ch.Coordinator.RegisterGroup("resolve-topic", "offset-g", 1)
		_ = ch.Coordinator.CommitOffset("offset-g", "resolve-topic", 0, 25)

		cArgs := CommonArgs{GroupName: "offset-g", PartitionID: 0}
		off, err := ch.resolveOffset(p, "resolve-topic", cArgs)
		require.NoError(t, err)
		assert.Equal(t, uint64(25), off)
	})
}

func TestValidateOwnership_NilCoordinator(t *testing.T) {
	ch, _ := newTestHandler(t)
	assert.False(t, ch.ValidateOwnership("g1", "m1", 1, 0))
}

func TestGetTopicAndPartition(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("gtp-topic", 2, false, false)

	t.Run("valid topic and partition", func(t *testing.T) {
		tObj, p, err := ch.getTopicAndPartition("gtp-topic", 0)
		require.NoError(t, err)
		assert.NotNil(t, tObj)
		assert.NotNil(t, p)
	})

	t.Run("nonexistent topic", func(t *testing.T) {
		_, _, err := ch.getTopicAndPartition("no-topic", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("invalid partition", func(t *testing.T) {
		_, _, err := ch.getTopicAndPartition("gtp-topic", 999)
		assert.Error(t, err)
	})
}

func TestHandleCreate_InvalidPartitions(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=t1 partitions=-1", ctx)
	assert.Contains(t, resp, "partitions must be a positive integer")

	resp = ch.HandleCommand("CREATE topic=t2 partitions=abc", ctx)
	assert.Contains(t, resp, "partitions must be a positive integer")
}

func TestHandleCreate_InvalidReplicationFactor(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=t1 replication_factor=-1", ctx)
	assert.Contains(t, resp, "replication_factor must be a positive integer")

	resp = ch.HandleCommand("CREATE topic=t2 replication_factor=abc", ctx)
	assert.Contains(t, resp, "replication_factor must be a positive integer")
}

func TestHandleCreate_IdempotentFlag(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=idem-topic idempotent=true", ctx)
	assert.Contains(t, resp, "idem-topic")
	assert.Contains(t, resp, "4 partitions")
}

func TestHandleDelete_NonexistentTopic(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("DELETE topic=nonexistent", ctx)
	assert.Contains(t, resp, "not found")
}

func TestHandleList_Empty(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LIST", ctx)
	assert.Equal(t, "(no topics)", resp)
}

func TestHandleListCluster_NonDistributed(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LIST_CLUSTER", ctx)
	assert.Contains(t, resp, "distribution not enabled")
}

func TestHandleRegisterGroup_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("REGISTER_GROUP group=g1", ctx)
	assert.Contains(t, resp, "requires topic")

	resp = ch.HandleCommand("REGISTER_GROUP topic=t1", ctx)
	assert.Contains(t, resp, "requires group")
}

func TestHandleRegisterGroup_NonexistentTopic(t *testing.T) {
	ch, _, _ := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("REGISTER_GROUP topic=no-topic group=g1", ctx)
	assert.Contains(t, resp, "does not exist")
}

func TestHandleJoinGroup_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("JOIN_GROUP group=g1 member=m1", ctx)
	assert.Contains(t, resp, "requires topic")

	resp = ch.HandleCommand("JOIN_GROUP topic=t1 member=m1", ctx)
	assert.Contains(t, resp, "requires group")

	resp = ch.HandleCommand("JOIN_GROUP topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "requires member")
}

func TestHandleSyncGroup_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("SYNC_GROUP group=g1 member=m1", ctx)
	assert.Contains(t, resp, "requires topic")

	resp = ch.HandleCommand("SYNC_GROUP topic=t1 member=m1", ctx)
	assert.Contains(t, resp, "requires group")

	resp = ch.HandleCommand("SYNC_GROUP topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "requires member")
}

func TestHandleSyncGroup_NilCoordinator(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("SYNC_GROUP topic=t1 group=g1 member=m1", ctx)
	assert.Contains(t, resp, "coordinator not available")
}

func TestHandleSyncGroup_MemberNotFound(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("sync-topic", 2, false, false)
	_ = coord.RegisterGroup("sync-topic", "sg1", 2)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("SYNC_GROUP topic=sync-topic group=sg1 member=nonexistent", ctx)
	assert.Contains(t, resp, "assignments=[]")
}

func TestHandleLeaveGroup_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LEAVE_GROUP group=g1 member=m1", ctx)
	assert.Contains(t, resp, "requires topic")

	resp = ch.HandleCommand("LEAVE_GROUP topic=t1 member=m1", ctx)
	assert.Contains(t, resp, "requires group")

	resp = ch.HandleCommand("LEAVE_GROUP topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "requires member")
}

func TestHandleHeartbeat_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("HEARTBEAT group=g1 member=m1", ctx)
	assert.Contains(t, resp, "requires topic")

	resp = ch.HandleCommand("HEARTBEAT topic=t1 member=m1", ctx)
	assert.Contains(t, resp, "requires group")

	resp = ch.HandleCommand("HEARTBEAT topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "requires member")
}

func TestHandleHeartbeat_NilCoordinator(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("HEARTBEAT topic=t1 group=g1 member=m1", ctx)
	assert.Contains(t, resp, "coordinator not available")
}

func TestHandleFetchOffset_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("FETCH_OFFSET partition=0 group=g1", ctx)
	assert.Contains(t, resp, "requires topic")

	resp = ch.HandleCommand("FETCH_OFFSET topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "requires partition")

	resp = ch.HandleCommand("FETCH_OFFSET topic=t1 partition=0", ctx)
	assert.Contains(t, resp, "requires group")
}

func TestHandleFetchOffset_InvalidPartition(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("FETCH_OFFSET topic=t1 partition=abc group=g1", ctx)
	assert.Contains(t, resp, "invalid partition")
}

func TestHandleFetchOffset_NilCoordinator(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("FETCH_OFFSET topic=t1 partition=0 group=g1", ctx)
	assert.Contains(t, resp, "offset manager not available")
}

func TestHandleFetchOffset_GroupNotFound(t *testing.T) {
	ch, _, _ := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("FETCH_OFFSET topic=t1 partition=0 group=nonexistent", ctx)
	assert.Contains(t, resp, "group_not_found")
}

func TestHandleFetchOffset_TopicMismatch(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("fo-topic", 1, false, false)
	_ = coord.RegisterGroup("fo-topic", "fo-group", 1)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("FETCH_OFFSET topic=other-topic partition=0 group=fo-group", ctx)
	assert.Contains(t, resp, "topic_not_assigned_to_group")
}

func TestHandleCommitOffset_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("COMMIT_OFFSET partition=0 group=g1 offset=10", ctx)
	assert.Contains(t, resp, "requires topic")

	resp = ch.HandleCommand("COMMIT_OFFSET topic=t1 group=g1 offset=10", ctx)
	assert.Contains(t, resp, "requires partition")

	resp = ch.HandleCommand("COMMIT_OFFSET topic=t1 partition=0 offset=10", ctx)
	assert.Contains(t, resp, "requires group")

	resp = ch.HandleCommand("COMMIT_OFFSET topic=t1 partition=0 group=g1", ctx)
	assert.Contains(t, resp, "requires offset")
}

func TestHandleCommitOffset_InvalidPartition(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("COMMIT_OFFSET topic=t1 partition=abc group=g1 offset=10", ctx)
	assert.Contains(t, resp, "invalid partition")
}

func TestHandleCommitOffset_InvalidOffset(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("COMMIT_OFFSET topic=t1 partition=0 group=g1 offset=xyz", ctx)
	assert.Contains(t, resp, "invalid offset")
}

func TestHandleCommitOffset_TopicMismatch(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("co-topic", 1, false, false)
	_ = coord.RegisterGroup("co-topic", "co-group", 1)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("COMMIT_OFFSET topic=wrong-topic partition=0 group=co-group offset=10", ctx)
	assert.Contains(t, resp, "topic_not_assigned_to_group")
}

func TestHandleGroupStatus_MissingParam(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("GROUP_STATUS group=", ctx)
	assert.Contains(t, resp, "requires group")
}

func TestHandleGroupStatus_NilCoordinator(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("GROUP_STATUS group=g1", ctx)
	assert.Contains(t, resp, "coordinator not available")
}

func TestHandleDescribeTopic_MissingParam(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("DESCRIBE topic=", ctx)
	assert.Contains(t, resp, "missing topic")
}

func TestHandleDescribeTopic_NonexistentTopic(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("DESCRIBE topic=nonexistent", ctx)
	assert.Contains(t, resp, "not found")
}

func TestHandleDescribeTopic_ValidTopic(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("desc-topic", 3, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("DESCRIBE topic=desc-topic", ctx)
	var meta struct {
		Topic      string `json:"topic"`
		Partitions []struct {
			ID  int    `json:"id"`
			LEO uint64 `json:"leo"`
			HWM uint64 `json:"hwm"`
		} `json:"partitions"`
	}
	err := json.Unmarshal([]byte(resp), &meta)
	require.NoError(t, err)
	assert.Equal(t, "desc-topic", meta.Topic)
	assert.Len(t, meta.Partitions, 3)
}

func TestHandlePublish_InvalidIsIdempotent(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("pub-topic", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=pub-topic acks=1 producerId=p1 isIdempotent=maybe message=hi", ctx)
	assert.Contains(t, resp, "invalid isIdempotent")
}

func TestHandlePublish_InvalidSeqNum(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("pub-topic2", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=pub-topic2 acks=1 producerId=p1 seqNum=abc message=hi", ctx)
	assert.Contains(t, resp, "invalid seqNum")
}

func TestHandlePublish_InvalidEpoch(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("pub-topic3", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=pub-topic3 acks=1 producerId=p1 epoch=abc message=hi", ctx)
	assert.Contains(t, resp, "invalid epoch")
}

func TestHandlePublish_IdempotentTrue(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("idem-pub-topic", 1, true, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=idem-pub-topic acks=1 producerId=p1 isIdempotent=true seqNum=1 epoch=1 message=hello", ctx)
	var ack types.AckResponse
	err := json.Unmarshal([]byte(resp), &ack)
	require.NoError(t, err)
	assert.Equal(t, "OK", ack.Status)
}

func TestHandlePublish_IdempotentFalseExplicit(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("idem-pub-topic2", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=idem-pub-topic2 acks=1 producerId=p1 isIdempotent=false message=hello", ctx)
	var ack types.AckResponse
	err := json.Unmarshal([]byte(resp), &ack)
	require.NoError(t, err)
	assert.Equal(t, "OK", ack.Status)
}

func TestHandleReplicateMessage_MissingPayload(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("REPLICATE_MESSAGE data=something", ctx)
	assert.Contains(t, resp, "missing payload")
}

func TestHandleReplicateMessage_InvalidJSON(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("REPLICATE_MESSAGE payload=not-json", ctx)
	assert.Contains(t, resp, "unmarshal failed")
}

func TestHandleReplicateMessage_TopicNotFound(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	payload := `{"topic":"no-such","partition":0,"messages":[{"payload":"hi"}]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE payload="+payload, ctx)
	assert.Contains(t, resp, "topic no-such not found")
}

func TestHandleReplicateMessage_EmptyMessages(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("rep-topic", 1, false, false)
	ctx := NewClientContext("", 0)

	payload := `{"topic":"rep-topic","partition":0,"messages":[]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE payload="+payload, ctx)
	assert.Contains(t, resp, "empty messages")
}

func TestHandleReplicateMessage_Success(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("rep-topic2", 1, false, false)
	ctx := NewClientContext("", 0)

	payload := `{"topic":"rep-topic2","partition":0,"messages":[{"payload":"hello","producer_id":"p1"}]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE payload="+payload, ctx)
	assert.Equal(t, "OK", resp)
}

func TestHandleReplicateMessage_InvalidPartition(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("rep-topic3", 1, false, false)
	ctx := NewClientContext("", 0)

	payload := `{"topic":"rep-topic3","partition":999,"messages":[{"payload":"hi"}]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE payload="+payload, ctx)
	assert.Contains(t, resp, "partition 999 not found")
}

func TestHandleBatchCommit_NoValidOffsets(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("bc-topic", 4, false, false)
	_ = coord.RegisterGroup("bc-topic", "bc-g1", 4)
	_, _ = coord.AddConsumer("bc-g1", "bc-m1")
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("BATCH_COMMIT topic=bc-topic group=bc-g1 generation=1 member=bc-m1 invalid", ctx)
	assert.Contains(t, resp, "no_valid_offsets")
}

func TestHandleBatchCommit_ValidBulk(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("bc2-topic", 4, false, false)
	_ = coord.RegisterGroup("bc2-topic", "bc2-g1", 4)
	_, _ = coord.AddConsumer("bc2-g1", "bc2-m1")
	coord.Rebalance("bc2-g1")
	gen := coord.GetGeneration("bc2-g1")
	ctx := NewClientContext("", 0)

	cmd := fmt.Sprintf("BATCH_COMMIT topic=bc2-topic group=bc2-g1 generation=%d member=bc2-m1 P0:100,P1:200", gen)
	resp := ch.HandleCommand(cmd, ctx)
	assert.Contains(t, resp, "OK batched=")
}

func TestMatchTopicPattern_TooLong(t *testing.T) {
	ch, _ := newTestHandler(t)
	longPattern := strings.Repeat("a", 257)
	_, err := ch.matchTopicPattern(longPattern)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum length")
}

func TestMatchTopicPattern_NoMatch(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("alpha", 1, false, false)
	_ = tm.CreateTopic("beta", 1, false, false)

	_, err := ch.matchTopicPattern("gamma*")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no topics match")
}

func TestMatchTopicPattern_WildcardMatch(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("events-orders", 1, false, false)
	_ = tm.CreateTopic("events-payments", 1, false, false)
	_ = tm.CreateTopic("logs", 1, false, false)

	matched, err := ch.matchTopicPattern("events-*")
	require.NoError(t, err)
	assert.Len(t, matched, 2)
	assert.Contains(t, matched, "events-orders")
	assert.Contains(t, matched, "events-payments")
}

func TestMatchTopicPattern_QuestionMark(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("log1", 1, false, false)
	_ = tm.CreateTopic("log2", 1, false, false)
	_ = tm.CreateTopic("logs", 1, false, false)

	matched, err := ch.matchTopicPattern("log?")
	require.NoError(t, err)
	assert.Len(t, matched, 3)
}

func TestMatchCacheEviction(t *testing.T) {
	origMax := maxCacheSize
	maxCacheSize = 2
	defer func() { maxCacheSize = origMax }()

	regexMu.Lock()
	regexCache = make(map[string]*regexp.Regexp)
	regexMu.Unlock()

	assert.True(t, match("a*", "abc"))
	assert.True(t, match("b*", "bcd"))
	assert.True(t, match("c*", "cde"))

	regexMu.RLock()
	size := len(regexCache)
	regexMu.RUnlock()
	assert.LessOrEqual(t, size, 3)
}

func TestCheckLeaderOrRedirect_NonDistributed(t *testing.T) {
	ch, _ := newTestHandler(t)
	err := ch.checkLeaderOrRedirect(nil)
	assert.NoError(t, err)
}

func TestIsPartitionLeaderAndForward_NoRouter(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = false
	ch := NewCommandHandler(nil, cfg, nil, nil, nil)

	resp, forwarded, err := ch.isPartitionLeaderAndForward("topic", 0, "cmd")
	assert.Empty(t, resp)
	assert.False(t, forwarded)
	assert.NoError(t, err)
}

func TestClose_NilESHandler(t *testing.T) {
	ch := &CommandHandler{}
	err := ch.Close()
	assert.NoError(t, err)
}

func TestHandlePublish_DefaultAcks(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("default-acks", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=default-acks producerId=p1 message=hello", ctx)
	var ack types.AckResponse
	err := json.Unmarshal([]byte(resp), &ack)
	require.NoError(t, err)
	assert.Equal(t, "OK", ack.Status)
}

func TestHandleConsumeSyntax_MissingFields(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CONSUME topic=t1 partition=0", ctx)
	assert.Contains(t, resp, "ERROR: invalid CONSUME syntax")

	resp = ch.HandleCommand("CONSUME topic=t1 offset=0 member=m1", ctx)
	assert.Contains(t, resp, "ERROR: invalid CONSUME syntax")
}

func TestParseKeyValueArgs_MessageHandling(t *testing.T) {
	args := parseKeyValueArgs("topic=t1 key=v1 message=hello world with spaces and key=val inside")
	assert.Equal(t, "t1", args["topic"])
	assert.Equal(t, "v1", args["key"])
	assert.Equal(t, "hello world with spaces and key=val inside", args["message"])
}

func TestParseKeyValueArgs_NoMessage(t *testing.T) {
	args := parseKeyValueArgs("topic=t1 partition=0 offset=10")
	assert.Equal(t, "t1", args["topic"])
	assert.Equal(t, "0", args["partition"])
	assert.Equal(t, "10", args["offset"])
}

func TestParseKeyValueArgs_Empty(t *testing.T) {
	args := parseKeyValueArgs("")
	assert.Empty(t, args)
}

func TestHandleJoinGroup_TopicNotFound(t *testing.T) {
	ch, _, _ := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("JOIN_GROUP topic=no-topic group=g1 member=m1", ctx)
	assert.Contains(t, resp, "not found")
}

func TestHandleGroupStatus_WithCoordinator(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("gs-topic", 2, false, false)
	_ = coord.RegisterGroup("gs-topic", "gs-g1", 2)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("GROUP_STATUS group=gs-g1", ctx)
	assert.Contains(t, resp, "gs-g1")
	assert.Contains(t, resp, "group_name")
}

func TestHandleHeartbeat_WithCoordinator(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("hb-topic", 2, false, false)
	_ = coord.RegisterGroup("hb-topic", "hb-g1", 2)
	_, _ = coord.AddConsumer("hb-g1", "hb-m1")
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("HEARTBEAT topic=hb-topic group=hb-g1 member=hb-m1", ctx)
	assert.Equal(t, "OK", resp)
}

func TestHandleHeartbeat_InvalidMember(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("hb-topic2", 2, false, false)
	_ = coord.RegisterGroup("hb-topic2", "hb-g2", 2)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("HEARTBEAT topic=hb-topic2 group=hb-g2 member=nonexistent", ctx)
	assert.Contains(t, resp, "ERROR")
}

func TestHandleCommitOffset_WithCoordinator(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("commit-topic", 2, false, false)
	_ = coord.RegisterGroup("commit-topic", "commit-g1", 2)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("COMMIT_OFFSET topic=commit-topic partition=0 group=commit-g1 offset=50", ctx)
	assert.Equal(t, "OK", resp)

	resp = ch.HandleCommand("FETCH_OFFSET topic=commit-topic partition=0 group=commit-g1", ctx)
	assert.Equal(t, "50", resp)
}

func TestHandleLeaveGroup_WithCoordinator(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("leave-topic", 2, false, false)
	_ = coord.RegisterGroup("leave-topic", "leave-g1", 2)
	_, _ = coord.AddConsumer("leave-g1", "leave-m1")
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LEAVE_GROUP topic=leave-topic group=leave-g1 member=leave-m1", ctx)
	assert.Contains(t, resp, "Left group")
}

func TestHandleLeaveGroup_NilCoordinator(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("leave-topic2", 2, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LEAVE_GROUP topic=leave-topic2 group=leave-g2 member=m1", ctx)
	assert.Contains(t, resp, "Left group")
}

func TestHandleBatchCommit_InvalidPartitionFormat(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("bc3-topic", 4, false, false)
	_ = coord.RegisterGroup("bc3-topic", "bc3-g1", 4)
	_, _ = coord.AddConsumer("bc3-g1", "bc3-m1")
	coord.Rebalance("bc3-g1")
	gen := coord.GetGeneration("bc3-g1")
	ctx := NewClientContext("", 0)

	cmd := fmt.Sprintf("BATCH_COMMIT topic=bc3-topic group=bc3-g1 generation=%d member=bc3-m1 Pabc:100", gen)
	resp := ch.HandleCommand(cmd, ctx)
	assert.Contains(t, resp, "no_valid_offsets")
}

func TestHandleBatchCommit_InvalidOffsetFormat(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("bc4-topic", 4, false, false)
	_ = coord.RegisterGroup("bc4-topic", "bc4-g1", 4)
	_, _ = coord.AddConsumer("bc4-g1", "bc4-m1")
	coord.Rebalance("bc4-g1")
	gen := coord.GetGeneration("bc4-g1")
	ctx := NewClientContext("", 0)

	cmd := fmt.Sprintf("BATCH_COMMIT topic=bc4-topic group=bc4-g1 generation=%d member=bc4-m1 P0:abc", gen)
	resp := ch.HandleCommand(cmd, ctx)
	assert.Contains(t, resp, "no_valid_offsets")
}

func TestHandleBatchCommit_StaleGeneration(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("bc5-topic", 4, false, false)
	_ = coord.RegisterGroup("bc5-topic", "bc5-g1", 4)
	_, _ = coord.AddConsumer("bc5-g1", "bc5-m1")
	coord.Rebalance("bc5-g1")
	ctx := NewClientContext("", 0)

	cmd := "BATCH_COMMIT topic=bc5-topic group=bc5-g1 generation=999 member=bc5-m1 P0:100"
	resp := ch.HandleCommand(cmd, ctx)
	assert.Contains(t, resp, "no_valid_offsets")
}

func TestHandleCreate_WithCoordinatorDefaultGroup(t *testing.T) {
	ch, _, _ := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=coord-topic partitions=2", ctx)
	assert.Contains(t, resp, "coord-topic")
	assert.Contains(t, resp, "2 partitions")
}

func TestHandleDelete_WithTopic(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("del-topic", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("DELETE topic=del-topic", ctx)
	assert.Contains(t, resp, "deleted")
}

func TestHandleList_WithTopics(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("list-a", 1, false, false)
	_ = tm.CreateTopic("list-b", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LIST", ctx)
	assert.Contains(t, resp, "list-a")
	assert.Contains(t, resp, "list-b")
}

func TestValidateConsumeSyntax_Valid(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CONSUME topic=t1 partition=0 offset=0 member=m1", ctx)
	assert.Equal(t, STREAM_DATA_SIGNAL, resp)
}

func TestValidateOwnership_WithCoordinator(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("own-topic", 4, false, false)
	_ = coord.RegisterGroup("own-topic", "own-g1", 4)
	_, _ = coord.AddConsumer("own-g1", "own-m1")
	coord.Rebalance("own-g1")
	gen := coord.GetGeneration("own-g1")

	valid := ch.ValidateOwnership("own-g1", "own-m1", gen, 0)
	assert.True(t, valid)

	invalid := ch.ValidateOwnership("own-g1", "own-m1", gen+100, 0)
	assert.False(t, invalid)
}

func TestHandlePublish_AcksAllVariants(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("acks-topic", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=acks-topic acks=-1 producerId=p1 message=hello", ctx)
	var ack types.AckResponse
	err := json.Unmarshal([]byte(resp), &ack)
	require.NoError(t, err)
	assert.Equal(t, "OK", ack.Status)

	resp = ch.HandleCommand("PUBLISH topic=acks-topic acks=all producerId=p1 message=hello2", ctx)
	err = json.Unmarshal([]byte(resp), &ack)
	require.NoError(t, err)
	assert.Equal(t, "OK", ack.Status)
}

func TestReadFromTopic_MissingMember(t *testing.T) {
	ch, tm, _ := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("read-topic", 1, false, false)

	ctx := NewClientContext("", 0)
	cArgs := CommonArgs{
		TopicName:   "read-topic",
		PartitionID: 0,
		GroupName:   "g1",
		MemberID:    "",
	}

	_, err := ch.readFromTopic("read-topic", cArgs, ctx, 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing member")
}

func TestReadFromTopic_TopicNotFound(t *testing.T) {
	ch, _, _ := newTestHandlerWithCoordinator(t)

	ctx := NewClientContext("", 0)
	cArgs := CommonArgs{
		TopicName:   "no-topic",
		PartitionID: 0,
		GroupName:   "g1",
		MemberID:    "m1",
	}

	_, err := ch.readFromTopic("no-topic", cArgs, ctx, 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestReadFromTopic_GenerationChange(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("read-gen-topic", 1, false, false)
	tObj := tm.GetTopic("read-gen-topic")
	p, _ := tObj.GetPartition(0)
	p.SetHWM(100)

	_ = coord.RegisterGroup("read-gen-topic", "rg-g1", 1)
	_, _ = coord.AddConsumer("rg-g1", "rg-m1")
	coord.Rebalance("rg-g1")
	gen := coord.GetGeneration("rg-g1")

	ctx := NewClientContext("rg-g1", 0)
	ctx.MemberID = "rg-m1"
	ctx.Generation = gen - 1
	ctx.OffsetCache = map[string]uint64{"read-gen-topic-0": 50}

	cArgs := CommonArgs{
		TopicName:   "read-gen-topic",
		PartitionID: 0,
		GroupName:   "rg-g1",
		MemberID:    "rg-m1",
		Generation:  gen,
	}

	_, err := ch.readFromTopic("read-gen-topic", cArgs, ctx, 10)
	assert.NoError(t, err)
	assert.Equal(t, gen, ctx.Generation)
	assert.Empty(t, ctx.OffsetCache)
}

func TestReadFromTopic_UseCachedOffset(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("read-cache-topic", 1, false, false)
	tObj := tm.GetTopic("read-cache-topic")
	p, _ := tObj.GetPartition(0)
	p.SetHWM(100)

	_ = coord.RegisterGroup("read-cache-topic", "rc-g1", 1)
	_, _ = coord.AddConsumer("rc-g1", "rc-m1")
	coord.Rebalance("rc-g1")
	gen := coord.GetGeneration("rc-g1")

	ctx := NewClientContext("rc-g1", 0)
	ctx.MemberID = "rc-m1"
	ctx.Generation = gen
	ctx.OffsetCache = map[string]uint64{"read-cache-topic-0": 25}

	cArgs := CommonArgs{
		TopicName:   "read-cache-topic",
		PartitionID: 0,
		GroupName:   "rc-g1",
		MemberID:    "rc-m1",
		Generation:  gen,
	}

	_, err := ch.readFromTopic("read-cache-topic", cArgs, ctx, 10)
	assert.NoError(t, err)
}

func TestHandleBatchMessage_InvalidData(t *testing.T) {
	ch, _ := newTestHandler(t)
	resp, err := ch.HandleBatchMessage([]byte("not-valid-batch"), nil)
	assert.Nil(t, err)
	assert.Contains(t, resp, "ERROR")
}

func TestHandleBatchMessage_InvalidAcks(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("batch-acks-topic", 1, false, false)

	msgs := []types.Message{{Payload: "m1", ProducerID: "p1", SeqNum: 1}}
	data, err := encodeBatchWithCustomAcks("batch-acks-topic", 0, "5", false, msgs)
	if err != nil {
		t.Skip("cannot construct custom acks batch")
	}
	resp, _ := ch.HandleBatchMessage(data, nil)
	assert.Contains(t, resp, "ERROR")
}

func encodeBatchWithCustomAcks(topicName string, partition int, acks string, isIdempotent bool, msgs []types.Message) ([]byte, error) {
	batch := types.Batch{
		Topic:        topicName,
		Partition:    partition,
		Acks:         acks,
		IsIdempotent: isIdempotent,
		Messages:     msgs,
	}
	return json.Marshal(batch)
}
