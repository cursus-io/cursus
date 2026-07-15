package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyPublisher struct{}

func (d *dummyPublisher) Publish(topic string, msg *types.Message) error { return nil }
func (d *dummyPublisher) CreateTopic(string, int, bool, bool) error      { return nil }

type testMockStorage struct{}

func (m *testMockStorage) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	return []types.Message{}, nil
}
func (m *testMockStorage) GetAbsoluteOffset() uint64               { return 0 }
func (m *testMockStorage) GetFirstOffset() uint64                  { return 0 }
func (m *testMockStorage) GetFlushedOffset() uint64                { return 0 }
func (m *testMockStorage) GetLatestOffset() uint64                 { return 0 }
func (m *testMockStorage) GetSegmentPath(baseOffset uint64) string { return "" }
func (m *testMockStorage) AppendMessage(topic string, partition int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *testMockStorage) AppendMessageSync(topic string, partition int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *testMockStorage) AppendMessageWithOffset(topic string, partition int, msg *types.Message) error {
	return nil
}
func (m *testMockStorage) WriteBatch(batch []types.DiskMessage) error { return nil }
func (m *testMockStorage) TruncateTo(uint64) error                    { return nil }
func (m *testMockStorage) Flush()                                     {}
func (m *testMockStorage) Close() error                               { return nil }

type testMockHandlerProvider struct{}

func (m *testMockHandlerProvider) GetHandler(topic string, partitionID int) (types.StorageHandler, error) {
	return &testMockStorage{}, nil
}

func newTestHandler(t *testing.T) (*CommandHandler, *topic.TopicManager) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	hp := &testMockHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	ch := NewCommandHandler(tm, cfg, nil, nil, nil)
	t.Cleanup(func() { _ = ch.Close() })
	return ch, tm
}

func newTestHandlerWithCoordinator(t *testing.T) (*CommandHandler, *topic.TopicManager, *coordinator.Coordinator) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
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
	assert.Equal(t, "ERROR: broker_error reason=\"something went wrong\"", resp)
}

func TestValidateStreamArgs(t *testing.T) {
	ch, _ := newTestHandler(t)

	err := ch.validateStreamArgs(map[string]string{"topic": "t1", "partition": "0"})
	assert.NoError(t, err)

	err = ch.validateStreamArgs(map[string]string{"partition": "0"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing_topic")

	err = ch.validateStreamArgs(map[string]string{"topic": "t1"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing_partition")
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
	assert.Contains(t, err.Error(), "missing_topic")

	err = ch.validateConsumeArgs(map[string]string{
		"topic": "t1", "offset": "0", "member": "m1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing_partition")

	err = ch.validateConsumeArgs(map[string]string{
		"topic": "t1", "partition": "0", "member": "m1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing_offset")

	err = ch.validateConsumeArgs(map[string]string{
		"topic": "t1", "partition": "0", "offset": "0",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing_member")
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

	t.Run("invalid_partition", func(t *testing.T) {
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

	t.Run("invalid_offset", func(t *testing.T) {
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

	t.Run("invalid_partition", func(t *testing.T) {
		_, _, err := ch.getTopicAndPartition("gtp-topic", 999)
		assert.Error(t, err)
	})
}

func TestHandleCreate_InvalidPartitions(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=t1 partitions=-1", ctx)
	assert.Contains(t, resp, "invalid_partitions")

	resp = ch.HandleCommand("CREATE topic=t2 partitions=abc", ctx)
	assert.Contains(t, resp, "invalid_partitions")
}

func TestHandleCreate_InvalidReplicationFactor(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=t1 replication_factor=-1", ctx)
	assert.Contains(t, resp, "invalid_replication_factor")

	resp = ch.HandleCommand("CREATE topic=t2 replication_factor=abc", ctx)
	assert.Contains(t, resp, "invalid_replication_factor")
}

func TestHandleCreate_IdempotentFlag(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=idem-topic idempotent=true", ctx)
	assert.Contains(t, resp, "idem-topic")
	assert.Contains(t, resp, "partitions=4")
}

func TestHandleDelete_NonexistentTopic(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("DELETE topic=nonexistent", ctx)
	assert.Contains(t, resp, "topic_not_found")
}

func TestHandleList_Empty(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LIST", ctx)
	assert.Equal(t, "OK count=0 topics=", resp)
}

func TestHandleListCluster_NonDistributed(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LIST_CLUSTER", ctx)
	assert.Contains(t, resp, "distribution_not_enabled")
}

func TestHandleRegisterGroup_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("REGISTER_GROUP group=g1", ctx)
	assert.Contains(t, resp, "missing_topic")

	resp = ch.HandleCommand("REGISTER_GROUP topic=t1", ctx)
	assert.Contains(t, resp, "missing_group")
}

func TestHandleRegisterGroup_NonexistentTopic(t *testing.T) {
	ch, _, _ := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("REGISTER_GROUP topic=no-topic group=g1", ctx)
	assert.Contains(t, resp, "topic_not_found")
}

func TestHandleJoinGroup_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("JOIN_GROUP group=g1 member=m1", ctx)
	assert.Contains(t, resp, "missing_topic")

	resp = ch.HandleCommand("JOIN_GROUP topic=t1 member=m1", ctx)
	assert.Contains(t, resp, "missing_group")

	resp = ch.HandleCommand("JOIN_GROUP topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "missing_member")
}

func TestHandleSyncGroup_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("SYNC_GROUP group=g1 member=m1", ctx)
	assert.Contains(t, resp, "missing_topic")

	resp = ch.HandleCommand("SYNC_GROUP topic=t1 member=m1", ctx)
	assert.Contains(t, resp, "missing_group")

	resp = ch.HandleCommand("SYNC_GROUP topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "missing_member")
}

func TestHandleSyncGroup_NilCoordinator(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("SYNC_GROUP topic=t1 group=g1 member=m1", ctx)
	assert.Contains(t, resp, "coordinator_not_available")
}

func TestHandleSyncGroup_MemberNotFound(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("sync-topic", 2, false, false)
	_ = coord.RegisterGroup("sync-topic", "sg1", 2)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("SYNC_GROUP topic=sync-topic group=sg1 member=nonexistent generation=0", ctx)
	assert.Contains(t, resp, "member_not_found")
}

func TestHandleLeaveGroup_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LEAVE_GROUP group=g1 member=m1", ctx)
	assert.Contains(t, resp, "missing_topic")

	resp = ch.HandleCommand("LEAVE_GROUP topic=t1 member=m1", ctx)
	assert.Contains(t, resp, "missing_group")

	resp = ch.HandleCommand("LEAVE_GROUP topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "missing_member")
}

func TestHandleHeartbeat_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("HEARTBEAT group=g1 member=m1", ctx)
	assert.Contains(t, resp, "missing_topic")

	resp = ch.HandleCommand("HEARTBEAT topic=t1 member=m1", ctx)
	assert.Contains(t, resp, "missing_group")

	resp = ch.HandleCommand("HEARTBEAT topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "missing_member")
}

func TestHandleHeartbeat_NilCoordinator(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("HEARTBEAT topic=t1 group=g1 member=m1", ctx)
	assert.Contains(t, resp, "coordinator_not_available")
}

func TestHandleFetchOffset_MissingParams(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("FETCH_OFFSET partition=0 group=g1", ctx)
	assert.Contains(t, resp, "missing_topic")

	resp = ch.HandleCommand("FETCH_OFFSET topic=t1 group=g1", ctx)
	assert.Contains(t, resp, "missing_partition")

	resp = ch.HandleCommand("FETCH_OFFSET topic=t1 partition=0", ctx)
	assert.Contains(t, resp, "missing_group")
}

func TestHandleFetchOffset_InvalidPartition(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("FETCH_OFFSET topic=t1 partition=abc group=g1", ctx)
	assert.Contains(t, resp, "invalid_partition")
}

func TestHandleFetchOffset_NilCoordinator(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("FETCH_OFFSET topic=t1 partition=0 group=g1", ctx)
	assert.Contains(t, resp, "offset_manager_not_available")
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
	assert.Contains(t, resp, "missing_topic")

	resp = ch.HandleCommand("COMMIT_OFFSET topic=t1 group=g1 offset=10", ctx)
	assert.Contains(t, resp, "missing_partition")

	resp = ch.HandleCommand("COMMIT_OFFSET topic=t1 partition=0 offset=10", ctx)
	assert.Contains(t, resp, "missing_group")

	resp = ch.HandleCommand("COMMIT_OFFSET topic=t1 partition=0 group=g1", ctx)
	assert.Contains(t, resp, "missing_offset")
}

func TestHandleCommitOffset_InvalidPartition(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("COMMIT_OFFSET topic=t1 partition=abc group=g1 offset=10", ctx)
	assert.Contains(t, resp, "invalid_partition")
}

func TestHandleCommitOffset_InvalidOffset(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("COMMIT_OFFSET topic=t1 partition=0 group=g1 offset=xyz", ctx)
	assert.Contains(t, resp, "invalid_offset")
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
	assert.Contains(t, resp, "missing_group")
}

func TestHandleGroupStatus_NilCoordinator(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("GROUP_STATUS group=g1", ctx)
	assert.Contains(t, resp, "coordinator_not_available")
}

func TestHandleDescribeTopic_MissingParam(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("DESCRIBE topic=", ctx)
	assert.Contains(t, resp, "missing_topic")
}

func TestHandleDescribeTopic_NonexistentTopic(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("DESCRIBE topic=nonexistent", ctx)
	assert.Contains(t, resp, "topic_not_found")
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
	assert.Contains(t, resp, "invalid_is_idempotent")
}

func TestHandlePublish_InvalidSeqNum(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("pub-topic2", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=pub-topic2 acks=1 producerId=p1 seqNum=abc message=hi", ctx)
	assert.Contains(t, resp, "invalid_seq_num")
}

func TestHandlePublish_InvalidEpoch(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("pub-topic3", 1, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=pub-topic3 acks=1 producerId=p1 epoch=abc message=hi", ctx)
	assert.Contains(t, resp, "invalid_epoch")
}

func TestHandlePublishRejectsExternalTransactionMetadata(t *testing.T) {
	ch, tm := newTestHandler(t)
	require.NoError(t, tm.CreateTopic("txn-spoof-topic", 1, false, false))
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=txn-spoof-topic partition=0 acks=1 producerId=p1 seqNum=1 epoch=0 transactional_id=tx-spoof transaction_state=open transaction_marker=commit message=spoof", ctx)
	assert.Contains(t, resp, "transaction_metadata_forbidden")
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
	assert.Contains(t, resp, "missing_payload")
}

func TestHandleReplicateMessage_InvalidJSON(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("REPLICATE_MESSAGE payload=not-json", ctx)
	assert.Contains(t, resp, "unmarshal_failed")
}

func TestHandleReplicateMessageRejectsUnownedTransactionMetadata(t *testing.T) {
	ch, tm := newTestHandler(t)
	require.NoError(t, tm.CreateTopic("rep-spoof-topic", 1, false, false))
	ctx := NewClientContext("", 0)

	payload := `{"topic":"rep-spoof-topic","partition":0,"messages":[{"Payload":"spoof","ProducerID":"p1","SeqNum":1,"Epoch":0,"TransactionalID":"tx-spoof","TransactionState":"open"}]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE payload="+payload, ctx)
	assert.Contains(t, resp, "transaction_not_found")
}
func TestHandleReplicateMessage_TopicNotFound(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	payload := `{"topic":"no-such","partition":0,"messages":[{"payload":"hi"}]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE payload="+payload, ctx)
	assert.Contains(t, resp, "topic_not_found topic=no-such")
}

func TestHandleReplicateMessage_EmptyMessages(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("rep-topic", 1, false, false)
	ctx := NewClientContext("", 0)

	payload := `{"topic":"rep-topic","partition":0,"messages":[]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE payload="+payload, ctx)
	assert.Contains(t, resp, "empty_messages")
}

func TestHandleReplicateMessage_Success(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("rep-topic2", 1, false, false)
	ctx := NewClientContext("", 0)

	payload := `{"topic":"rep-topic2","partition":0,"messages":[{"offset":0,"payload":"hello","producer_id":"p1"}]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE payload="+payload, ctx)
	assert.Equal(t, "OK leo=1 hwm=0", resp)
}

func TestHandleReplicateMessage_InvalidPartition(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("rep-topic3", 1, false, false)
	ctx := NewClientContext("", 0)

	payload := `{"topic":"rep-topic3","partition":999,"messages":[{"payload":"hi"}]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE payload="+payload, ctx)
	assert.Contains(t, resp, "partition_not_found partition=999")
}

func TestHandleBatchCommit_NoValidOffsets(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("bc-topic", 4, false, false)
	_ = coord.RegisterGroup("bc-topic", "bc-g1", 4)
	_, _ = coord.AddConsumer("bc-g1", "bc-m1")
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("BATCH_COMMIT topic=bc-topic group=bc-g1 generation=1 member=bc-m1 invalid", ctx)
	assert.Contains(t, resp, "invalid_batch_commit_entry")
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
	assert.Contains(t, resp, "ERROR: invalid_consume_syntax")

	resp = ch.HandleCommand("CONSUME topic=t1 offset=0 member=m1", ctx)
	assert.Contains(t, resp, "ERROR: invalid_consume_syntax")
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
	assert.Contains(t, resp, "topic_not_found")
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

	resp := ch.HandleCommand(fmt.Sprintf("HEARTBEAT topic=hb-topic group=hb-g1 member=hb-m1 generation=%d", coord.GetGeneration("hb-g1")), ctx)
	assert.Contains(t, resp, "OK member=")
}

func TestHandleHeartbeat_InvalidMember(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("hb-topic2", 2, false, false)
	_ = coord.RegisterGroup("hb-topic2", "hb-g2", 2)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("HEARTBEAT topic=hb-topic2 group=hb-g2 member=nonexistent generation=0", ctx)
	assert.Contains(t, resp, "ERROR")
}

func TestHandleCommitOffset_WithCoordinator(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("commit-topic", 2, false, false)
	_ = coord.RegisterGroup("commit-topic", "commit-g1", 2)
	assignments, err := coord.AddConsumer("commit-g1", "commit-m1")
	require.NoError(t, err)
	require.Contains(t, assignments, 0)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand(fmt.Sprintf("COMMIT_OFFSET topic=commit-topic partition=0 group=commit-g1 offset=50 member=commit-m1 generation=%d", coord.GetGeneration("commit-g1")), ctx)
	assert.Equal(t, "OK", resp)

	resp = ch.HandleCommand("FETCH_OFFSET topic=commit-topic partition=0 group=commit-g1", ctx)
	assert.Equal(t, "OK offset=50", resp)
}

func TestHandleCommitOffset_ValidatesMemberGenerationAndOwnership(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("commit-validated-topic", 4, false, false))
	require.NoError(t, coord.RegisterGroup("commit-validated-topic", "commit-validated-g1", 4))
	_, err := coord.AddConsumer("commit-validated-g1", "commit-m1")
	require.NoError(t, err)
	_, err = coord.AddConsumer("commit-validated-g1", "commit-m2")
	require.NoError(t, err)
	coord.Rebalance("commit-validated-g1")
	gen := coord.GetGeneration("commit-validated-g1")
	ctx := NewClientContext("", 0)

	owned := coord.GetMemberAssignments("commit-validated-g1", "commit-m1")
	require.NotEmpty(t, owned)

	valid := fmt.Sprintf("COMMIT_OFFSET topic=commit-validated-topic partition=%d group=commit-validated-g1 offset=10 member=commit-m1 generation=%d", owned[0], gen)
	resp := ch.HandleCommand(valid, ctx)
	assert.Equal(t, "OK", resp)

	stale := fmt.Sprintf("COMMIT_OFFSET topic=commit-validated-topic partition=%d group=commit-validated-g1 offset=11 member=commit-m1 generation=%d", owned[0], gen+1)
	resp = ch.HandleCommand(stale, ctx)
	assert.Contains(t, resp, "GEN_MISMATCH")

	unknown := fmt.Sprintf("COMMIT_OFFSET topic=commit-validated-topic partition=%d group=commit-validated-g1 offset=11 member=ghost generation=%d", owned[0], gen)
	resp = ch.HandleCommand(unknown, ctx)
	assert.Contains(t, resp, "member_not_found")

	notOwned := -1
	for p := 0; p < 4; p++ {
		ownedByM1 := false
		for _, assigned := range owned {
			if assigned == p {
				ownedByM1 = true
				break
			}
		}
		if !ownedByM1 {
			notOwned = p
			break
		}
	}
	require.NotEqual(t, -1, notOwned)

	notOwner := fmt.Sprintf("COMMIT_OFFSET topic=commit-validated-topic partition=%d group=commit-validated-g1 offset=11 member=commit-m1 generation=%d", notOwned, gen)
	resp = ch.HandleCommand(notOwner, ctx)
	assert.Contains(t, resp, "NOT_OWNER")
}
func TestHandleCommitOffsetValidateOnlyDoesNotMutateOffset(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("commit-validate-only-topic", 2, false, false))
	require.NoError(t, coord.RegisterGroup("commit-validate-only-topic", "commit-validate-only-g1", 2))
	_, err := coord.AddConsumer("commit-validate-only-g1", "commit-validate-only-m1")
	require.NoError(t, err)
	coord.Rebalance("commit-validate-only-g1")
	gen := coord.GetGeneration("commit-validate-only-g1")
	owned := coord.GetMemberAssignments("commit-validate-only-g1", "commit-validate-only-m1")
	require.NotEmpty(t, owned)
	ctx := NewClientContext("", 0)

	validate := fmt.Sprintf("COMMIT_OFFSET topic=commit-validate-only-topic partition=%d group=commit-validate-only-g1 offset=10 member=commit-validate-only-m1 generation=%d validate_only=true", owned[0], gen)
	resp := ch.HandleCommand(validate, ctx)
	assert.Equal(t, "OK validated=true", resp)

	resp = ch.HandleCommand(fmt.Sprintf("FETCH_OFFSET topic=commit-validate-only-topic partition=%d group=commit-validate-only-g1", owned[0]), ctx)
	assert.Equal(t, "OK offset=0", resp)

	commit := fmt.Sprintf("COMMIT_OFFSET topic=commit-validate-only-topic partition=%d group=commit-validate-only-g1 offset=10 member=commit-validate-only-m1 generation=%d", owned[0], gen)
	resp = ch.HandleCommand(commit, ctx)
	assert.Equal(t, "OK", resp)

	regression := fmt.Sprintf("COMMIT_OFFSET topic=commit-validate-only-topic partition=%d group=commit-validate-only-g1 offset=9 member=commit-validate-only-m1 generation=%d validate_only=true", owned[0], gen)
	resp = ch.HandleCommand(regression, ctx)
	assert.Contains(t, resp, "offset_regression")
}
func TestHandleLeaveGroup_WithCoordinator(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("leave-topic", 2, false, false)
	_ = coord.RegisterGroup("leave-topic", "leave-g1", 2)
	_, _ = coord.AddConsumer("leave-g1", "leave-m1")
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand(fmt.Sprintf("LEAVE_GROUP topic=leave-topic group=leave-g1 member=leave-m1 generation=%d", coord.GetGeneration("leave-g1")), ctx)
	assert.Contains(t, resp, "left=true")
}

func TestHandleLeaveGroup_NilCoordinator(t *testing.T) {
	ch, tm := newTestHandler(t)
	_ = tm.CreateTopic("leave-topic2", 2, false, false)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LEAVE_GROUP topic=leave-topic2 group=leave-g2 member=m1 generation=1", ctx)
	assert.Contains(t, resp, "coordinator_not_available")
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
	assert.Contains(t, resp, "invalid_partition")
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
	assert.Contains(t, resp, "invalid_offset")
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
	assert.Contains(t, resp, "GEN_MISMATCH")
}

func TestHandleCreate_WithCoordinatorDefaultGroup(t *testing.T) {
	ch, _, _ := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=coord-topic partitions=2", ctx)
	assert.Contains(t, resp, "coord-topic")
	assert.Contains(t, resp, "partitions=2")
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

func TestReadFromTopic_NoMember_StatelessRead(t *testing.T) {
	ch, tm, _ := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("read-topic", 1, false, false)

	ctx := NewClientContext("", 0)
	cArgs := CommonArgs{
		TopicName:   "read-topic",
		PartitionID: 0,
		GroupName:   "g1",
		MemberID:    "",
		HasOffset:   true,
		Offset:      0,
	}

	msgs, err := ch.readFromTopic("read-topic", cArgs, ctx, 10)
	assert.NoError(t, err)
	assert.Empty(t, msgs)
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

func TestReadFromTopic_StatelessRead_UsesCachedOffset(t *testing.T) {
	ch, tm, _ := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("read-gen-topic", 1, false, false)
	tObj := tm.GetTopic("read-gen-topic")
	p, _ := tObj.GetPartition(0)
	p.SetHWM(100)

	ctx := NewClientContext("rg-g1", 0)
	ctx.OffsetCache = map[string]uint64{"read-gen-topic-0": 50}

	cArgs := CommonArgs{
		TopicName:   "read-gen-topic",
		PartitionID: 0,
		GroupName:   "rg-g1",
		MemberID:    "rg-m1",
	}

	_, err := ch.readFromTopic("read-gen-topic", cArgs, ctx, 10)
	assert.NoError(t, err)
	// Stateless read: offset cache is preserved, no generation tracking
	assert.Contains(t, ctx.OffsetCache, "read-gen-topic-0")
}

func TestReadFromTopic_UseCachedOffset(t *testing.T) {
	ch, tm, _ := newTestHandlerWithCoordinator(t)
	_ = tm.CreateTopic("read-cache-topic", 1, false, false)
	tObj := tm.GetTopic("read-cache-topic")
	p, _ := tObj.GetPartition(0)
	p.SetHWM(100)

	ctx := NewClientContext("rc-g1", 0)
	ctx.OffsetCache = map[string]uint64{"read-cache-topic-0": 25}

	cArgs := CommonArgs{
		TopicName:   "read-cache-topic",
		PartitionID: 0,
		GroupName:   "rc-g1",
		MemberID:    "rc-m1",
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

func TestHandleCommitAndFetchOffset_AllowsWildcardGroupTopic(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("wild-alpha", 1, false, false))
	require.NoError(t, coord.RegisterGroup("wild-*", "wild-group", 1))
	assignments, err := coord.AddConsumer("wild-group", "wild-member")
	require.NoError(t, err)
	require.Equal(t, []int{0}, assignments)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand(fmt.Sprintf("COMMIT_OFFSET topic=wild-alpha partition=0 group=wild-group offset=7 member=wild-member generation=%d", coord.GetGeneration("wild-group")), ctx)
	assert.Equal(t, "OK", resp)

	resp = ch.HandleCommand("FETCH_OFFSET topic=wild-alpha partition=0 group=wild-group", ctx)
	assert.Equal(t, "OK offset=7", resp)
}

func TestHandleCommitAndFetchOffset_AllowsQuestionMarkGroupTopic(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("q-1", 1, false, false))
	require.NoError(t, coord.RegisterGroup("q-?", "question-group", 1))
	assignments, err := coord.AddConsumer("question-group", "question-member")
	require.NoError(t, err)
	require.Equal(t, []int{0}, assignments)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand(fmt.Sprintf("COMMIT_OFFSET topic=q-1 partition=0 group=question-group offset=3 member=question-member generation=%d", coord.GetGeneration("question-group")), ctx)
	assert.Equal(t, "OK", resp)

	resp = ch.HandleCommand("FETCH_OFFSET topic=q-1 partition=0 group=question-group", ctx)
	assert.Equal(t, "OK offset=3", resp)
}

func TestHandleReplicateSnapshot_SavesFollowerSnapshot(t *testing.T) {
	ch, tm, _ := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("snap-rep-topic", 1, false, true))
	ctx := NewClientContext("", 0)

	payload := `{"topic":"snap-rep-topic","key":"agg-1","version":2,"partition":0,"payload":"{\"state\":\"ok\"}"}`
	resp := ch.HandleCommand("REPLICATE_SNAPSHOT payload="+payload, ctx)
	assert.Equal(t, "OK", resp)

	resp = ch.HandleCommand("READ_SNAPSHOT topic=snap-rep-topic key=agg-1", ctx)
	assert.Contains(t, resp, `"version":2`)
	assert.Contains(t, resp, `state`)
}

func TestHandleCreate_TopicPolicy(t *testing.T) {
	ch, tm := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=policy-topic partitions=2 retention_hours=24 retention_bytes=4096 partitioner=round_robin auth_policy=open", ctx)
	assert.Contains(t, resp, "partitioner=round_robin")
	assert.Contains(t, resp, "retention_hours=24")

	tObj := tm.GetTopic("policy-topic")
	require.NotNil(t, tObj)
	assert.Equal(t, topic.PartitionerRoundRobin, tObj.Policy.Partitioner)
	assert.Equal(t, 24, tObj.Policy.RetentionHours)
	assert.Equal(t, int64(4096), tObj.Policy.RetentionBytes)

	metaResp := ch.HandleCommand("METADATA topic=policy-topic", ctx)
	assert.Contains(t, metaResp, "partitioner=round_robin")
	assert.Contains(t, metaResp, "retention_bytes=4096")
}

func TestHandleCreate_InvalidTopicPolicy(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=bad-policy partitioner=random", ctx)
	assert.Contains(t, resp, "invalid_topic_policy")

	resp = ch.HandleCommand("CREATE topic=bad-retention retention_hours=-1", ctx)
	assert.Contains(t, resp, "invalid_topic_policy")
}

func TestHandleCreate_ShrinkDoesNotUpdateTopicPolicy(t *testing.T) {
	ch, tm := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=stable-policy partitions=2 auth_policy=open", ctx)
	assert.Contains(t, resp, "auth_policy=open")

	resp = ch.HandleCommand("CREATE topic=stable-policy partitions=1 auth_policy=deny_write", ctx)
	assert.Contains(t, resp, "create_topic_failed")

	tObj := tm.GetTopic("stable-policy")
	require.NotNil(t, tObj)
	assert.Equal(t, topic.AuthPolicyOpen, tObj.Policy.AuthPolicy)
}
func TestHandlePublish_DenyWriteTopicPolicy(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("CREATE topic=deny-write partitions=1 auth_policy=deny_write", ctx)
	assert.Contains(t, resp, "auth_policy=deny_write")

	resp = ch.HandleCommand("PUBLISH topic=deny-write acks=1 producerId=p1 message=blocked", ctx)
	assert.Contains(t, resp, "NOT_AUTHORIZED_FOR_TOPIC")
	assert.Contains(t, resp, "operation=write")
}

func TestReadFromTopic_DenyReadTopicPolicy(t *testing.T) {
	ch, tm, _ := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopicWithPolicy("deny-read", 1, false, false, topic.Policy{AuthPolicy: topic.AuthPolicyDenyRead}))

	ctx := NewClientContext("", 0)
	cArgs := CommonArgs{TopicName: "deny-read", PartitionID: 0, GroupName: "g1", HasOffset: true, Offset: 0}
	_, err := ch.readFromTopic("deny-read", cArgs, ctx, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NOT_AUTHORIZED_FOR_TOPIC")
}

func TestTopicPolicy_RoundRobinIgnoresMessageKey(t *testing.T) {
	_, tm := newTestHandler(t)
	require.NoError(t, tm.CreateTopicWithPolicy("rr-topic", 2, false, false, topic.Policy{Partitioner: topic.PartitionerRoundRobin}))
	tObj := tm.GetTopic("rr-topic")
	require.NotNil(t, tObj)

	msg := types.Message{Key: "same-key", ProducerID: "p1"}
	first := tObj.GetPartitionForMessage(msg)
	second := tObj.GetPartitionForMessage(msg)
	assert.NotEqual(t, first, second)
}

func TestHandlePublish_IdempotentExplicitPartitionUsesPartitionSequences(t *testing.T) {
	ch, tm := newTestHandler(t)
	require.NoError(t, tm.CreateTopic("idem-explicit-partition", 2, true, false))
	ctx := NewClientContext("", 0)

	commands := []string{
		"PUBLISH topic=idem-explicit-partition partition=0 acks=1 producerId=p1 isIdempotent=true seqNum=1 epoch=1 message=p0-1",
		"PUBLISH topic=idem-explicit-partition partition=1 acks=1 producerId=p1 isIdempotent=true seqNum=1 epoch=1 message=p1-1",
		"PUBLISH topic=idem-explicit-partition partition=0 acks=1 producerId=p1 isIdempotent=true seqNum=2 epoch=1 message=p0-2",
		"PUBLISH topic=idem-explicit-partition partition=1 acks=1 producerId=p1 isIdempotent=true seqNum=2 epoch=1 message=p1-2",
	}

	for _, cmd := range commands {
		resp := ch.HandleCommand(cmd, ctx)
		var ack types.AckResponse
		require.NoError(t, json.Unmarshal([]byte(resp), &ack), resp)
		assert.Equal(t, "OK", ack.Status)
	}
}

func TestHandlePublish_InvalidExplicitPartition(t *testing.T) {
	ch, tm := newTestHandler(t)
	require.NoError(t, tm.CreateTopic("invalid-publish-partition", 1, false, false))
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("PUBLISH topic=invalid-publish-partition partition=99 acks=1 producerId=p1 message=bad", ctx)
	assert.Contains(t, resp, "partition_not_found")
}

func TestHandleListOffsets(t *testing.T) {
	ch, tm := newTestHandler(t)
	require.NoError(t, tm.CreateTopic("offsets-topic", 2, false, false))
	tObj := tm.GetTopic("offsets-topic")
	p0, err := tObj.GetPartition(0)
	require.NoError(t, err)
	p0.UpdateLEO(7)
	p0.SetHWM(7)
	p1, err := tObj.GetPartition(1)
	require.NoError(t, err)
	p1.UpdateLEO(3)
	p1.SetHWM(3)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("LIST_OFFSETS topic=offsets-topic", ctx)
	assert.Contains(t, resp, "OK topic=offsets-topic partitions=2 offsets=")
	assert.Contains(t, resp, "P0:earliest=0:latest=0:leo=7:hwm=7")
	assert.Contains(t, resp, "P1:earliest=0:latest=0:leo=3:hwm=3")

	resp = ch.HandleCommand("LIST_OFFSETS topic=offsets-topic partition=1", ctx)
	assert.Equal(t, "OK topic=offsets-topic partitions=1 offsets=P1:earliest=0:latest=0:leo=3:hwm=3", resp)
}

func TestHandleListOffsetsErrors(t *testing.T) {
	ch, tm := newTestHandler(t)
	require.NoError(t, tm.CreateTopic("offsets-error-topic", 1, false, false))
	ctx := NewClientContext("", 0)

	assert.Contains(t, ch.HandleCommand("LIST_OFFSETS", ctx), "missing_topic")
	assert.Contains(t, ch.HandleCommand("LIST_OFFSETS topic=missing", ctx), "topic_not_found")
	assert.Contains(t, ch.HandleCommand("LIST_OFFSETS topic=offsets-error-topic partition=abc", ctx), "invalid_partition")
	assert.Contains(t, ch.HandleCommand("LIST_OFFSETS topic=offsets-error-topic partition=9", ctx), "partition_not_found")
}

func TestInitProducerIDFencesPreviousEpoch(t *testing.T) {
	ch, _, _ := newTestHandlerWithCoordinator(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("INIT_PRODUCER_ID transactional_id=tx-init", ctx)
	assert.Contains(t, resp, "producerId=")
	assert.Contains(t, resp, "epoch=0")
	producerID := valueFromOKResponse(resp, "producerId")
	require.NotEmpty(t, producerID)

	resp = ch.HandleCommand("BEGIN_TXN transactional_id=tx-init producerId="+producerID+" epoch=0", ctx)
	assert.Contains(t, resp, "state=open")
	resp = ch.HandleCommand("INIT_PRODUCER_ID transactional_id=tx-init", ctx)
	assert.Contains(t, resp, "epoch=1")
	resp = ch.HandleCommand("TXN_STATUS transactional_id=tx-init", ctx)
	assert.Contains(t, resp, "state=aborted")
	resp = ch.HandleCommand("BEGIN_TXN transactional_id=tx-init producerId="+producerID+" epoch=0", ctx)
	assert.Contains(t, resp, "transaction_begin_failed")
	resp = ch.HandleCommand("BEGIN_TXN transactional_id=tx-init producerId="+producerID+" epoch=1", ctx)
	assert.Contains(t, resp, "state=open")
}

func valueFromOKResponse(resp string, key string) string {
	for _, field := range strings.Fields(resp) {
		prefix := key + "="
		if strings.HasPrefix(field, prefix) {
			return strings.TrimPrefix(field, prefix)
		}
	}
	return ""
}
func initTransactionSession(t *testing.T, ch *CommandHandler, ctx *ClientContext, txnID string) (string, string) {
	t.Helper()
	resp := ch.HandleCommand("INIT_PRODUCER_ID transactional_id="+txnID, ctx)
	require.Contains(t, resp, "producerId=")
	producerID := valueFromOKResponse(resp, "producerId")
	epoch := valueFromOKResponse(resp, "epoch")
	require.NotEmpty(t, producerID)
	require.NotEmpty(t, epoch)
	return producerID, epoch
}
func TestTransactionPublishRequiresSeqNum(t *testing.T) {
	ch, tm, _ := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("txn-seq-topic", 1, false, false))
	ctx := NewClientContext("", 0)

	producerID, epoch := initTransactionSession(t, ch, ctx, "tx-seq")
	resp := ch.HandleCommand("BEGIN_TXN transactional_id=tx-seq producerId="+producerID+" epoch="+epoch, ctx)
	assert.Contains(t, resp, "state=open")
	resp = ch.HandleCommand("TXN_PUBLISH transactional_id=tx-seq topic=txn-seq-topic partition=0 producerId="+producerID+" epoch="+epoch+" message=missing-seq", ctx)
	assert.Contains(t, resp, "invalid_seq_num")
}
func TestTransactionCommitStagesMessagesAndOffsets(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("txn-topic", 1, false, false))
	require.NoError(t, coord.RegisterGroup("txn-topic", "txn-group", 1))
	_, err := coord.AddConsumer("txn-group", "txn-member")
	require.NoError(t, err)
	generation := coord.GetGeneration("txn-group")
	ctx := NewClientContext("", 0)

	producerID, epoch := initTransactionSession(t, ch, ctx, "tx-1")
	resp := ch.HandleCommand("BEGIN_TXN transactional_id=tx-1 producerId="+producerID+" epoch="+epoch, ctx)
	assert.Contains(t, resp, "state=open")

	resp = ch.HandleCommand("TXN_PUBLISH transactional_id=tx-1 topic=txn-topic partition=0 producerId="+producerID+" seqNum=1 epoch="+epoch+" message=created", ctx)
	assert.Contains(t, resp, "staged_messages=1")

	resp = ch.HandleCommand("SEND_OFFSETS_TO_TXN transactional_id=tx-1 producerId="+producerID+" epoch="+epoch+" topic=txn-topic group=txn-group member=txn-member generation="+strconv.Itoa(generation)+" P0:4", ctx)
	assert.Contains(t, resp, "staged_offsets=1")

	resp = ch.HandleCommand("FETCH_OFFSET topic=txn-topic partition=0 group=txn-group", ctx)
	assert.Equal(t, "OK offset=0", resp)

	resp = ch.HandleCommand("END_TXN transactional_id=tx-1 producerId="+producerID+" epoch="+epoch+" result=commit", ctx)
	assert.Contains(t, resp, "state=committed")

	resp = ch.HandleCommand("FETCH_OFFSET topic=txn-topic partition=0 group=txn-group", ctx)
	assert.Equal(t, "OK offset=4", resp)

	p, err := tm.GetTopic("txn-topic").GetPartition(0)
	require.NoError(t, err)
	nextOffsetAfterCommit := p.NextOffset()
	resp = ch.HandleCommand("END_TXN transactional_id=tx-1 producerId="+producerID+" epoch="+epoch+" result=commit", ctx)
	assert.Contains(t, resp, "state=committed")
	resp = ch.HandleCommand("FETCH_OFFSET topic=txn-topic partition=0 group=txn-group", ctx)
	assert.Equal(t, "OK offset=4", resp)
	assert.Equal(t, nextOffsetAfterCommit, p.NextOffset())

}

func TestTransactionAbortDiscardsOffsets(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("txn-abort-topic", 1, false, false))
	require.NoError(t, coord.RegisterGroup("txn-abort-topic", "txn-abort-group", 1))
	_, err := coord.AddConsumer("txn-abort-group", "txn-abort-member")
	require.NoError(t, err)
	generation := coord.GetGeneration("txn-abort-group")
	ctx := NewClientContext("", 0)

	producerID, epoch := initTransactionSession(t, ch, ctx, "tx-abort")
	resp := ch.HandleCommand("BEGIN_TXN transactional_id=tx-abort producerId="+producerID+" epoch="+epoch, ctx)
	assert.Contains(t, resp, "state=open")
	resp = ch.HandleCommand("TXN_PUBLISH transactional_id=tx-abort topic=txn-abort-topic partition=0 producerId="+producerID+" seqNum=1 epoch="+epoch+" message=discarded", ctx)
	assert.Contains(t, resp, "staged_messages=1")
	resp = ch.HandleCommand("SEND_OFFSETS_TO_TXN transactional_id=tx-abort producerId="+producerID+" epoch="+epoch+" topic=txn-abort-topic group=txn-abort-group member=txn-abort-member generation="+strconv.Itoa(generation)+" P0:9", ctx)
	assert.Contains(t, resp, "staged_offsets=1")
	resp = ch.HandleCommand("END_TXN transactional_id=tx-abort producerId="+producerID+" epoch="+epoch+" result=abort", ctx)
	assert.Contains(t, resp, "state=aborted")

	resp = ch.HandleCommand("FETCH_OFFSET topic=txn-abort-topic partition=0 group=txn-abort-group", ctx)
	assert.Equal(t, "OK offset=0", resp)

	p, err := tm.GetTopic("txn-abort-topic").GetPartition(0)
	require.NoError(t, err)
	nextOffsetAfterAbort := p.NextOffset()
	resp = ch.HandleCommand("END_TXN transactional_id=tx-abort producerId="+producerID+" epoch="+epoch+" result=abort", ctx)
	assert.Contains(t, resp, "state=aborted")
	assert.Equal(t, nextOffsetAfterAbort, p.NextOffset())
}

func TestTransactionRejectsOffsetRegressionBeforePublishing(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("txn-regression-topic", 1, false, false))
	require.NoError(t, coord.RegisterGroup("txn-regression-topic", "txn-regression-group", 1))
	_, err := coord.AddConsumer("txn-regression-group", "txn-regression-member")
	require.NoError(t, err)
	generation := coord.GetGeneration("txn-regression-group")
	require.NoError(t, coord.CommitOffset("txn-regression-group", "txn-regression-topic", 0, 10))
	ctx := NewClientContext("", 0)

	producerID, epoch := initTransactionSession(t, ch, ctx, "tx-regression")
	resp := ch.HandleCommand("BEGIN_TXN transactional_id=tx-regression producerId="+producerID+" epoch="+epoch, ctx)
	assert.Contains(t, resp, "state=open")
	resp = ch.HandleCommand("TXN_PUBLISH transactional_id=tx-regression topic=txn-regression-topic partition=0 producerId="+producerID+" seqNum=1 epoch="+epoch+" message=should-not-commit", ctx)
	assert.Contains(t, resp, "staged_messages=1")
	resp = ch.HandleCommand("SEND_OFFSETS_TO_TXN transactional_id=tx-regression producerId="+producerID+" epoch="+epoch+" topic=txn-regression-topic group=txn-regression-group member=txn-regression-member generation="+strconv.Itoa(generation)+" P0:5", ctx)
	assert.Contains(t, resp, "staged_offsets=1")

	resp = ch.HandleCommand("END_TXN transactional_id=tx-regression producerId="+producerID+" epoch="+epoch+" result=commit", ctx)
	assert.Contains(t, resp, "transaction_commit_failed")
	assert.Contains(t, resp, "offset regression")

	resp = ch.HandleCommand("FETCH_OFFSET topic=txn-regression-topic partition=0 group=txn-regression-group", ctx)
	assert.Equal(t, "OK offset=10", resp)
	resp = ch.HandleCommand("TXN_STATUS transactional_id=tx-regression", ctx)
	assert.Contains(t, resp, "state=committing")
	resp = ch.HandleCommand("INIT_PRODUCER_ID transactional_id=tx-regression", ctx)
	assert.Contains(t, resp, "init_producer_failed")
}
func TestTransactionCommitRejectsStaleGroupGenerationBeforePublishing(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("txn-stale-generation-topic", 1, false, false))
	require.NoError(t, coord.RegisterGroup("txn-stale-generation-topic", "txn-stale-generation-group", 1))
	_, err := coord.AddConsumer("txn-stale-generation-group", "txn-stale-generation-member")
	require.NoError(t, err)
	generation := coord.GetGeneration("txn-stale-generation-group")
	ctx := NewClientContext("", 0)

	producerID, epoch := initTransactionSession(t, ch, ctx, "tx-stale-generation")
	resp := ch.HandleCommand("BEGIN_TXN transactional_id=tx-stale-generation producerId="+producerID+" epoch="+epoch, ctx)
	assert.Contains(t, resp, "state=open")
	resp = ch.HandleCommand("TXN_PUBLISH transactional_id=tx-stale-generation topic=txn-stale-generation-topic partition=0 producerId="+producerID+" seqNum=1 epoch="+epoch+" message=must-not-publish", ctx)
	assert.Contains(t, resp, "staged_messages=1")
	resp = ch.HandleCommand("SEND_OFFSETS_TO_TXN transactional_id=tx-stale-generation producerId="+producerID+" epoch="+epoch+" topic=txn-stale-generation-topic group=txn-stale-generation-group member=txn-stale-generation-member generation="+strconv.Itoa(generation)+" P0:3", ctx)
	assert.Contains(t, resp, "staged_offsets=1")

	p, err := tm.GetTopic("txn-stale-generation-topic").GetPartition(0)
	require.NoError(t, err)
	nextOffsetBeforeCommit := p.NextOffset()
	require.NoError(t, coord.RemoveConsumer("txn-stale-generation-group", "txn-stale-generation-member"))

	resp = ch.HandleCommand("END_TXN transactional_id=tx-stale-generation producerId="+producerID+" epoch="+epoch+" result=commit", ctx)
	assert.Contains(t, resp, "transaction_commit_failed")
	assert.Contains(t, resp, "member_not_found")
	assert.Equal(t, nextOffsetBeforeCommit, p.NextOffset())

	resp = ch.HandleCommand("FETCH_OFFSET topic=txn-stale-generation-topic partition=0 group=txn-stale-generation-group", ctx)
	assert.Equal(t, "OK offset=0", resp)
	resp = ch.HandleCommand("TXN_STATUS transactional_id=tx-stale-generation", ctx)
	assert.Contains(t, resp, "state=committing")
}
func TestRecoverPreparedTransactionsCommitsCommittingState(t *testing.T) {
	ch, tm, coord := newTestHandlerWithCoordinator(t)
	require.NoError(t, tm.CreateTopic("txn-recover-topic", 1, false, false))
	require.NoError(t, coord.RegisterGroup("txn-recover-topic", "txn-recover-group", 1))
	_, err := coord.AddConsumer("txn-recover-group", "txn-recover-member")
	require.NoError(t, err)
	generation := coord.GetGeneration("txn-recover-group")

	producerID, epoch, initErr := ch.TxnManager.InitProducer("tx-recover")
	require.NoError(t, initErr)
	require.NoError(t, ch.TxnManager.Begin("tx-recover", producerID, epoch))
	require.NoError(t, ch.TxnManager.AddMessage("tx-recover", producerID, epoch, transaction.MessageOperation{
		Topic:     "txn-recover-topic",
		Partition: 0,
		Message: types.Message{
			Payload:          "recover-me",
			ProducerID:       producerID,
			SeqNum:           1,
			Epoch:            epoch,
			TransactionalID:  "tx-recover",
			TransactionState: types.TransactionStateOpen,
		},
	}))
	require.NoError(t, ch.TxnManager.AddOffsets("tx-recover", producerID, epoch, []transaction.OffsetOperation{{Topic: "txn-recover-topic", Group: "txn-recover-group", Member: "txn-recover-member", Generation: generation, Partition: 0, Offset: 6}}))
	_, err = ch.TxnManager.PrepareCommit("tx-recover", producerID, epoch)
	require.NoError(t, err)

	require.NoError(t, ch.RecoverPreparedTransactions())
	tx, err := ch.TxnManager.Status("tx-recover")
	require.NoError(t, err)
	assert.Equal(t, transaction.StateCommitted, tx.State)
	offset, ok := coord.GetOffset("txn-recover-group", "txn-recover-topic", 0)
	require.True(t, ok)
	assert.Equal(t, uint64(6), offset)
}

func TestInternalCommandsRequireTokenInDistributedMode(t *testing.T) {
	ch, _ := newTestHandler(t)
	ch.Config.EnabledDistribution = true
	ch.Config.InternalAuthToken = "secret-token"
	ctx := NewClientContext("", 0)

	for _, cmd := range []string{
		"REPLICATE_MESSAGE payload={}",
		"REPLICATE_SNAPSHOT payload={}",
		"LIST_SNAPSHOTS topic=t partition=0",
		"FETCH_SNAPSHOT topic=t partition=0 key=k",
		"CATCHUP_SNAPSHOTS topic=t partition=0",
		"RAFT_APPLY type=REGISTER payload={}",
	} {
		resp := ch.HandleCommand(cmd, ctx)
		assert.Contains(t, resp, "internal_command_unauthorized", cmd)
	}
}

func TestInternalCommandsRejectMissingTokenConfigInDistributedMode(t *testing.T) {
	ch, _ := newTestHandler(t)
	ch.Config.EnabledDistribution = true
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand("REPLICATE_MESSAGE payload={}", ctx)
	assert.Contains(t, resp, "internal_auth_not_configured")
}

func TestInternalCommandTokenAllowsHandlerDispatch(t *testing.T) {
	ch, tm := newTestHandler(t)
	ch.Config.EnabledDistribution = true
	ch.Config.InternalAuthToken = "secret-token"
	require.NoError(t, tm.CreateTopic("secure-rep-topic", 1, false, false))
	ctx := NewClientContext("", 0)

	payload := `{"topic":"secure-rep-topic","partition":0,"messages":[{"offset":0,"payload":"hello","producer_id":"p1"}]}`
	resp := ch.HandleCommand("REPLICATE_MESSAGE internal_token=secret-token payload="+payload, ctx)
	assert.Equal(t, "OK leo=1 hwm=0", resp)
}

func TestRedactCommandSecrets(t *testing.T) {
	input := "REPLICATE_MESSAGE internal_token=super-secret payload={\"internal_token\":\"payload-value\"}"
	redacted := redactCommandSecrets(input)

	assert.NotContains(t, redacted, "super-secret")
	assert.Contains(t, redacted, "internal_token=<redacted>")
	assert.Contains(t, redacted, "payload-value")
}

func TestFormatReplicatedGroupErrorPreservesWireCode(t *testing.T) {
	err := fmt.Errorf("leader raft apply: ERROR: GEN_MISMATCH current=3 requested=2 group=g1")
	assert.Equal(t, "ERROR: GEN_MISMATCH current=3 requested=2 group=g1", formatReplicatedGroupError(err, "offset_sync_failed"))

	err = fmt.Errorf("raft apply failed: unavailable")
	assert.Equal(t, `ERROR: offset_sync_failed reason="raft apply failed: unavailable"`, formatReplicatedGroupError(err, "offset_sync_failed"))
}
