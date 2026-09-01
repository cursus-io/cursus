package controller

import (
	"fmt"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/require"
)

func TestBrokerRejectsIdempotenceWithWeakAcknowledgementsBeforeAppend(t *testing.T) {
	handler, manager := newTestHandler(t)
	require.NoError(t, manager.CreateTopic("idempotent", 1, true, false))
	partition, err := manager.GetTopic("idempotent").GetPartition(0)
	require.NoError(t, err)

	for _, acks := range []string{"0", "1"} {
		before := partition.NextOffset()
		response := handler.HandleCommand(
			"PUBLISH topic=idempotent partition=0 acks="+acks+" producerId=p1 isIdempotent=true seqNum=1 epoch=1 message=value",
			NewClientContext("", 0),
		)
		require.Contains(t, response, "ERROR: invalid_acks")
		require.Equal(t, before, partition.NextOffset())
	}
}

func TestStandaloneIdempotentDuplicatesReturnOriginalOffset(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.MinInSyncReplicas = 1
	diskManager := disk.NewDiskManager(cfg)
	manager := topic.NewTopicManager(cfg, diskManager, nil)
	handler := NewCommandHandler(manager, cfg, nil, nil, nil)
	t.Cleanup(func() {
		_ = handler.Close()
		for _, name := range manager.ListTopics() {
			for _, partition := range manager.GetTopic(name).Partitions {
				partition.Close()
			}
		}
		diskManager.CloseAllHandlers()
	})
	require.NoError(t, manager.CreateTopic("single", 1, true, false))
	require.NoError(t, manager.CreateTopic("batch", 1, true, false))

	publishSingle := func(seq uint64) string {
		return handler.HandleCommand(
			"PUBLISH topic=single partition=0 acks=all producerId=p1 isIdempotent=true seqNum="+fmt.Sprint(seq)+" epoch=7 message=value",
			NewClientContext("", 0),
		)
	}
	require.Contains(t, publishSingle(1), `"last_offset":0`)
	require.Contains(t, publishSingle(2), `"last_offset":1`)
	require.Contains(t, publishSingle(1), `"last_offset":0`)

	publishBatch := func(seq uint64) string {
		data, err := util.EncodeBatchMessages("batch", 0, "-1", true, []types.Message{{
			Payload: "value", ProducerID: "p2", SeqNum: seq, Epoch: 7,
		}})
		require.NoError(t, err)
		response, err := handler.HandleBatchMessage(data, nil, NewClientContext("", 0))
		require.NoError(t, err)
		return response
	}
	require.Contains(t, publishBatch(1), `"last_offset":0`)
	require.Contains(t, publishBatch(2), `"last_offset":1`)
	require.Contains(t, publishBatch(1), `"last_offset":0`)
}

func TestBrokerRejectsIdempotentBatchWithWeakAcknowledgementsBeforeAppend(t *testing.T) {
	handler, manager := newTestHandler(t)
	require.NoError(t, manager.CreateTopic("idempotent-batch", 1, true, false))
	partition, err := manager.GetTopic("idempotent-batch").GetPartition(0)
	require.NoError(t, err)

	for _, acks := range []string{"0", "1"} {
		data, encodeErr := util.EncodeBatchMessages(
			"idempotent-batch",
			0,
			acks,
			true,
			[]types.Message{{Payload: "value", ProducerID: "p1", SeqNum: 1, Epoch: 1}},
		)
		require.NoError(t, encodeErr)
		before := partition.NextOffset()
		response, handleErr := handler.HandleBatchMessage(data, nil, NewClientContext("", 0))
		require.NoError(t, handleErr)
		require.Contains(t, response, "ERROR: invalid_acks")
		require.Equal(t, before, partition.NextOffset())
	}
}
