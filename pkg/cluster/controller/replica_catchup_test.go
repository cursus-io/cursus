package controller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type recordingCatchupFetcher struct {
	requests []fsm.ReplicaCatchupRequest
}

func (f *recordingCatchupFetcher) FetchReplicaCatchup(_ context.Context, _ string, _ int, request fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	f.requests = append(f.requests, request)
	return fsm.ReplicaCatchupBatch{
		Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
		StartOffset: request.NextOffset, CommittedHWM: request.CommittedHWM,
		Leader: request.Leader, LeaderEpoch: request.LeaderEpoch, LifecycleEpoch: request.LifecycleEpoch,
		Messages: []types.Message{{Offset: request.NextOffset, Payload: "backfill"}},
	}, nil
}

func TestRunReplicaCatchupOnceFetchesUntilCommittedHWM(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	brokerFSM := fsm.NewBrokerFSM(topicManager, nil)
	for _, broker := range []fsm.BrokerInfo{
		{ID: "node-1", Addr: "127.0.0.1:9001", Status: "active"},
		{ID: "node-2", Addr: "127.0.0.1:9002", Status: "active"},
	} {
		data, err := json.Marshal(broker)
		require.NoError(t, err)
		require.Nil(t, brokerFSM.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...)}))
	}
	definition := topic.DefaultDefinition("orders", cfg)
	definition.Partitions = 1
	definition.ReplicationFactor = 2
	command, err := json.Marshal(fsm.TopicCommand{Definition: &definition, LeaderID: "node-1"})
	require.NoError(t, err)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: append([]byte("TOPIC:"), command...)}))
	metadata := brokerFSM.GetPartitionMetadata("orders-0")
	require.NotNil(t, metadata)
	metadata.CommittedHWM = 3
	metadata.CommittedHWMKnown = true
	metadata.ISR = []string{"node-1"}
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: append([]byte("PARTITION:orders-0:"), data...)}))

	rm := &MockRaftManager{mockFSM: brokerFSM}
	cc := NewClusterController(context.Background(), cfg, rm, nil, "node-2", "127.0.0.1:9002")
	fetcher := &recordingCatchupFetcher{}
	var applied []fsm.ReplicaCatchupBatch
	err = cc.RunReplicaCatchupOnce(context.Background(), fetcher, func(batch fsm.ReplicaCatchupBatch) error {
		applied = append(applied, batch)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, fetcher.requests, 3)
	require.Len(t, applied, 3)
	require.Equal(t, []uint64{0, 1, 2}, []uint64{
		fetcher.requests[0].NextOffset, fetcher.requests[1].NextOffset, fetcher.requests[2].NextOffset,
	})
}

func TestValidateReplicaCatchupBatchRejectsFenceAndGap(t *testing.T) {
	request := fsm.ReplicaCatchupRequest{
		Topic: "orders", Partition: 0, BrokerID: "node-2", NextOffset: 4, CommittedHWM: 6,
		Leader: "node-1", LeaderEpoch: 3, LifecycleEpoch: 1, MaxRecords: 2,
	}
	batch := fsm.ReplicaCatchupBatch{
		Topic: "orders", Partition: 0, BrokerID: "node-2", StartOffset: 4, CommittedHWM: 6,
		Leader: "node-1", LeaderEpoch: 3, LifecycleEpoch: 1,
		Messages: []types.Message{{Offset: 4}, {Offset: 5}},
	}
	require.NoError(t, validateReplicaCatchupBatch(request, batch))
	batch.LeaderEpoch++
	require.ErrorContains(t, validateReplicaCatchupBatch(request, batch), "fence")
	batch.LeaderEpoch = request.LeaderEpoch
	batch.Messages[1].Offset = 7
	require.ErrorContains(t, validateReplicaCatchupBatch(request, batch), "offset")
}
