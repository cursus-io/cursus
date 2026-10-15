package controller

import (
	"context"
	"encoding/json"
	"fmt"
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
	requests     []fsm.ReplicaCatchupRequest
	committedHWM uint64
}

func (f *recordingCatchupFetcher) FetchReplicaCatchup(_ context.Context, _ string, _ int, request fsm.ReplicaCatchupRequest) (fsm.ReplicaCatchupBatch, error) {
	f.requests = append(f.requests, request)
	committedHWM := request.CommittedHWM
	if f.committedHWM > committedHWM {
		committedHWM = f.committedHWM
	}
	if request.NextOffset == committedHWM {
		return fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{
			Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
			StartOffset: request.NextOffset, EndOffset: request.NextOffset, CommittedHWM: committedHWM,
			Leader: request.Leader, SourceBroker: request.SourceBroker,
			LeaderEpoch: request.LeaderEpoch, LifecycleEpoch: request.LifecycleEpoch, Verified: true,
		})
	}
	endOffset := request.NextOffset + 1
	return fsm.SealReplicaCatchupBatch(fsm.ReplicaCatchupBatch{
		Topic: request.Topic, Partition: request.Partition, BrokerID: request.BrokerID,
		StartOffset: request.NextOffset, EndOffset: endOffset, CommittedHWM: committedHWM,
		Leader: request.Leader, SourceBroker: request.SourceBroker,
		LeaderEpoch: request.LeaderEpoch, LifecycleEpoch: request.LifecycleEpoch,
		Verified: endOffset == committedHWM,
		Messages: []types.Message{{Offset: request.NextOffset, Payload: "backfill", LeaderEpoch: int64(request.LeaderEpoch)}},
	})
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
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	var applied []fsm.ReplicaCatchupBatch
	err = cc.RunReplicaCatchupOnceVerified(context.Background(), fetcher, func(ctx context.Context, request fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		applied = append(applied, batch)
		return applySchedulerTestBatch(ctx, partition, request, batch)
	})
	require.NoError(t, err)
	require.Len(t, fetcher.requests, 3)
	require.Len(t, applied, 3)
	require.Equal(t, []uint64{0, 1, 2}, []uint64{
		fetcher.requests[0].NextOffset, fetcher.requests[1].NextOffset, fetcher.requests[2].NextOffset,
	})
	require.Empty(t, fetcher.requests[0].PreviousRecordDigest)
	require.NotEmpty(t, fetcher.requests[1].PreviousRecordDigest)
	require.NotEmpty(t, fetcher.requests[2].PreviousRecordDigest)
	require.NotEqual(t, fetcher.requests[1].PreviousRecordDigest, fetcher.requests[2].PreviousRecordDigest)
	require.Equal(t, int64(fetcher.requests[0].LeaderEpoch), fetcher.requests[1].PreviousLeaderEpoch)
	require.Equal(t, int64(fetcher.requests[0].LeaderEpoch), fetcher.requests[2].PreviousLeaderEpoch)
}

func TestCatchupReplicaFollowsCommittedHWMThatAdvancesDuringRecovery(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = true
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
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

	cc := NewClusterController(context.Background(), cfg, &MockRaftManager{mockFSM: brokerFSM}, nil, "node-2", "127.0.0.1:9002")
	proofAttempts := 0
	cc.SetReplicaCatchupProofSubmitter(func(_ context.Context, proof fsm.ISRCatchupProof) error {
		proofAttempts++
		require.Equal(t, uint64(3), proof.CommittedHWM)
		if proofAttempts == 1 {
			return fmt.Errorf("authoritative HWM advanced")
		}
		return nil
	})
	fetcher := &recordingCatchupFetcher{committedHWM: 3}
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	request := fsm.ReplicaCatchupRequest{
		Topic: "orders", Partition: 0, BrokerID: "node-2", NextOffset: 0, CommittedHWM: 2,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: metadata.LeaderEpoch,
		LifecycleEpoch: metadata.LifecycleEpoch, MaxRecords: fsm.MaxReplicaCatchupRecords,
		SourceAddress: "127.0.0.1:9001",
	}
	request.LocalWitness, err = partition.CaptureReplicaRequestWitness(context.Background(), 0)
	require.NoError(t, err)
	err = cc.catchupReplica(context.Background(), fetcher, func(ctx context.Context, request fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
		return applySchedulerTestBatch(ctx, partition, request, batch)
	}, request)
	require.NoError(t, err)
	require.Len(t, fetcher.requests, 4)
	require.Equal(t, 2, proofAttempts)
	require.Equal(t, uint64(2), fetcher.requests[0].CommittedHWM)
	require.Equal(t, uint64(3), fetcher.requests[1].CommittedHWM)
	require.Equal(t, uint64(3), partition.GetHWM())
}

func TestValidateReplicaCatchupBatchRejectsFenceAndGap(t *testing.T) {
	request := fsm.ReplicaCatchupRequest{
		Topic: "orders", Partition: 0, BrokerID: "node-2", NextOffset: 4, CommittedHWM: 6,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 3, LifecycleEpoch: 1, MaxRecords: 2,
	}
	batch := fsm.ReplicaCatchupBatch{
		Topic: "orders", Partition: 0, BrokerID: "node-2", StartOffset: 4, CommittedHWM: 6,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 3, LifecycleEpoch: 1,
		Messages: []types.Message{{Offset: 4}, {Offset: 5}},
	}
	batch, err := fsm.SealReplicaCatchupBatch(batch)
	require.NoError(t, err)
	require.NoError(t, validateReplicaCatchupBatch(request, batch))
	advanced := batch
	advanced.CommittedHWM = 8
	advanced, err = fsm.SealReplicaCatchupBatch(advanced)
	require.NoError(t, err)
	require.NoError(t, validateReplicaCatchupBatch(request, advanced))
	regressed := batch
	regressed.CommittedHWM = request.CommittedHWM - 1
	regressed, err = fsm.SealReplicaCatchupBatch(regressed)
	require.NoError(t, err)
	require.ErrorContains(t, validateReplicaCatchupBatch(request, regressed), "boundary")
	batch.LeaderEpoch++
	require.ErrorContains(t, validateReplicaCatchupBatch(request, batch), "fence")
	batch.LeaderEpoch = request.LeaderEpoch
	batch.Messages[1].Offset = 7
	require.ErrorContains(t, validateReplicaCatchupBatch(request, batch), "checksum")
}

func applySchedulerTestBatch(ctx context.Context, partition *topic.Partition, request fsm.ReplicaCatchupRequest, batch fsm.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
	release := partition.BeginReplicationMutation()
	defer release()
	if err := partition.ValidateReplicaRequestWitness(ctx, request.LocalWitness, batch.StartOffset); err != nil {
		return nil, err
	}
	if request.RecoveryProtocol == 0 {
		if err := fsm.ValidateReplicaRequestChallenge(partition, request); err != nil {
			return nil, err
		}
	} else {
		matched, err := partition.VerifyReplicaLineage(*batch.MatchedPrefix, batch.CommittedHWM)
		if err != nil {
			return nil, err
		}
		if !matched {
			return nil, fmt.Errorf("prefix mismatch")
		}
	}
	if len(batch.Messages) != 0 {
		if err := partition.ReplicaAppendWithMode(batch.Messages, true); err != nil {
			return nil, err
		}
	}
	if !batch.Verified || batch.EndOffset != batch.CommittedHWM {
		return nil, nil
	}
	if err := partition.ApplyReplicaHWMChecked(ctx, request.LocalWitness, batch.CommittedHWM); err != nil {
		return nil, err
	}
	return partition.CompleteReplicaCatchupVerification(ctx, request.LocalWitness, batch.Leader, batch.LeaderEpoch, batch.LifecycleEpoch, batch.CommittedHWM, 0)
}
