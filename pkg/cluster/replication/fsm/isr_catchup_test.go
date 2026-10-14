package fsm

import (
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func newISRCatchupTestFSM(t *testing.T) *BrokerFSM {
	t.Helper()
	brokerFSM := newTestFSM()
	registerActiveBroker(t, brokerFSM, "node-1")
	registerActiveBroker(t, brokerFSM, "node-2")
	topicCommand := testTopicCommand("orders", 1, 2)
	topicCommand.LeaderID = "node-1"
	command, err := json.Marshal(topicCommand)
	require.NoError(t, err)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: []byte("TOPIC:" + string(command)), Index: 3}))

	brokerFSM.mu.Lock()
	metadata := brokerFSM.partitionMetadata["orders-0"]
	metadata.ISR = []string{"node-1"}
	metadata.LeaderEpoch = 4
	metadata.CommittedHWM = 0
	metadata.CommittedHWMKnown = true
	brokerFSM.mu.Unlock()
	partition, err := brokerFSM.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	partition.MarkReplicaCatchupVerified(4, topic.InitialLifecycleEpoch, 0)
	return brokerFSM
}

func TestBuildISRCatchupProofsRequiresCurrentPrefixVerification(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	brokerFSM.mu.Lock()
	brokerFSM.partitionMetadata["orders-0"].LeaderEpoch = 5
	brokerFSM.mu.Unlock()
	require.Empty(t, brokerFSM.BuildISRCatchupProofs("node-2"))

	partition, err := brokerFSM.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	partition.MarkReplicaCatchupVerified(5, topic.InitialLifecycleEpoch, 0)
	require.Len(t, brokerFSM.BuildISRCatchupProofs("node-2"), 1)
}

func TestBuildISRCatchupProofsIncludesOnlySynchronizedOutOfISRReplica(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	proofs := brokerFSM.BuildISRCatchupProofs("node-2")
	require.Equal(t, []ISRCatchupProof{{
		Topic: "orders", Partition: 0, BrokerID: "node-2",
		CommittedHWM: 0, LocalLEO: 0, LocalHWM: 0,
		LeaderEpoch: 4, LifecycleEpoch: topic.InitialLifecycleEpoch,
	}}, proofs)
	require.Empty(t, brokerFSM.BuildISRCatchupProofs("node-1"), "existing ISR members do not produce proofs")

	partition, err := brokerFSM.tm.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	partition.UpdateLEO(1)
	require.Empty(t, brokerFSM.BuildISRCatchupProofs("node-2"), "a replica ahead of the authoritative HWM must reconcile first")
}

func TestApplyISRCatchupProofReentersISRInReplicaOrderAndIsIdempotent(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	proof := brokerFSM.BuildISRCatchupProofs("node-2")[0]
	data, err := json.Marshal(proof)
	require.NoError(t, err)

	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: []byte("ISR_CATCHUP:" + string(data)), Index: 4}))
	metadata := brokerFSM.GetPartitionMetadata("orders-0")
	require.Equal(t, metadata.Replicas, metadata.ISR)

	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: []byte("ISR_CATCHUP:" + string(data)), Index: 5}))
	require.Equal(t, metadata.ISR, brokerFSM.GetPartitionMetadata("orders-0").ISR)
}

func TestValidateISRCatchupProofRejectsFencedAndUnsynchronizedReplica(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	valid := brokerFSM.BuildISRCatchupProofs("node-2")[0]
	tests := []struct {
		name   string
		mutate func(*ISRCatchupProof)
	}{
		{name: "non replica", mutate: func(p *ISRCatchupProof) { p.BrokerID = "node-3" }},
		{name: "committed HWM", mutate: func(p *ISRCatchupProof) { p.CommittedHWM++ }},
		{name: "ahead LEO", mutate: func(p *ISRCatchupProof) { p.LocalLEO++ }},
		{name: "local HWM", mutate: func(p *ISRCatchupProof) { p.LocalHWM++ }},
		{name: "leader epoch", mutate: func(p *ISRCatchupProof) { p.LeaderEpoch++ }},
		{name: "lifecycle epoch", mutate: func(p *ISRCatchupProof) { p.LifecycleEpoch++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proof := valid
			test.mutate(&proof)
			required, err := brokerFSM.ValidateISRCatchupProof(proof)
			require.Error(t, err)
			require.False(t, required)
			require.Equal(t, []string{"node-1"}, brokerFSM.GetPartitionMetadata("orders-0").ISR)
		})
	}
}

func TestValidateISRCatchupProofRejectsReplicaBelowAuthoritativeHWM(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	brokerFSM.mu.Lock()
	brokerFSM.partitionMetadata["orders-0"].CommittedHWM = 2
	brokerFSM.mu.Unlock()

	required, err := brokerFSM.ValidateISRCatchupProof(ISRCatchupProof{
		Topic: "orders", Partition: 0, BrokerID: "node-2",
		CommittedHWM: 2, LocalLEO: 1, LocalHWM: 1,
		LeaderEpoch: 4, LifecycleEpoch: topic.InitialLifecycleEpoch,
	})
	require.ErrorContains(t, err, "not synchronized")
	require.False(t, required)
	require.Equal(t, []string{"node-1"}, brokerFSM.GetPartitionMetadata("orders-0").ISR)
}

func TestBuildReplicaCatchupRequestsUsesLocalLEOAndLeaderFence(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	brokerFSM.mu.Lock()
	brokerFSM.partitionMetadata["orders-0"].CommittedHWM = 3
	brokerFSM.mu.Unlock()

	requests := brokerFSM.BuildReplicaCatchupRequests("node-2")
	require.Len(t, requests, 1)
	require.Equal(t, ReplicaCatchupRequest{
		Topic: "orders", Partition: 0, BrokerID: "node-2", NextOffset: 0, CommittedHWM: 3,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4, LifecycleEpoch: topic.InitialLifecycleEpoch,
		MaxRecords: MaxReplicaCatchupRecords, SourceAddress: "127.0.0.1:9000",
	}, requests[0])
	require.Empty(t, brokerFSM.BuildReplicaCatchupRequests("node-1"))
	brokerFSM.mu.Lock()
	brokerFSM.partitionMetadata["orders-0"].ISR = []string{"node-1", "node-2"}
	brokerFSM.mu.Unlock()
	require.Len(t, brokerFSM.BuildReplicaCatchupRequests("node-2"), 1, "a lagging replica must catch up even before ISR eviction commits")
}

func TestFetchReplicaCatchupUsesCurrentHWMWhenFollowerObservedOlderBoundary(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	t.Cleanup(partition.Close)
	require.NoError(t, partition.ReplicaAppendWithMode([]types.Message{
		{Offset: 0, Payload: "zero", LeaderEpoch: 4},
		{Offset: 1, Payload: "one", LeaderEpoch: 4},
	}, true))
	require.NoError(t, partition.ApplyReplicaHWM(2))

	brokerFSM := NewBrokerFSM(topicManager, nil)
	registerActiveBroker(t, brokerFSM, "node-1")
	registerActiveBroker(t, brokerFSM, "node-2")
	definition := topicManager.GetTopic("orders").Definition()
	brokerFSM.mu.Lock()
	brokerFSM.topicState["orders"] = &definition
	brokerFSM.partitionMetadata["orders-0"] = &PartitionMetadata{
		Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch,
		CommittedHWM: 2, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"node-1", "node-2"}, ISR: []string{"node-1"},
	}
	brokerFSM.mu.Unlock()

	request := ReplicaCatchupRequest{
		Topic: "orders", Partition: 0, BrokerID: "node-2", NextOffset: 0, CommittedHWM: 1,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4,
		LifecycleEpoch: definition.LifecycleEpoch, MaxRecords: MaxReplicaCatchupRecords,
	}
	batch, err := brokerFSM.FetchReplicaCatchup(request)
	require.NoError(t, err)
	require.Equal(t, uint64(2), batch.CommittedHWM)
	require.Equal(t, uint64(2), batch.EndOffset)
	require.True(t, batch.Verified)

	request.CommittedHWM = 3
	_, err = brokerFSM.FetchReplicaCatchup(request)
	require.ErrorContains(t, err, "regressed")
}

func TestAdvanceReplicaCatchupRequestMovesBoundaryAndPrefixProof(t *testing.T) {
	request := ReplicaCatchupRequest{NextOffset: 2, CommittedHWM: 4}
	batch := ReplicaCatchupBatch{
		StartOffset: 2,
		EndOffset:   3,
		Messages:    []types.Message{{Offset: 2, Payload: "next", LeaderEpoch: 7}},
	}
	require.NoError(t, AdvanceReplicaCatchupRequest(&request, batch))
	require.Equal(t, uint64(3), request.NextOffset)
	require.Equal(t, int64(7), request.PreviousLeaderEpoch)
	require.NotEmpty(t, request.PreviousRecordDigest)

	verified := ReplicaCatchupBatch{StartOffset: 3, EndOffset: 3, CommittedHWM: 3, Verified: true}
	request.CommittedHWM = 3
	require.NoError(t, AdvanceReplicaCatchupRequest(&request, verified))
	require.Equal(t, uint64(3), request.NextOffset)
}

// TestBuildReplicaCatchupRequestsTruncatesTailWithoutPublishingHWM keeps local
// visibility fenced until the retained prefix has been verified.
func TestBuildReplicaCatchupRequestsTruncatesTailWithoutPublishingHWM(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	t.Cleanup(partition.Close)
	require.NoError(t, partition.ReplicaAppendWithMode([]types.Message{
		{Offset: 0, Payload: "common", LeaderEpoch: 1},
		{Offset: 1, Payload: "committed-but-unverified", LeaderEpoch: 1},
		{Offset: 2, Payload: "uncommitted-tail", LeaderEpoch: 2},
	}, true))
	require.Equal(t, uint64(3), partition.NextOffset())
	require.Zero(t, partition.GetHWM())

	brokerFSM := NewBrokerFSM(topicManager, nil)
	registerActiveBroker(t, brokerFSM, "node-1")
	registerActiveBroker(t, brokerFSM, "node-2")
	definition := topicManager.GetTopic("orders").Definition()
	brokerFSM.mu.Lock()
	brokerFSM.topicState["orders"] = &definition
	brokerFSM.partitionMetadata["orders-0"] = &PartitionMetadata{
		Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch,
		CommittedHWM: 2, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"node-1", "node-2"}, ISR: []string{"node-1"},
	}
	brokerFSM.mu.Unlock()

	requests := brokerFSM.BuildReplicaCatchupRequests("node-2")
	require.Len(t, requests, 1)
	require.Equal(t, uint64(2), requests[0].NextOffset)
	require.NotEmpty(t, requests[0].PreviousRecordDigest)
	require.Equal(t, uint64(2), partition.NextOffset())
	require.Zero(t, partition.GetHWM(), "prefix verification must precede committed HWM publication")
	require.NoError(t, partition.ReconcileCommittedHWM(1), "a verified divergence boundary must remain repairable")
	require.Equal(t, uint64(1), partition.NextOffset())
	require.Equal(t, uint64(1), partition.GetHWM())
}

// TestBuildReplicaCatchupRequestsNeverTruncatesInISRAppendTail protects appends
// that race an ISR member's metadata snapshot.
func TestBuildReplicaCatchupRequestsNeverTruncatesInISRAppendTail(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.EnabledDistribution = true
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	t.Cleanup(partition.Close)
	require.NoError(t, partition.ReplicaAppendWithMode([]types.Message{
		{Offset: 0, Payload: "committed", LeaderEpoch: 4},
		{Offset: 1, Payload: "in-flight", LeaderEpoch: 4},
	}, true))

	brokerFSM := NewBrokerFSM(topicManager, nil)
	registerActiveBroker(t, brokerFSM, "node-1")
	registerActiveBroker(t, brokerFSM, "node-2")
	definition := topicManager.GetTopic("orders").Definition()
	brokerFSM.mu.Lock()
	brokerFSM.topicState["orders"] = &definition
	brokerFSM.partitionMetadata["orders-0"] = &PartitionMetadata{
		Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch,
		CommittedHWM: 1, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"node-1", "node-2"}, ISR: []string{"node-1", "node-2"},
	}
	brokerFSM.mu.Unlock()

	require.Empty(t, brokerFSM.BuildReplicaCatchupRequests("node-2"))
	require.Equal(t, uint64(2), partition.NextOffset(), "an ISR scan must preserve a direct append beyond its metadata snapshot")
	require.Equal(t, uint64(1), partition.GetHWM(), "the scan may only publish the known committed boundary")
	messages, err := partition.ReadMessages(1, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "in-flight", messages[0].Payload)
}

func TestBuildReplicaCatchupRequestsRepairsLaggingMetadataLeaderFromActiveISR(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	brokerFSM.mu.Lock()
	brokerFSM.partitionMetadata["orders-0"].CommittedHWM = 3
	brokerFSM.partitionMetadata["orders-0"].ISR = []string{"node-2"}
	brokerFSM.mu.Unlock()

	requests := brokerFSM.BuildReplicaCatchupRequests("node-1")
	require.Len(t, requests, 1)
	require.Equal(t, "node-1", requests[0].Leader)
	require.Equal(t, "node-2", requests[0].SourceBroker)
	require.Equal(t, "127.0.0.1:9000", requests[0].SourceAddress)
}

// TestFetchReplicaCatchupReturnsBoundedRawCommittedRange verifies epoch jumps,
// fallback, response integrity, and authoritative source selection.
func TestFetchReplicaCatchupReturnsBoundedRawCommittedRange(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	t.Cleanup(partition.Close)
	require.NoError(t, partition.EnqueueSync(types.Message{Payload: "zero", LeaderEpoch: 1}))
	require.NoError(t, partition.EnqueueSync(types.Message{Payload: "one", LeaderEpoch: 1}))
	require.NoError(t, partition.EnqueueSync(types.Message{Payload: "two", LeaderEpoch: 2}))
	partition.FlushDisk()

	brokerFSM := NewBrokerFSM(topicManager, nil)
	registerActiveBroker(t, brokerFSM, "node-1")
	registerActiveBroker(t, brokerFSM, "node-2")
	definition := topicManager.GetTopic("orders").Definition()
	brokerFSM.mu.Lock()
	brokerFSM.topicState["orders"] = &definition
	brokerFSM.partitionMetadata["orders-0"] = &PartitionMetadata{
		Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch,
		CommittedHWM: 3, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"node-1", "node-2"}, ISR: []string{"node-1"},
	}
	brokerFSM.mu.Unlock()
	request := ReplicaCatchupRequest{
		Topic: "orders", Partition: 0, BrokerID: "node-2", NextOffset: 1, CommittedHWM: 3,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch, MaxRecords: 1,
	}
	batch, err := brokerFSM.FetchReplicaCatchup(request)
	require.NoError(t, err)
	require.Equal(t, uint64(1), batch.StartOffset)
	require.Equal(t, uint64(2), batch.EndOffset)
	require.False(t, batch.Compacted)
	require.Equal(t, uint64(3), batch.CommittedHWM)
	require.Equal(t, "node-1", batch.SourceBroker)
	require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
	require.Len(t, batch.Messages, 1)
	require.Equal(t, uint64(1), batch.Messages[0].Offset)
	require.Equal(t, "one", batch.Messages[0].Payload)

	divergent := types.Message{Offset: 2, Payload: "divergent", LeaderEpoch: 1}
	previousDigest, err := replicaRecordDigest(divergent)
	require.NoError(t, err)
	divergentRequest := request
	divergentRequest.NextOffset = 3
	divergentRequest.PreviousLeaderEpoch = 1
	divergentRequest.PreviousRecordDigest = previousDigest
	rollback, err := brokerFSM.FetchReplicaCatchup(divergentRequest)
	require.NoError(t, err)
	require.NotNil(t, rollback.TruncateTo)
	require.Equal(t, uint64(2), *rollback.TruncateTo)
	require.Empty(t, rollback.Messages)
	require.NoError(t, ValidateReplicaCatchupBatchDigest(rollback))

	divergentRequest.PreviousLeaderEpoch = 99
	rollback, err = brokerFSM.FetchReplicaCatchup(divergentRequest)
	require.NoError(t, err)
	require.NotNil(t, rollback.TruncateTo)
	require.Equal(t, uint64(2), *rollback.TruncateTo)

	request.BrokerID = "node-3"
	_, err = brokerFSM.FetchReplicaCatchup(request)
	require.ErrorContains(t, err, "not a configured replica")
	request.BrokerID = "node-2"
	request.LeaderEpoch++
	_, err = brokerFSM.FetchReplicaCatchup(request)
	require.ErrorContains(t, err, "stale leader fence")
	request.LeaderEpoch--
	request.SourceBroker = "node-2"
	_, err = brokerFSM.FetchReplicaCatchup(request)
	require.ErrorContains(t, err, "not an in-sync replica")

	batch.Messages[0].Payload = "tampered"
	require.ErrorContains(t, ValidateReplicaCatchupBatchDigest(batch), "checksum mismatch")

	brokerFSM.mu.Lock()
	brokerFSM.partitionMetadata["orders-0"].ISR = []string{"node-2"}
	brokerFSM.mu.Unlock()
	request.BrokerID = "node-1"
	request.SourceBroker = "node-2"
	batch, err = brokerFSM.FetchReplicaCatchup(request)
	require.NoError(t, err, "an in-sync non-leader may serve a lagging metadata leader")
	require.Equal(t, "node-2", batch.SourceBroker)
	require.NoError(t, ValidateReplicaCatchupBatchDigest(batch))
}

// TestFetchReplicaCatchupWalksBackDivergentTailWithoutEpochBoundary verifies each
// rollback proves the next prefix before authoritative records are returned.
func TestFetchReplicaCatchupWalksBackDivergentTailWithoutEpochBoundary(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		sourceEpoch, followerEpoch int64
	}{
		{"legacy", 0, 0},
		{"same epoch", 4, 4},
		{"source legacy", 0, 4},
		{"follower newer epoch", 4, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.LogDir = t.TempDir()
			cfg.DiskMinFreeBytes = 1
			cfg.DiskMinFreePercent = 0.1
			diskManager := disk.NewDiskManager(cfg)
			t.Cleanup(diskManager.CloseAllHandlers)
			topicManager := topic.NewTopicManager(cfg, diskManager, nil)
			require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
			partition, err := topicManager.GetTopic("orders").GetPartition(0)
			require.NoError(t, err)
			t.Cleanup(partition.Close)
			for _, payload := range []string{"common", "source-one", "source-two"} {
				require.NoError(t, partition.EnqueueSync(types.Message{Payload: payload, LeaderEpoch: tc.sourceEpoch}))
			}
			partition.FlushDisk()

			brokerFSM := NewBrokerFSM(topicManager, nil)
			registerActiveBroker(t, brokerFSM, "node-1")
			registerActiveBroker(t, brokerFSM, "node-2")
			definition := topicManager.GetTopic("orders").Definition()
			brokerFSM.mu.Lock()
			brokerFSM.topicState["orders"] = &definition
			brokerFSM.partitionMetadata["orders-0"] = &PartitionMetadata{
				Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch,
				CommittedHWM: 3, CommittedHWMKnown: true, PartitionCount: 1,
				Replicas: []string{"node-1", "node-2"}, ISR: []string{"node-1"},
			}
			brokerFSM.mu.Unlock()

			if tc.followerEpoch == 0 {
				_, err := brokerFSM.replicaLeaderEpochEndOffset("orders", 0, 0, 3)
				require.ErrorIs(t, err, errReplicaEpochBoundaryUnavailable)
			}

			request := ReplicaCatchupRequest{
				Topic: "orders", Partition: 0, BrokerID: "node-2", NextOffset: 3, CommittedHWM: 3,
				Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4,
				LifecycleEpoch: definition.LifecycleEpoch, MaxRecords: MaxReplicaCatchupRecords,
				PreviousLeaderEpoch: tc.followerEpoch,
			}

			request.PreviousRecordDigest, err = replicaRecordDigest(types.Message{Offset: 2, Payload: "follower-two", LeaderEpoch: tc.followerEpoch})
			require.NoError(t, err)
			firstRollback, err := brokerFSM.FetchReplicaCatchup(request)
			require.NoError(t, err)
			require.NotNil(t, firstRollback.TruncateTo)
			require.Equal(t, uint64(2), *firstRollback.TruncateTo)
			require.NoError(t, ValidateReplicaCatchupBatchDigest(firstRollback))

			request.NextOffset = *firstRollback.TruncateTo
			request.PreviousRecordDigest, err = replicaRecordDigest(types.Message{Offset: 1, Payload: "follower-one", LeaderEpoch: tc.followerEpoch})
			require.NoError(t, err)
			secondRollback, err := brokerFSM.FetchReplicaCatchup(request)
			require.NoError(t, err)
			require.NotNil(t, secondRollback.TruncateTo)
			require.Equal(t, uint64(1), *secondRollback.TruncateTo)
			require.NoError(t, ValidateReplicaCatchupBatchDigest(secondRollback))

			request.NextOffset = *secondRollback.TruncateTo
			commonPrefix, err := brokerFSM.ReadCommittedLogRange("orders", 0, 0, 1, 1)
			require.NoError(t, err)
			require.Len(t, commonPrefix, 1)
			request.PreviousRecordDigest, err = replicaRecordDigest(commonPrefix[0])
			require.NoError(t, err)
			catchup, err := brokerFSM.FetchReplicaCatchup(request)
			require.NoError(t, err)
			require.Nil(t, catchup.TruncateTo)
			require.True(t, catchup.Verified)
			require.Equal(t, uint64(3), catchup.EndOffset)
			require.Equal(t, []string{"source-one", "source-two"}, []string{catchup.Messages[0].Payload, catchup.Messages[1].Payload})
			require.NoError(t, ValidateReplicaCatchupBatchDigest(catchup))
		})
	}
}

func TestFetchReplicaCatchupRejectsPrefixProofAtOffsetZero(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	_, err := brokerFSM.FetchReplicaCatchup(ReplicaCatchupRequest{
		Topic: "orders", Partition: 0, BrokerID: "node-2", CommittedHWM: 0,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4,
		LifecycleEpoch: topic.InitialLifecycleEpoch, MaxRecords: MaxReplicaCatchupRecords,
		PreviousRecordDigest: "impossible-at-offset-zero",
	})
	require.ErrorContains(t, err, "offset zero")
}

// TestFetchReplicaCatchupCarriesCompactedOffsetRange preserves logical offsets
// when compaction has removed superseded physical records.
func TestFetchReplicaCatchupCarriesCompactedOffsetRange(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.DiskMinFreeBytes = 1
	cfg.DiskMinFreePercent = 0.1
	cfg.CleanupPolicy = config.CleanupPolicyCompact
	cfg.MinCleanableDirtyRatio = 0.01
	cfg.RetentionCheckIntervalMS = 60_000
	cfg.CompactionCheckIntervalMS = 60_000
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("state", 1, false, false))
	partition, err := topicManager.GetTopic("state").GetPartition(0)
	require.NoError(t, err)
	t.Cleanup(partition.Close)
	require.NoError(t, partition.EnqueueSync(types.Message{Key: "key", Payload: "old"}))
	require.NoError(t, partition.EnqueueSync(types.Message{Key: "key", Payload: "current"}))
	require.NoError(t, partition.EnqueueSync(types.Message{Key: "other", Payload: "retained"}))
	partition.FlushDisk()
	handlerValue, err := diskManager.GetHandler("state", 0)
	require.NoError(t, err)
	handler := handlerValue.(*disk.DiskHandler)
	require.NoError(t, handler.RollSegmentAt(handler.GetAbsoluteOffset()))
	result, err := handler.EnforceCompaction()
	require.NoError(t, err)
	require.Equal(t, 1, result.RecordsRemoved)

	brokerFSM := NewBrokerFSM(topicManager, nil)
	registerActiveBroker(t, brokerFSM, "node-1")
	registerActiveBroker(t, brokerFSM, "node-2")
	definition := topicManager.GetTopic("state").Definition()
	brokerFSM.mu.Lock()
	brokerFSM.topicState["state"] = &definition
	brokerFSM.partitionMetadata["state-0"] = &PartitionMetadata{
		Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch,
		CommittedHWM: 3, CommittedHWMKnown: true, PartitionCount: 1,
		Replicas: []string{"node-1", "node-2"}, ISR: []string{"node-1"},
	}
	brokerFSM.mu.Unlock()

	batch, err := brokerFSM.FetchReplicaCatchup(ReplicaCatchupRequest{
		Topic: "state", Partition: 0, BrokerID: "node-2", NextOffset: 0, CommittedHWM: 3,
		Leader: "node-1", SourceBroker: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch, MaxRecords: 10,
	})
	require.NoError(t, err)
	require.True(t, batch.Compacted)
	require.Equal(t, uint64(3), batch.EndOffset)
	require.Equal(t, []uint64{1, 2}, []uint64{batch.Messages[0].Offset, batch.Messages[1].Offset})
}
