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
	return brokerFSM
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
		Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: topic.InitialLifecycleEpoch,
		MaxRecords: MaxReplicaCatchupRecords, LeaderAddress: "127.0.0.1:9000",
	}, requests[0])
	require.Empty(t, brokerFSM.BuildReplicaCatchupRequests("node-1"))
	brokerFSM.mu.Lock()
	brokerFSM.partitionMetadata["orders-0"].ISR = []string{"node-1", "node-2"}
	brokerFSM.mu.Unlock()
	require.Len(t, brokerFSM.BuildReplicaCatchupRequests("node-2"), 1, "a lagging replica must catch up even before ISR eviction commits")
}

func TestFetchReplicaCatchupReturnsBoundedRawCommittedRange(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	diskManager := disk.NewDiskManager(cfg)
	t.Cleanup(diskManager.CloseAllHandlers)
	topicManager := topic.NewTopicManager(cfg, diskManager, nil)
	require.NoError(t, topicManager.CreateTopic("orders", 1, false, false))
	partition, err := topicManager.GetTopic("orders").GetPartition(0)
	require.NoError(t, err)
	t.Cleanup(partition.Close)
	require.NoError(t, partition.EnqueueSync(types.Message{Payload: "zero"}))
	require.NoError(t, partition.EnqueueSync(types.Message{Payload: "one"}))
	require.NoError(t, partition.EnqueueSync(types.Message{Payload: "two"}))
	partition.FlushDisk()

	brokerFSM := NewBrokerFSM(topicManager, nil)
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
		Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch, MaxRecords: 1,
	}
	batch, err := brokerFSM.FetchReplicaCatchup(request)
	require.NoError(t, err)
	require.Equal(t, uint64(1), batch.StartOffset)
	require.Equal(t, uint64(2), batch.EndOffset)
	require.False(t, batch.Compacted)
	require.Equal(t, uint64(3), batch.CommittedHWM)
	require.Len(t, batch.Messages, 1)
	require.Equal(t, uint64(1), batch.Messages[0].Offset)
	require.Equal(t, "one", batch.Messages[0].Payload)

	request.BrokerID = "node-3"
	_, err = brokerFSM.FetchReplicaCatchup(request)
	require.ErrorContains(t, err, "not a configured replica")
	request.BrokerID = "node-2"
	request.LeaderEpoch++
	_, err = brokerFSM.FetchReplicaCatchup(request)
	require.ErrorContains(t, err, "stale leader fence")
}

func TestFetchReplicaCatchupCarriesCompactedOffsetRange(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
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
		Leader: "node-1", LeaderEpoch: 4, LifecycleEpoch: definition.LifecycleEpoch, MaxRecords: 10,
	})
	require.NoError(t, err)
	require.True(t, batch.Compacted)
	require.Equal(t, uint64(3), batch.EndOffset)
	require.Equal(t, []uint64{1, 2}, []uint64{batch.Messages[0].Offset, batch.Messages[1].Offset})
}
