package fsm

import (
	"encoding/json"
	"testing"

	"github.com/cursus-io/cursus/pkg/topic"
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
