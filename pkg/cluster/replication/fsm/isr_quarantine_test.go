package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestISRQuarantineSurvivesRestoreAndRequiresPrefixProofToClear(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	brokerFSM.mu.Lock()
	brokerFSM.brokers["node-1"].LifecycleProtocol = BrokerProtocolVersionCurrent
	brokerFSM.brokers["node-2"].LifecycleProtocol = BrokerProtocolVersionCurrent
	brokerFSM.partitionMetadata["orders-0"].ISR = []string{"node-1", "node-2"}
	brokerFSM.mu.Unlock()
	command := ISRQuarantineCommand{
		Topic: "orders", Partition: 0, BrokerID: "node-2", Leader: "node-1",
		LeaderEpoch: 4, LifecycleEpoch: 1, CommittedHWM: 0,
		ExpectedISR: []string{"node-1", "node-2"}, ExpectedReplicas: []string{"node-1", "node-2"},
	}
	data, err := json.Marshal(command)
	require.NoError(t, err)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: []byte("ISR_QUARANTINE:" + string(data)), Index: 4}))

	metadata := brokerFSM.GetPartitionMetadata("orders-0")
	require.Equal(t, []string{"node-1"}, metadata.ISR)
	require.Equal(t, []string{"node-2"}, metadata.RecoveryReplicas)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: []byte("ISR_QUARANTINE:" + string(data)), Index: 5}), "quarantine replay must be idempotent")
	health := brokerFSM.EvaluateTopology(1)
	require.Equal(t, 1, health.RecoveryPending)
	require.ErrorContains(t, health.ReadinessError(), "recovery_pending=1")
	legacyRegistration := brokerFSM.Apply(&raft.Log{Data: []byte(fmt.Sprintf(
		`REGISTER:{"id":"node-3","addr":"127.0.0.1:7003","status":"active","lifecycle_protocol":%d}`,
		ReplicaGapRecoveryProtocolVersion-1,
	)), Index: 5})
	require.ErrorContains(t, resultError(legacyRegistration), "replica_gap_recovery_protocol")

	var encoded bytes.Buffer
	snapshot, err := brokerFSM.Snapshot()
	require.NoError(t, err)
	require.NoError(t, snapshot.Persist(&MockSnapshotSink{Writer: &encoded}))
	restored := newTestFSM()
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(encoded.Bytes()))))
	require.Equal(t, []string{"node-2"}, restored.GetPartitionMetadata("orders-0").RecoveryReplicas)

	proofs := brokerFSM.BuildISRCatchupProofs("node-2")
	require.Len(t, proofs, 1)
	proofData, err := json.Marshal(proofs[0])
	require.NoError(t, err)
	require.Nil(t, brokerFSM.Apply(&raft.Log{Data: []byte("ISR_CATCHUP:" + string(proofData)), Index: 6}))
	metadata = brokerFSM.GetPartitionMetadata("orders-0")
	require.Equal(t, metadata.Replicas, metadata.ISR)
	require.Empty(t, metadata.RecoveryReplicas)
	require.NoError(t, brokerFSM.EvaluateTopology(1).ReadinessError())
}

func TestISRQuarantineRequiresClusterProtocolUpgrade(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	brokerFSM.mu.Lock()
	brokerFSM.brokers["node-1"].LifecycleProtocol = BrokerProtocolVersionCurrent
	brokerFSM.brokers["node-2"].LifecycleProtocol = ReplicaGapRecoveryProtocolVersion - 1
	brokerFSM.partitionMetadata["orders-0"].ISR = []string{"node-1", "node-2"}
	brokerFSM.mu.Unlock()
	command := ISRQuarantineCommand{
		Topic: "orders", Partition: 0, BrokerID: "node-2", Leader: "node-1",
		LeaderEpoch: 4, LifecycleEpoch: 1, CommittedHWM: 0,
		ExpectedISR: []string{"node-1", "node-2"}, ExpectedReplicas: []string{"node-1", "node-2"},
	}
	data, err := json.Marshal(command)
	require.NoError(t, err)
	result := brokerFSM.Apply(&raft.Log{Data: []byte("ISR_QUARANTINE:" + string(data)), Index: 4})
	require.ErrorContains(t, resultError(result), "requires broker protocol")
}

func TestISRQuarantineRejectsStaleBoundary(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	brokerFSM.mu.Lock()
	brokerFSM.brokers["node-1"].LifecycleProtocol = BrokerProtocolVersionCurrent
	brokerFSM.brokers["node-2"].LifecycleProtocol = BrokerProtocolVersionCurrent
	brokerFSM.partitionMetadata["orders-0"].ISR = []string{"node-1", "node-2"}
	brokerFSM.mu.Unlock()
	command := ISRQuarantineCommand{
		Topic: "orders", Partition: 0, BrokerID: "node-2", Leader: "node-1",
		LeaderEpoch: 4, LifecycleEpoch: 1, CommittedHWM: 1,
		ExpectedISR: []string{"node-1", "node-2"}, ExpectedReplicas: []string{"node-1", "node-2"},
	}
	data, err := json.Marshal(command)
	require.NoError(t, err)
	result := brokerFSM.Apply(&raft.Log{Data: []byte("ISR_QUARANTINE:" + string(data)), Index: 4})
	require.ErrorContains(t, result.(error), "stale committed HWM")
}

func TestISRQuarantineRejectsStaleMembership(t *testing.T) {
	brokerFSM := newISRCatchupTestFSM(t)
	brokerFSM.mu.Lock()
	brokerFSM.brokers["node-1"].LifecycleProtocol = BrokerProtocolVersionCurrent
	brokerFSM.brokers["node-2"].LifecycleProtocol = BrokerProtocolVersionCurrent
	brokerFSM.partitionMetadata["orders-0"].ISR = []string{"node-1", "node-2"}
	brokerFSM.mu.Unlock()
	command := ISRQuarantineCommand{
		Topic: "orders", Partition: 0, BrokerID: "node-2", Leader: "node-1",
		LeaderEpoch: 4, LifecycleEpoch: 1, CommittedHWM: 0,
		ExpectedISR: []string{"node-1"}, ExpectedReplicas: []string{"node-1", "node-2"},
	}
	data, err := json.Marshal(command)
	require.NoError(t, err)
	result := brokerFSM.Apply(&raft.Log{Data: []byte("ISR_QUARANTINE:" + string(data)), Index: 4})
	require.ErrorContains(t, result.(error), "stale ISR membership")
}
