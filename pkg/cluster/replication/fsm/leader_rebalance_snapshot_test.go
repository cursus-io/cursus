package fsm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeaderRebalanceSnapshotIsDetached(t *testing.T) {
	state := adminTestFSM(t, "active")
	snapshot := state.GetLeaderRebalanceSnapshot()

	definition := snapshot.Topics["orders"]
	definition.Partitions = 99
	snapshot.Topics["orders"] = definition
	metadata := snapshot.Partitions["orders-0"]
	metadata.Replicas[0] = "mutated"
	metadata.ISR[0] = "mutated"
	metadata.RecoveryReplicas = append(metadata.RecoveryReplicas, "mutated")
	snapshot.Partitions["orders-0"] = metadata
	broker := snapshot.Brokers["broker-1"]
	broker.Status = "inactive"
	snapshot.Brokers["broker-1"] = broker

	currentDefinition, found := state.GetTopicDefinition("orders")
	require.True(t, found)
	require.Equal(t, 1, currentDefinition.Partitions)
	currentMetadata := state.GetPartitionMetadata("orders-0")
	require.Equal(t, "broker-1", currentMetadata.Replicas[0])
	require.Equal(t, "broker-1", currentMetadata.ISR[0])
	require.Empty(t, currentMetadata.RecoveryReplicas)
	require.Equal(t, "active", state.GetBroker("broker-1").Status)
}

func TestLeaderRebalanceSnapshotOfNilFSMIsEmpty(t *testing.T) {
	var state *BrokerFSM
	snapshot := state.GetLeaderRebalanceSnapshot()
	require.Empty(t, snapshot.Topics)
	require.Empty(t, snapshot.Partitions)
	require.Empty(t, snapshot.Brokers)
}
