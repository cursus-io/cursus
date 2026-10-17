package controller

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConsumerRoutePreservedAcrossForwardingAndFencesRestamp(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	registerRoutingBroker(t, state, "node-1")
	installRoutingOffsetsTopology(t, state, "node-1")
	h := newCoordinatorRoutingHandler("node-1", state, nil)
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	_, route, err := h.consumerOffsetRoute(nil, "workers")
	require.NoError(t, err)
	encoded, err := json.Marshal(route)
	require.NoError(t, err)
	wire := base64.RawURLEncoding.EncodeToString(encoded)
	decoded, err := h.parseConsumerRoute(wire, config.ConsumerOffsetsTopicName, route.Partition, NewInternalClientContext("", 0))
	require.NoError(t, err)
	require.Equal(t, route, decoded)
	snapshot := clusterController.PartitionReplicationSnapshot{Leader: route.Owner, LeaderEpoch: route.LeaderEpoch, LifecycleEpoch: route.LifecycleEpoch, PartitionCount: route.PartitionCount}
	require.NoError(t, validateConsumerRouteSnapshot(decoded, snapshot))
	snapshot.LeaderEpoch++
	require.Error(t, validateConsumerRouteSnapshot(decoded, snapshot))
	snapshot.LeaderEpoch--
	snapshot.PartitionCount++
	require.Error(t, validateConsumerRouteSnapshot(decoded, snapshot))
	_, err = h.parseConsumerRoute(wire, config.ConsumerOffsetsTopicName, route.Partition, NewClientContext("", 0))
	require.Error(t, err)
	_, err = h.parseConsumerRoute(wire, "events", route.Partition, NewInternalClientContext("", 0))
	require.Error(t, err)
	_, err = h.parseConsumerRoute(base64.RawURLEncoding.EncodeToString(append(encoded, []byte("{}")...)), config.ConsumerOffsetsTopicName, route.Partition, NewInternalClientContext("", 0))
	require.Error(t, err)
	// A newly elected destination cannot overwrite the old request's identity.
	metadata := state.GetPartitionMetadata(fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, route.Partition))
	metadata.LeaderEpoch++
	payload, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, h.Cluster.RaftManager.ApplyCommand(fmt.Sprintf("PARTITION:%s-%d", config.ConsumerOffsetsTopicName, route.Partition), payload))
	_, err = h.parseConsumerRoute(wire, config.ConsumerOffsetsTopicName, route.Partition, NewInternalClientContext("", 0))
	require.Error(t, err)
}

func TestConsumerRouteWriterPreservesPayloadAndRejectsDelayedPublish(t *testing.T) {
	h, manager, executor := newDistributedAckTestHandler(t, 1)
	require.NoError(t, manager.CreateTopic(config.ConsumerOffsetsTopicName, 1, false, false))
	state := h.Cluster.RaftManager.GetFSM()
	registerRoutingBroker(t, state, "broker-1")
	installPartitionMetadata(t, h, config.ConsumerOffsetsTopicName, []string{"broker-1"})
	close(executor.barrier)
	record := coordinator.ConsumerMetadataRecord{Version: coordinator.ConsumerMetadataRecordVersion, Type: coordinator.ConsumerMetadataRecordRegistration, Group: "workers", Topic: "events", PartitionCount: 1, Epoch: 1}
	require.NoError(t, h.writeConsumerOffsetRecord(record))
	messages, err := manager.ReadTopicPartition(config.ConsumerOffsetsTopicName, 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	want, _, err := coordinator.EncodeConsumerMetadataRecord(record)
	require.NoError(t, err)
	require.Equal(t, string(want), messages[0].Payload)
	routes, _, err := state.ConsumerMetadataRoutes([]string{"workers"})
	require.NoError(t, err)
	route := routes["workers"]
	route.LeaderEpoch--
	data, err := json.Marshal(route)
	require.NoError(t, err)
	response := h.handlePublish(fmt.Sprintf("PUBLISH topic=%s partition=0 acks=all producerId=consumer-offset-coordinator consumer_route=%s message=delayed", config.ConsumerOffsetsTopicName, base64.RawURLEncoding.EncodeToString(data)), NewInternalClientContext("", 0))
	require.Contains(t, response, "consumer_route_fenced")
	got, err := manager.ReadTopicPartition(config.ConsumerOffsetsTopicName, 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, []types.Message{messages[0]}, got)
}

func TestConsumerRouteRejectsNonOwnerOrigin(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	registerRoutingBroker(t, state, "node-1")
	installRoutingOffsetsTopology(t, state, "node-1")
	h := newCoordinatorRoutingHandler("node-2", state, nil)
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	_, _, err := h.consumerOffsetRoute(nil, "workers")
	require.ErrorContains(t, err, "not the current coordinator")
}
