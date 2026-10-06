package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestClusterTopologyDegradationDoesNotGateNodeReadiness(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	for _, brokerID := range []string{"broker-1", "broker-2", "broker-3"} {
		payload, err := json.Marshal(fsm.BrokerInfo{
			ID: brokerID, Addr: brokerID + ":9001", Status: "active",
			LifecycleProtocol: fsm.BrokerProtocolVersionCurrent,
		})
		require.NoError(t, err)
		require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("REGISTER:"), payload...)}))
	}
	definition := topic.DefaultDefinition("orders", nil)
	definition.Partitions = 1
	definition.ReplicationFactor = 3
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: append([]byte("TOPIC:"), payload...)}))

	applyPartitionMetadata(t, state, "orders-0", fsm.PartitionMetadata{
		PartitionCount: 1, Leader: "broker-1", LeaderEpoch: 1,
		CommittedHWMKnown: true, LifecycleEpoch: topic.InitialLifecycleEpoch,
		Replicas: []string{"broker-1"}, ISR: []string{"broker-1"},
	})
	require.Error(t, state.EvaluateTopology(2).ReadinessError())

	health := NewHealthState()
	health.SetReady(true)
	response := httptest.NewRecorder()
	newHealthHandler(health).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
	require.Equal(t, http.StatusOK, response.Code)
}

func applyPartitionMetadata(t *testing.T, state *fsm.BrokerFSM, key string, metadata fsm.PartitionMetadata) {
	t.Helper()
	payload, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.Nil(t, state.Apply(&raft.Log{Data: []byte("PARTITION:" + key + ":" + string(payload))}))
}
