package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockServiceDiscovery struct {
	mock.Mock
}

func (m *MockServiceDiscovery) Register() error   { return m.Called().Error(0) }
func (m *MockServiceDiscovery) Deregister() error { return m.Called().Error(0) }
func (m *MockServiceDiscovery) DiscoverBrokers() ([]fsm.BrokerInfo, error) {
	args := m.Called()
	return args.Get(0).([]fsm.BrokerInfo), args.Error(1)
}
func (m *MockServiceDiscovery) AddNode(nodeID string, addr string) (string, error) {
	args := m.Called(nodeID, addr)
	return args.String(0), args.Error(1)
}
func (m *MockServiceDiscovery) RemoveNode(nodeID string) (string, error) {
	args := m.Called(nodeID)
	return args.String(0), args.Error(1)
}
func (m *MockServiceDiscovery) UpdateHeartbeat(nodeID string) { m.Called(nodeID) }
func (m *MockServiceDiscovery) HandleHeartbeat(nodeID string, proofs []fsm.ISRCatchupProof) error {
	return m.Called(nodeID, proofs).Error(0)
}
func (m *MockServiceDiscovery) StartReconciler(ctx context.Context) { m.Called(ctx) }
func (m *MockServiceDiscovery) Reconcile()                          { m.Called() }

func clusterRoundTrip(t *testing.T, address string, command wire.Command, fields map[string]string) wire.Frame {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	connection, err := wire.ClientHandshake(conn, []wire.Compression{wire.CompressionNone})
	require.NoError(t, err)
	payload, err := wire.EncodeCommandPayload(wire.CommandPayload{Fields: fields})
	require.NoError(t, err)
	require.NoError(t, connection.WriteFrame(wire.Frame{
		Kind: wire.KindRequest, Command: command, RequestID: 1, Payload: payload,
	}))
	response, err := connection.ReadFrame()
	require.NoError(t, err)
	require.Equal(t, command, response.Command)
	require.Equal(t, uint64(1), response.RequestID)
	return response
}

func TestClusterServer_Join(t *testing.T) {
	msd := new(MockServiceDiscovery)
	server := NewClusterServer(msd)

	ln, err := server.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()

	t.Run("Join Success", func(t *testing.T) {
		msd.On("AddNode", "node1", "127.0.0.1:9001").Return("leader-addr", nil).Once()

		response := clusterRoundTrip(t, addr, wire.CommandJoinCluster, map[string]string{
			"node_id": "node1", "address": "127.0.0.1:9001",
		})
		require.Equal(t, wire.StatusOK, response.Status)
		var resp joinResponse
		err = json.Unmarshal(response.Payload, &resp)
		assert.NoError(t, err)

		assert.True(t, resp.Success)
		assert.Equal(t, "leader-addr", resp.Leader)
		msd.AssertExpectations(t)
	})

	t.Run("Join Fail", func(t *testing.T) {
		msd.On("AddNode", "node2", "127.0.0.1:9002").Return("", fmt.Errorf("error")).Once()

		response := clusterRoundTrip(t, addr, wire.CommandJoinCluster, map[string]string{
			"node_id": "node2", "address": "127.0.0.1:9002",
		})
		require.Equal(t, wire.StatusError, response.Status)
		brokerError, err := wire.DecodeError(response.Payload)
		require.NoError(t, err)
		assert.Equal(t, "cluster_join_failed", brokerError.Code)
		msd.AssertExpectations(t)
	})
}

func TestClusterServer_Heartbeat(t *testing.T) {
	msd := new(MockServiceDiscovery)
	server := NewClusterServer(msd)

	ln, err := server.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()

	msd.On("HandleHeartbeat", "node-hb", []fsm.ISRCatchupProof(nil)).Return(nil).Once()

	response := clusterRoundTrip(t, addr, wire.CommandHeartbeatCluster, map[string]string{"node_id": "node-hb"})
	require.Equal(t, wire.StatusOK, response.Status)
	assert.Contains(t, string(response.Payload), "true")
	msd.AssertExpectations(t)
}

func TestClusterServer_HeartbeatCarriesCatchupProofs(t *testing.T) {
	msd := new(MockServiceDiscovery)
	server := NewClusterServer(msd)
	listener, err := server.Start("127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	proofs := []fsm.ISRCatchupProof{{
		Topic: "orders", Partition: 0, BrokerID: "node-hb",
		CommittedHWM: 3, LocalLEO: 3, LocalHWM: 3, LeaderEpoch: 2, LifecycleEpoch: 1,
	}}
	msd.On("HandleHeartbeat", "node-hb", proofs).Return(nil).Once()
	payload, err := json.Marshal(proofs)
	require.NoError(t, err)
	response := clusterRoundTrip(t, listener.Addr().String(), wire.CommandHeartbeatCluster, map[string]string{
		"node_id": "node-hb", "catchup_proofs": string(payload),
	})
	require.Equal(t, wire.StatusOK, response.Status)
	require.Contains(t, string(response.Payload), `"success":true`)
	msd.AssertExpectations(t)
}

func TestClusterServer_List(t *testing.T) {
	msd := new(MockServiceDiscovery)
	server := NewClusterServer(msd)

	ln, err := server.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().String()

	msd.On("DiscoverBrokers").Return([]fsm.BrokerInfo{{ID: "n1"}}, nil).Once()

	response := clusterRoundTrip(t, addr, wire.CommandListCluster, nil)
	require.Equal(t, wire.StatusOK, response.Status)
	assert.Contains(t, string(response.Payload), "n1")
	msd.AssertExpectations(t)
}
