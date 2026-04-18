package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
func (m *MockServiceDiscovery) UpdateHeartbeat(nodeID string)       { m.Called(nodeID) }
func (m *MockServiceDiscovery) StartReconciler(ctx context.Context) { m.Called(ctx) }
func (m *MockServiceDiscovery) Reconcile()                          { m.Called() }

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

		conn, err := net.Dial("tcp", addr)
		assert.NoError(t, err)
		defer func() { _ = conn.Close() }()

		payload := `{"node_id":"node1","address":"127.0.0.1:9001"}`
		msg := util.EncodeMessage("cluster", "JOIN_CLUSTER "+payload)
		err = util.WriteWithLength(conn, msg)
		assert.NoError(t, err)

		respData, err := util.ReadWithLength(conn)
		assert.NoError(t, err)
		var resp joinResponse
		err = json.Unmarshal(respData, &resp)
		assert.NoError(t, err)

		assert.True(t, resp.Success)
		assert.Equal(t, "leader-addr", resp.Leader)
		msd.AssertExpectations(t)
	})

	t.Run("Join Fail", func(t *testing.T) {
		msd.On("AddNode", "node2", "127.0.0.1:9002").Return("", fmt.Errorf("error")).Once()

		conn, err := net.Dial("tcp", addr)
		assert.NoError(t, err)
		defer func() { _ = conn.Close() }()

		payload := `{"node_id":"node2","address":"127.0.0.1:9002"}`
		msg := util.EncodeMessage("cluster", "JOIN_CLUSTER "+payload)
		err = util.WriteWithLength(conn, msg)
		assert.NoError(t, err)

		respData, err := util.ReadWithLength(conn)
		assert.NoError(t, err)
		var resp joinResponse
		err = json.Unmarshal(respData, &resp)
		assert.NoError(t, err)

		assert.False(t, resp.Success)
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

	msd.On("UpdateHeartbeat", "node-hb").Return().Once()

	conn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer func() { _ = conn.Close() }()

	payload := `{"node_id":"node-hb"}`
	msg := util.EncodeMessage("cluster", "HEARTBEAT_CLUSTER "+payload)
	err = util.WriteWithLength(conn, msg)
	assert.NoError(t, err)

	respData, err := util.ReadWithLength(conn)
	assert.NoError(t, err)
	assert.Contains(t, string(respData), "true")
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

	conn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	defer func() { _ = conn.Close() }()

	msg := util.EncodeMessage("cluster", "LIST_CLUSTER")
	err = util.WriteWithLength(conn, msg)
	assert.NoError(t, err)

	respData, err := util.ReadWithLength(conn)
	assert.NoError(t, err)
	assert.Contains(t, string(respData), "n1")
	msd.AssertExpectations(t)
}
