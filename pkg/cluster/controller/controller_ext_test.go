package controller

import (
	"context"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockServiceDiscovery struct {
	mock.Mock
}

func (m *MockServiceDiscovery) Register() error {
	return m.Called().Error(0)
}

func (m *MockServiceDiscovery) Deregister() error {
	return m.Called().Error(0)
}

func (m *MockServiceDiscovery) DiscoverBrokers() ([]fsm.BrokerInfo, error) {
	args := m.Called()
	return args.Get(0).([]fsm.BrokerInfo), args.Error(1)
}

func (m *MockServiceDiscovery) AddNode(id string, addr string) (string, error) {
	args := m.Called(id, addr)
	return args.String(0), args.Error(1)
}

func (m *MockServiceDiscovery) RemoveNode(id string) (string, error) {
	args := m.Called(id)
	return args.String(0), args.Error(1)
}

func (m *MockServiceDiscovery) UpdateHeartbeat(nodeID string) {
	m.Called(nodeID)
}

func (m *MockServiceDiscovery) StartReconciler(ctx context.Context) {
	m.Called(ctx)
}

func (m *MockServiceDiscovery) Reconcile() {
	m.Called()
}

func TestClusterController_Basic(t *testing.T) {
	cfg := config.DefaultConfig()
	rm := new(MockRaftManager)
	sd := new(MockServiceDiscovery)
	brokerID := "node1"
	localAddr := "localhost:9001"

	cc := NewClusterController(context.Background(), cfg, rm, sd, brokerID, localAddr)
	assert.NotNil(t, cc)

	t.Run("IsLeader", func(t *testing.T) {
		rm.isLeader = true
		assert.True(t, cc.IsLeader())
		rm.isLeader = false
		assert.False(t, cc.IsLeader())
	})

	t.Run("GetClusterLeader", func(t *testing.T) {
		leader, err := cc.GetClusterLeader()
		assert.NoError(t, err)
		assert.Equal(t, "localhost:9001", leader)
	})

	t.Run("JoinNewBroker", func(t *testing.T) {
		sd.On("AddNode", "node2", "localhost:9002").Return("localhost:9001", nil).Once()
		err := cc.JoinNewBroker("node2", "localhost:9002")
		assert.NoError(t, err)
		sd.AssertExpectations(t)
	})

	t.Run("SetLocalProcessor", func(t *testing.T) {
		lp := &MockLocalProcessor{}
		cc.SetLocalProcessor(lp)
		assert.Equal(t, lp, cc.Router.localProcessor)
		
		cc.SetLocalProcessor(nil) // Should ignore with warning
		assert.Equal(t, lp, cc.Router.localProcessor)
	})
}

func TestClusterRouter_Forwarding(t *testing.T) {
	rm := &MockRaftManager{isLeader: false}
	lp := &MockLocalProcessor{}
	router := NewClusterRouter("node1", "localhost:9001", lp, rm, 9000)

	t.Run("ForwardToLeader - Leader is self (error case)", func(t *testing.T) {
		rm.isLeader = false
		router.LocalAddr = "localhost:9001"
		_, err := router.ForwardToLeader("test")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "node marked as leader in Raft but IsLeader() is false")
	})

	t.Run("Process locally when leader", func(t *testing.T) {
		rm.isLeader = true
		resp, err := router.ForwardToLeader("test")
		assert.NoError(t, err)
		assert.Equal(t, "OK", resp)
	})

	t.Run("ForwardToPartitionLeader - FSM nil", func(t *testing.T) {
		rm.isLeader = true
		resp, err := router.ForwardToPartitionLeader("topic1", 0, "test")
		assert.NoError(t, err)
		assert.Equal(t, "OK", resp)
	})

	t.Run("ForwardToPartitionLeader - Local Leader", func(t *testing.T) {
		rm.isLeader = true
		router.brokerID = "node1"
		resp, err := router.ForwardToPartitionLeader("non-existent", 0, "test")
		assert.NoError(t, err)
		assert.Equal(t, "OK", resp)
	})
}
