package controller

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockISRManager struct {
	mock.Mock
}

type LivenessMockISRManager struct {
	MockISRManager
	alive map[string]bool
}

func (m *LivenessMockISRManager) IsBrokerAlive(brokerID string) bool {
	return m.alive[brokerID]
}

type staticConfigurationFuture struct {
	configuration raft.Configuration
}

func (f staticConfigurationFuture) Error() error                      { return nil }
func (f staticConfigurationFuture) Index() uint64                     { return 1 }
func (f staticConfigurationFuture) Configuration() raft.Configuration { return f.configuration }

func (m *MockISRManager) HasQuorum(topic string, partition int, minISR int) bool {
	args := m.Called(topic, partition, minISR)
	return args.Bool(0)
}

func (m *MockISRManager) UpdateHeartbeat(brokerID string) {
	m.Called(brokerID)
}

func (m *MockISRManager) BuildCatchupProofs() []fsm.ISRCatchupProof {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]fsm.ISRCatchupProof)
}

func (m *MockISRManager) SubmitCatchupProofs(nodeID string, proofs []fsm.ISRCatchupProof) error {
	return m.Called(nodeID, proofs).Error(0)
}

func (m *MockISRManager) GetISR(topic string, partition int) []string {
	args := m.Called(topic, partition)
	return args.Get(0).([]string)
}

func (m *MockISRManager) ComputeISR(topic string, partition int) []string {
	args := m.Called(topic, partition)
	return args.Get(0).([]string)
}

func (m *MockISRManager) SetLeader(isLeader bool) {
	m.Called(isLeader)
}

func (m *MockISRManager) Start() {
	m.Called()
}

type ComprehensiveMockRaftManager struct {
	mock.Mock
	isLeader bool
	mockFSM  *fsm.BrokerFSM
}

func (m *ComprehensiveMockRaftManager) IsLeader() bool {
	return m.isLeader
}

func (m *ComprehensiveMockRaftManager) GetLeaderAddress() string {
	args := m.Called()
	return args.String(0)
}

func (m *ComprehensiveMockRaftManager) GetFSM() *fsm.BrokerFSM {
	return m.mockFSM
}

func (m *ComprehensiveMockRaftManager) ApplyCommand(prefix string, data []byte) error {
	args := m.Called(prefix, data)
	if args.Error(0) == nil {
		switch prefix {
		case "REGISTER":
			m.mockFSM.Apply(&raft.Log{Data: append([]byte("REGISTER:"), data...), Index: 1})
		case "DEREGISTER":
			m.mockFSM.Apply(&raft.Log{Data: append([]byte("DEREGISTER:"), data...), Index: 2})
		}
	}
	return args.Error(0)
}

func (m *ComprehensiveMockRaftManager) AddVoter(id string, addr string) error {
	return m.Called(id, addr).Error(0)
}

func (m *ComprehensiveMockRaftManager) RemoveServer(id string) error {
	return m.Called(id).Error(0)
}

func (m *ComprehensiveMockRaftManager) GetConfiguration() raft.ConfigurationFuture {
	args := m.Called()
	return args.Get(0).(raft.ConfigurationFuture)
}

func (m *ComprehensiveMockRaftManager) GetISRManager() replication.ISRManagerInterface {
	args := m.Called()
	res := args.Get(0)
	if res == nil {
		return nil
	}
	return res.(replication.ISRManagerInterface)
}

func (m *ComprehensiveMockRaftManager) LeaderCh() <-chan bool {
	return nil
}

func (m *ComprehensiveMockRaftManager) ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int, isIdempotent bool, sequenceScope string) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}

func (m *ComprehensiveMockRaftManager) ReplicateBatchWithQuorum(topic string, partition int, messages []types.Message, minISR int, acks string, isIdempotent bool, sequenceScope string) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}

func (m *ComprehensiveMockRaftManager) ApplyResponse(prefix string, data []byte, timeout time.Duration) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}

func TestServiceDiscovery_RegisterDeregister(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	sd := NewServiceDiscovery(rm, "node1", "localhost:9001", "")

	t.Run("Register Success", func(t *testing.T) {
		rm.On("ApplyCommand", "REGISTER", mock.Anything).Return(nil).Once()
		err := sd.Register()
		assert.NoError(t, err)

		brokers := rm.mockFSM.GetBrokers()
		assert.Len(t, brokers, 1)
		assert.Equal(t, "node1", brokers[0].ID)
		assert.Equal(t, fsm.BrokerProtocolVersionCurrent, brokers[0].LifecycleProtocol)
		rm.AssertExpectations(t)
	})

	t.Run("Deregister Success", func(t *testing.T) {
		rm.On("ApplyCommand", "DEREGISTER", mock.Anything).Return(nil).Once()
		err := sd.Deregister()
		assert.NoError(t, err)

		brokers := rm.mockFSM.GetBrokers()
		assert.Len(t, brokers, 1)
		assert.Equal(t, "inactive", brokers[0].Status)
		rm.AssertExpectations(t)
	})
}

func TestServiceDiscovery_NodeOperations(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	sd := NewServiceDiscovery(rm, "node1", "localhost:9001", "")

	t.Run("AddNode - Not Leader", func(t *testing.T) {
		rm.isLeader = false
		rm.On("GetLeaderAddress").Return("leader:9001").Once()
		leader, err := sd.AddNode("node2", "localhost:9002")
		assert.Error(t, err)
		assert.Equal(t, "leader:9001", leader)
		rm.AssertExpectations(t)
	})

	t.Run("AddNode - Success", func(t *testing.T) {
		rm.isLeader = true
		rm.On("GetLeaderAddress").Return("localhost:9001").Once()
		rm.On("GetConfiguration").Return(staticConfigurationFuture{configuration: raft.Configuration{}}).Once()
		rm.On("AddVoter", "node2", "localhost:9002").Return(nil).Once()
		rm.On("ApplyCommand", "REGISTER", mock.Anything).Return(nil).Once()

		leader, err := sd.AddNode("node2", "localhost:9002")
		assert.NoError(t, err)
		assert.Equal(t, "localhost:9001", leader)

		brokers := rm.mockFSM.GetBrokers()
		assert.Len(t, brokers, 1)
		assert.Equal(t, "node2", brokers[0].ID)
		rm.AssertExpectations(t)
	})

	t.Run("RemoveNode - Success", func(t *testing.T) {
		rm.isLeader = true
		rm.On("GetLeaderAddress").Return("localhost:9001").Once()
		rm.On("RemoveServer", "node2").Return(nil).Once()
		rm.On("ApplyCommand", "DEREGISTER", mock.Anything).Return(nil).Once()

		leader, err := sd.RemoveNode("node2")
		assert.NoError(t, err)
		assert.Equal(t, "localhost:9001", leader)

		brokers := rm.mockFSM.GetBrokers()
		assert.Len(t, brokers, 1)
		assert.Equal(t, "inactive", brokers[0].Status)
		rm.AssertExpectations(t)
	})
}

func TestServiceDiscoveryExistingVoterJoinIsMembershipNoOp(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.isLeader = true
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	rm.On("GetLeaderAddress").Return("leader:9001").Once()
	rm.On("GetConfiguration").Return(staticConfigurationFuture{configuration: raft.Configuration{Servers: []raft.Server{
		{ID: "node2", Address: "localhost:9002", Suffrage: raft.Voter},
	}}}).Once()

	sd := NewServiceDiscoveryImpl(rm, "node1", "localhost:9001", "")
	leader, err := sd.AddNodeWithTransactionCoordinatorShards("node2", "localhost:9002", 0)

	assert.NoError(t, err)
	assert.Equal(t, "leader:9001", leader)
	rm.AssertNotCalled(t, "AddVoter", mock.Anything, mock.Anything)
	rm.AssertNotCalled(t, "ApplyCommand", "REGISTER", mock.Anything)
	rm.AssertExpectations(t)
}

func TestServiceDiscoveryExistingVoterJoinRejectsAddressChange(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.isLeader = true
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	rm.On("GetLeaderAddress").Return("leader:9001").Once()
	rm.On("GetConfiguration").Return(staticConfigurationFuture{configuration: raft.Configuration{Servers: []raft.Server{
		{ID: "node2", Address: "localhost:9002", Suffrage: raft.Voter},
	}}}).Once()

	sd := NewServiceDiscoveryImpl(rm, "node1", "localhost:9001", "")
	_, err := sd.AddNodeWithTransactionCoordinatorShards("node2", "localhost:9999", 0)

	assert.ErrorContains(t, err, "already belongs to raft")
	rm.AssertNotCalled(t, "AddVoter", mock.Anything, mock.Anything)
	rm.AssertNotCalled(t, "ApplyCommand", "REGISTER", mock.Anything)
	rm.AssertExpectations(t)
}

func TestServiceDiscoveryRejectsMismatchedTransactionCoordinatorShardCountBeforeAddingVoter(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.mockFSM = fsm.NewBrokerFSMWithTransactionCoordinatorShards(nil, nil, 7)
	rm.isLeader = true
	rm.On("GetLeaderAddress").Return("localhost:9001").Once()
	sd := NewServiceDiscoveryImpl(rm, "node1", "localhost:9001", "")

	leader, err := sd.AddNodeWithTransactionCoordinatorShards("node2", "localhost:9002", 8)
	assert.Equal(t, "localhost:9001", leader)
	assert.ErrorContains(t, err, "configured=8 cluster=7")
	rm.AssertNotCalled(t, "AddVoter", mock.Anything, mock.Anything)
	rm.AssertExpectations(t)
}

func TestServiceDiscovery_Heartbeat(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	isr := new(MockISRManager)
	sd := NewServiceDiscovery(rm, "node1", "localhost:9001", "")

	rm.On("GetISRManager").Return(isr)
	isr.On("UpdateHeartbeat", "node1").Return()

	sd.UpdateHeartbeat("node1")

	rm.AssertExpectations(t)
	isr.AssertExpectations(t)
}

func TestServiceDiscovery_DiscoverBrokers(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	sd := NewServiceDiscovery(rm, "node1", "localhost:9001", "")

	// Register two brokers directly in FSM
	rm.mockFSM.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node1","addr":"localhost:9001","status":"active"}`)})
	rm.mockFSM.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node2","addr":"localhost:9002","status":"active"}`)})

	brokers, err := sd.DiscoverBrokers()
	assert.NoError(t, err)
	assert.Len(t, brokers, 2)
}

func TestServiceDiscovery_RemoveNode_NotLeader(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	rm.isLeader = false
	sd := NewServiceDiscovery(rm, "node1", "localhost:9001", "")

	rm.On("GetLeaderAddress").Return("leader:9001").Once()

	leader, err := sd.RemoveNode("node2")
	assert.Error(t, err)
	assert.Equal(t, "leader:9001", leader)
}

func TestServiceDiscovery_Heartbeat_NilISRManager(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	sd := NewServiceDiscovery(rm, "node1", "localhost:9001", "")

	rm.On("GetISRManager").Return(nil)

	// Should not panic
	sd.UpdateHeartbeat("node1")
	rm.AssertExpectations(t)
}

func TestServiceDiscoveryReconcileMarksHeartbeatExpiredBrokerInactive(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.isLeader = true
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	rm.mockFSM.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node1","addr":"localhost:9001","status":"active"}`)})
	rm.mockFSM.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node2","addr":"localhost:9002","status":"active"}`)})
	configuration := raft.Configuration{Servers: []raft.Server{
		{ID: "node1", Address: "localhost:9001"},
		{ID: "node2", Address: "localhost:9002"},
	}}
	rm.On("GetConfiguration").Return(staticConfigurationFuture{configuration: configuration}).Once()
	rm.On("ApplyCommand", "DEREGISTER", mock.Anything).Return(nil).Once()

	sd := NewServiceDiscoveryImpl(rm, "node1", "localhost:9001", "")
	sd.livenessMu.Lock()
	sd.leaderSince = time.Now().Add(-2 * sd.heartbeatTimeout)
	sd.livenessMu.Unlock()
	sd.Reconcile()

	assert.Equal(t, "inactive", rm.mockFSM.GetBroker("node2").Status)
	rm.AssertExpectations(t)
}

func TestServiceDiscoveryLeaderGraceDefersMembershipTransition(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.isLeader = true
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	rm.mockFSM.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node1","addr":"localhost:9001","status":"active"}`)})
	rm.mockFSM.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node2","addr":"localhost:9002","status":"active"}`)})
	configuration := raft.Configuration{Servers: []raft.Server{{ID: "node1", Address: "localhost:9001"}, {ID: "node2", Address: "localhost:9002"}}}
	rm.On("GetConfiguration").Return(staticConfigurationFuture{configuration: configuration}).Once()

	sd := NewServiceDiscoveryImpl(rm, "node1", "localhost:9001", "")
	sd.Reconcile()

	assert.Equal(t, "active", rm.mockFSM.GetBroker("node2").Status)
	rm.AssertNotCalled(t, "ApplyCommand", "DEREGISTER", mock.Anything)
	rm.AssertExpectations(t)
}

func TestServiceDiscoveryRejectsFencedHeartbeat(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.isLeader = true
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	rm.mockFSM.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node2","addr":"localhost:9002","status":"inactive","incarnation_id":"current","incarnation_epoch":2}`)})
	rm.On("GetISRManager").Return(nil).Once()
	rm.On("ApplyCommand", "REGISTER", mock.Anything).Return(nil).Once()

	sd := NewServiceDiscoveryImpl(rm, "node1", "localhost:9001", "")
	sd.UpdateHeartbeatWithIncarnation("node2", "stale")
	assert.Equal(t, "inactive", rm.mockFSM.GetBroker("node2").Status)
	sd.UpdateHeartbeatWithIncarnation("node2", "current")
	assert.Equal(t, "active", rm.mockFSM.GetBroker("node2").Status)
	rm.AssertExpectations(t)
}

func TestServiceDiscoveryHeartbeatReactivatesBroker(t *testing.T) {
	rm := new(ComprehensiveMockRaftManager)
	rm.isLeader = true
	rm.mockFSM = fsm.NewBrokerFSM(nil, nil)
	rm.mockFSM.Apply(&raft.Log{Data: []byte(`REGISTER:{"id":"node2","addr":"localhost:9002","status":"active"}`)})
	rm.mockFSM.Apply(&raft.Log{Data: []byte(`DEREGISTER:{"id":"node2"}`)})
	isr := new(MockISRManager)
	rm.On("GetISRManager").Return(isr)
	isr.On("UpdateHeartbeat", "node2").Return().Once()
	rm.On("ApplyCommand", "REGISTER", mock.Anything).Return(nil).Once()

	sd := NewServiceDiscoveryImpl(rm, "node1", "localhost:9001", "")
	sd.UpdateHeartbeat("node2")

	assert.Equal(t, "active", rm.mockFSM.GetBroker("node2").Status)
	rm.AssertExpectations(t)
	isr.AssertExpectations(t)
}
