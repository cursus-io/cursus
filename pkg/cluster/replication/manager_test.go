package replication

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockRaft struct {
	mock.Mock
}

func (m *MockRaft) Apply(data []byte, timeout time.Duration) raft.ApplyFuture {
	args := m.Called(data, timeout)
	return args.Get(0).(raft.ApplyFuture)
}

func (m *MockRaft) AddVoter(id raft.ServerID, addr raft.ServerAddress, index uint64, timeout time.Duration) raft.IndexFuture {
	args := m.Called(id, addr, index, timeout)
	return args.Get(0).(raft.IndexFuture)
}

func (m *MockRaft) RemoveServer(id raft.ServerID, index uint64, timeout time.Duration) raft.IndexFuture {
	args := m.Called(id, index, timeout)
	return args.Get(0).(raft.IndexFuture)
}

func (m *MockRaft) Leader() raft.ServerAddress {
	return raft.ServerAddress(m.Called().String(0))
}

func (m *MockRaft) State() raft.RaftState {
	return m.Called().Get(0).(raft.RaftState)
}

func (m *MockRaft) GetConfiguration() raft.ConfigurationFuture {
	return m.Called().Get(0).(raft.ConfigurationFuture)
}

func (m *MockRaft) BootstrapCluster(cfg raft.Configuration) raft.Future {
	return m.Called(cfg).Get(0).(raft.Future)
}

func (m *MockRaft) Shutdown() raft.Future {
	return m.Called().Get(0).(raft.Future)
}

type MockApplyFuture struct {
	mock.Mock
}

func (m *MockApplyFuture) Error() error { return m.Called().Error(0) }
func (m *MockApplyFuture) Response() interface{} { return m.Called().Get(0) }
func (m *MockApplyFuture) Index() uint64 { return 0 }

type MockIndexFuture struct {
	mock.Mock
}

func (m *MockIndexFuture) Error() error { return m.Called().Error(0) }
func (m *MockIndexFuture) Index() uint64 { return 0 }

func TestRaftReplicationManager_ApplyCommand(t *testing.T) {
	mr := new(MockRaft)
	rm := &RaftReplicationManager{raft: mr}
	
	fut := new(MockApplyFuture)
	fut.On("Error").Return(nil)
	mr.On("Apply", mock.MatchedBy(func(b []byte) bool {
		return string(b) == "TEST:data"
	}), 5*time.Second).Return(fut)
	
	err := rm.ApplyCommand("TEST", []byte("data"))
	assert.NoError(t, err)
}

func TestRaftReplicationManager_AddRemoveVoter(t *testing.T) {
	mr := new(MockRaft)
	rm := &RaftReplicationManager{raft: mr, peers: make(map[string]string)}
	
	t.Run("AddVoter", func(t *testing.T) {
		fut := new(MockIndexFuture)
		fut.On("Error").Return(nil)
		mr.On("AddVoter", raft.ServerID("n1"), raft.ServerAddress("a1"), uint64(0), 10*time.Second).Return(fut)
		
		err := rm.AddVoter("n1", "a1")
		assert.NoError(t, err)
		assert.Equal(t, "a1", rm.peers["n1"])
	})

	t.Run("RemoveServer", func(t *testing.T) {
		fut := new(MockIndexFuture)
		fut.On("Error").Return(nil)
		mr.On("RemoveServer", raft.ServerID("n1"), uint64(0), 10*time.Second).Return(fut)
		
		err := rm.RemoveServer("n1")
		assert.NoError(t, err)
		assert.Empty(t, rm.peers["n1"])
	})
}

type MockISRManager struct {
	mock.Mock
}

func (m *MockISRManager) HasQuorum(topic string, partition int, minISR int) bool {
	return m.Called(topic, partition, minISR).Bool(0)
}
func (m *MockISRManager) UpdateHeartbeat(id string) { m.Called(id) }
func (m *MockISRManager) GetISR(t string, p int) []string { return nil }
func (m *MockISRManager) ComputeISR(t string, p int) []string { return nil }
func (m *MockISRManager) SetLeader(l bool) { m.Called(l) }
func (m *MockISRManager) Start() { m.Called() }

func TestRaftReplicationManager_ReplicateWithQuorum(t *testing.T) {
	mr := new(MockRaft)
	mi := new(MockISRManager)
	rm := &RaftReplicationManager{raft: mr, isrManager: mi}
	
	msg := types.Message{Payload: "hello"}
	
	t.Run("No Quorum", func(t *testing.T) {
		mi.On("HasQuorum", "t1", 0, 2).Return(false).Once()
		_, err := rm.ReplicateWithQuorum("t1", 0, msg, 2, false, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient")
	})

	t.Run("Success", func(t *testing.T) {
		mi.On("HasQuorum", "t1", 0, 1).Return(true).Once()
		fut := new(MockApplyFuture)
		fut.On("Error").Return(nil)
		fut.On("Response").Return(types.AckResponse{Status: "OK"})
		mr.On("Apply", mock.Anything, 5*time.Second).Return(fut)
		
		resp, err := rm.ReplicateWithQuorum("t1", 0, msg, 1, false, "")
		assert.NoError(t, err)
		assert.Equal(t, "OK", resp.Status)
	})
}
