package replication

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
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

func (m *MockRaft) Stats() map[string]string {
	return m.Called().Get(0).(map[string]string)
}

type MockApplyFuture struct {
	mock.Mock
}

func (m *MockApplyFuture) Error() error          { return m.Called().Error(0) }
func (m *MockApplyFuture) Response() interface{} { return m.Called().Get(0) }
func (m *MockApplyFuture) Index() uint64         { return 0 }

type MockIndexFuture struct {
	mock.Mock
}

func (m *MockIndexFuture) Error() error  { return m.Called().Error(0) }
func (m *MockIndexFuture) Index() uint64 { return 0 }

func TestBuildRaftConfigMapsSnapshotSettings(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.RaftSnapshotIntervalMS = 250
	cfg.RaftSnapshotThreshold = 16
	cfg.RaftTrailingLogs = 0

	got, err := buildRaftConfig(cfg, "broker-1")
	assert.NoError(t, err)
	assert.Equal(t, 250*time.Millisecond, got.SnapshotInterval)
	assert.Equal(t, uint64(16), got.SnapshotThreshold)
	assert.Equal(t, uint64(0), got.TrailingLogs)
}

func TestBuildRaftConfigDefaultsMatchHashicorp(t *testing.T) {
	got, err := buildRaftConfig(config.DefaultConfig(), "broker-1")
	assert.NoError(t, err)
	want := raft.DefaultConfig()
	assert.Equal(t, want.SnapshotInterval, got.SnapshotInterval)
	assert.Equal(t, want.SnapshotThreshold, got.SnapshotThreshold)
	assert.Equal(t, want.TrailingLogs, got.TrailingLogs)
}

func TestBuildRaftConfigRejectsUnsafeSnapshotSettings(t *testing.T) {
	tests := []struct {
		name string
		edit func(*config.Config)
		want string
	}{
		{
			name: "interval below library minimum",
			edit: func(cfg *config.Config) { cfg.RaftSnapshotIntervalMS = 4 },
			want: "raft_snapshot_interval_ms",
		},
		{
			name: "zero threshold",
			edit: func(cfg *config.Config) { cfg.RaftSnapshotThreshold = 0 },
			want: "raft_snapshot_threshold",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			test.edit(cfg)
			_, err := buildRaftConfig(cfg, "broker-1")
			assert.ErrorContains(t, err, test.want)
		})
	}
}

func TestRaftReplicationManagerGetRaftStatus(t *testing.T) {
	mr := new(MockRaft)
	mr.On("Stats").Return(map[string]string{
		"state":               "Follower",
		"applied_index":       "41",
		"commit_index":        "42",
		"last_log_index":      "43",
		"last_snapshot_index": "40",
		"last_snapshot_term":  "3",
	})
	rm := &RaftReplicationManager{raft: mr}

	got, err := rm.GetRaftStatus()
	assert.NoError(t, err)
	assert.Equal(t, RaftStatus{
		State: "Follower", AppliedIndex: 41, CommitIndex: 42, LastLogIndex: 43,
		LastSnapshotIndex: 40, LastSnapshotTerm: 3,
	}, got)
}

func TestRaftReplicationManagerGetRaftStatusRejectsMissingOrBlankState(t *testing.T) {
	for _, test := range []struct {
		name  string
		state map[string]string
	}{
		{name: "missing", state: map[string]string{}},
		{name: "blank", state: map[string]string{"state": "  "}},
	} {
		t.Run(test.name, func(t *testing.T) {
			mr := new(MockRaft)
			mr.On("Stats").Return(test.state)
			rm := &RaftReplicationManager{raft: mr}
			_, err := rm.GetRaftStatus()
			assert.ErrorContains(t, err, "state")
		})
	}
}

func TestRaftReplicationManagerGetRaftStatusRejectsInvalidStats(t *testing.T) {
	mr := new(MockRaft)
	mr.On("Stats").Return(map[string]string{"state": "Follower", "applied_index": "not-a-number"})
	rm := &RaftReplicationManager{raft: mr}
	_, err := rm.GetRaftStatus()
	assert.ErrorContains(t, err, "applied_index")
}

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

		// Ensure the peer is removed from the map.
		// Simply using assert.Empty on value doesn't guarantee the key was deleted if value was "".
		_, exists := rm.peers["n1"]
		assert.False(t, exists, "peer n1 should be removed from the map")
	})
}

type MockISRManager struct {
	mock.Mock
}

func (m *MockISRManager) HasQuorum(topic string, partition int, minISR int) bool {
	return m.Called(topic, partition, minISR).Bool(0)
}
func (m *MockISRManager) UpdateHeartbeat(id string)           { m.Called(id) }
func (m *MockISRManager) GetISR(t string, p int) []string     { return nil }
func (m *MockISRManager) ComputeISR(t string, p int) []string { return nil }
func (m *MockISRManager) SetLeader(l bool)                    { m.Called(l) }
func (m *MockISRManager) Start()                              { m.Called() }

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
