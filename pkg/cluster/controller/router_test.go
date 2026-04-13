package controller

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
)

type MockRaftManager struct {
	isLeader bool
	leaderCh chan bool
	mockFSM  *fsm.BrokerFSM
}

func (m *MockRaftManager) IsLeader() bool { return m.isLeader }
func (m *MockRaftManager) GetLeaderAddress() string {
	return "localhost:9001"
}
func (m *MockRaftManager) ApplyCommand(prefix string, data []byte) error { return nil }
func (m *MockRaftManager) LeaderCh() <-chan bool {
	if m.leaderCh == nil {
		m.leaderCh = make(chan bool, 1)
	}
	return m.leaderCh
}
func (m *MockRaftManager) GetFSM() *fsm.BrokerFSM {
	return m.mockFSM
}
func (m *MockRaftManager) GetConfiguration() raft.ConfigurationFuture    { return nil }
func (m *MockRaftManager) ReplicateWithQuorum(topic string, partition int, msg types.Message, minISR int, isIdempotent bool, sequenceScope string) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}
func (m *MockRaftManager) ReplicateBatchWithQuorum(topic string, partition int, messages []types.Message, minISR int, acks string, isIdempotent bool, sequenceScope string) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}
func (m *MockRaftManager) ApplyResponse(prefix string, data []byte, timeout time.Duration) (types.AckResponse, error) {
	return types.AckResponse{}, nil
}

type MockLocalProcessor struct {
	processed bool
}

func (m *MockLocalProcessor) ProcessCommand(cmd string) string {
	m.processed = true
	return "OK"
}

func TestClusterRouter_LocalProcess(t *testing.T) {
	processor := &MockLocalProcessor{}

	rm := &MockRaftManager{isLeader: true}
	router := NewClusterRouter("node1", "localhost:7000", processor, rm, 9000)

	if router.processLocally("CREATE topic=t1") != "OK" {
		t.Fatal("Expected local processing to succeed")
	}
	if !processor.processed {
		t.Fatal("Expected processor.processed to be true")
	}
}
