package controller

import (
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication"
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
func (m *MockRaftManager) GetConfiguration() raft.ConfigurationFuture     { return nil }
func (m *MockRaftManager) AddVoter(id string, addr string) error          { return nil }
func (m *MockRaftManager) RemoveServer(id string) error                   { return nil }
func (m *MockRaftManager) GetISRManager() replication.ISRManagerInterface { return nil }
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

func TestClusterRouter_FindCoordinator(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil, nil)
	// Add some brokers
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"node1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"node2\",\"addr\":\"localhost:7002\",\"status\":\"active\"}")})
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"node3\",\"addr\":\"localhost:7003\",\"status\":\"active\"}")})

	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("node1", "localhost:7001", nil, rm, 7000)

	group1 := "group-a"
	id1, addr1, err := router.FindCoordinator(group1)
	if err != nil {
		t.Fatalf("FindCoordinator failed: %v", err)
	}

	group2 := "group-b"
	id2, addr2, err := router.FindCoordinator(group2)
	if err != nil {
		t.Fatalf("FindCoordinator failed: %v", err)
	}

	// Verify stability: same group always maps to same coordinator
	id1_retry, _, _ := router.FindCoordinator(group1)
	if id1 != id1_retry {
		t.Fatalf("Consistency failed: %s != %s", id1, id1_retry)
	}

	t.Logf("Group %s -> %s (%s)", group1, id1, addr1)
	t.Logf("Group %s -> %s (%s)", group2, id2, addr2)

	// Verify stability when a node is added
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"node4\",\"addr\":\"localhost:7004\",\"status\":\"active\"}")})
	id1_after, _, _ := router.FindCoordinator(group1)

	// With 3->4 nodes, group-a might or might not move, but it shouldn't depend on other groups
	t.Logf("Group %s after adding node4 -> %s", group1, id1_after)
}

func TestClusterRouter_FindCoordinator_CacheRebuild(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil, nil)
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n2\",\"addr\":\"localhost:7002\",\"status\":\"active\"}")})

	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000)

	// First call builds ring
	id1, _, err := router.FindCoordinator("group-x")
	if err != nil {
		t.Fatalf("FindCoordinator failed: %v", err)
	}

	// Same call should use cache (same result)
	id2, _, _ := router.FindCoordinator("group-x")
	if id1 != id2 {
		t.Fatalf("Cached ring returned different result: %s vs %s", id1, id2)
	}

	// Add broker -> should rebuild ring
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n3\",\"addr\":\"localhost:7003\",\"status\":\"active\"}")})
	id3, _, err := router.FindCoordinator("group-x")
	if err != nil {
		t.Fatalf("FindCoordinator after adding node failed: %v", err)
	}
	t.Logf("group-x: before=%s, after=%s", id1, id3)
}

func TestClusterRouter_FindCoordinator_NoActiveBrokers(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil, nil)
	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000)

	_, _, err := router.FindCoordinator("group-x")
	if err == nil {
		t.Fatal("Expected error when no active brokers")
	}
}

func TestClusterRouter_FindCoordinator_NilFSM(t *testing.T) {
	rm := &MockRaftManager{isLeader: true, mockFSM: nil}
	router := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000)

	_, _, err := router.FindCoordinator("group-x")
	if err == nil {
		t.Fatal("Expected error when FSM is nil")
	}
}

func TestClusterRouter_ForwardToCoordinator_Local(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil, nil)
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})

	processor := &MockLocalProcessor{}
	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("n1", "localhost:7001", processor, rm, 7000)

	// With only one broker, all groups map to n1 -> local processing
	resp, err := router.ForwardToCoordinator("any-group", "HEARTBEAT group=any-group member=m1")
	if err != nil {
		t.Fatalf("ForwardToCoordinator failed: %v", err)
	}
	if resp != "OK" {
		t.Fatalf("Expected OK, got %s", resp)
	}
	if !processor.processed {
		t.Fatal("Expected local processing")
	}
}

func TestClusterRouter_ForwardToPartitionLeader_Local(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil, nil)
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})

	// Create a topic with 1 partition, leader = n1
	topicData := `{"name":"t1","partitions":1,"leader_id":"n1","replication_factor":1}`
	mockFSM.Apply(&raft.Log{Data: []byte("TOPIC:" + topicData)})

	processor := &MockLocalProcessor{}
	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("n1", "localhost:7001", processor, rm, 7000)

	resp, err := router.ForwardToPartitionLeader("t1", 0, "PUBLISH topic=t1 message=hello")
	if err != nil {
		t.Fatalf("ForwardToPartitionLeader failed: %v", err)
	}
	if resp != "OK" {
		t.Fatalf("Expected OK, got %s", resp)
	}
}

func TestClusterRouter_ForwardToLeader_IsLeader(t *testing.T) {
	processor := &MockLocalProcessor{}
	rm := &MockRaftManager{isLeader: true}
	router := NewClusterRouter("n1", "localhost:7001", processor, rm, 7000)

	resp, err := router.ForwardToLeader("LIST")
	if err != nil {
		t.Fatalf("ForwardToLeader failed: %v", err)
	}
	if resp != "OK" {
		t.Fatalf("Expected OK, got %s", resp)
	}
}
