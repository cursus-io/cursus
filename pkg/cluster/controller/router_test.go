package controller

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
)

func installConsumerOffsetsTopology(t *testing.T, state *fsm.BrokerFSM, index uint64) uint64 {
	t.Helper()
	for _, broker := range state.GetBrokers() {
		broker.LifecycleProtocol = fsm.BrokerProtocolVersionCurrent
		payload, err := json.Marshal(broker)
		if err != nil {
			t.Fatal(err)
		}
		if result := state.Apply(&raft.Log{Index: index, Data: append([]byte("REGISTER:"), payload...)}); result != nil {
			t.Fatalf("upgrade broker protocol: %v", result)
		}
		index++
	}
	definition := topic.DefaultDefinition(config.ConsumerOffsetsTopicName, config.DefaultConfig())
	definition.Partitions = 4
	definition.ReplicationFactor = 3
	definition.Policy = topic.ConsumerMetadataPolicy()
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	result := state.Apply(&raft.Log{Index: index, Data: append([]byte("TOPIC:"), payload...)})
	if result != nil {
		t.Fatalf("install consumer offsets topology: %v", result)
	}
	return index + 1
}

func setConsumerOffsetsLeaders(t *testing.T, state *fsm.BrokerFSM, index uint64, leader string) uint64 {
	t.Helper()
	for partition := 0; partition < 4; partition++ {
		key := config.ConsumerOffsetsTopicName + "-" + strconv.Itoa(partition)
		metadata := state.GetPartitionMetadata(key)
		if metadata == nil {
			t.Fatalf("missing offsets partition metadata %s", key)
		}
		metadata.Leader = leader
		metadata.LeaderEpoch++
		data, err := json.Marshal(metadata)
		if err != nil {
			t.Fatal(err)
		}
		if result := state.Apply(&raft.Log{Index: index, Data: []byte("PARTITION:" + key + ":" + string(data))}); result != nil {
			t.Fatalf("set offsets partition leader: %v", result)
		}
		index++
	}
	return index
}

func TestWithInternalTokenPreservesLongRawCommand(t *testing.T) {
	router := &ClusterRouter{internalToken: "secret"}
	command := "REPLICATE_MESSAGE payload=" + strings.Repeat("x", 22000)

	got := router.withInternalToken(command)
	wantPrefix := "REPLICATE_MESSAGE internal_token=secret payload="
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("command prefix = %q, want %q", got[:min(len(got), len(wantPrefix))], wantPrefix)
	}
}

func TestInjectInternalTokenOnlyInspectsCommandArguments(t *testing.T) {
	command := "REPLICATE_MESSAGE payload=message internal_token=payload-value"
	got := injectInternalToken(command, "secret")
	want := "REPLICATE_MESSAGE internal_token=secret payload=message internal_token=payload-value"
	if got != want {
		t.Fatalf("command = %q, want %q", got, want)
	}
}

func TestInjectInternalTokenPreservesExistingFirstArgument(t *testing.T) {
	command := "REPLICATE_MESSAGE internal_token=secret payload=value"
	if got := injectInternalToken(command, "secret"); got != command {
		t.Fatalf("command = %q, want unchanged", got)
	}
}

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
	router := NewClusterRouter("node1", "localhost:7000", processor, rm, 9000, "", nil)

	if router.processLocally("CREATE topic=t1") != "OK" {
		t.Fatal("Expected local processing to succeed")
	}
	if !processor.processed {
		t.Fatal("Expected processor.processed to be true")
	}
}

func TestClusterRouter_FindCoordinator(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil)
	// Add some brokers
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"node1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"node2\",\"addr\":\"localhost:7002\",\"status\":\"active\"}")})
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"node3\",\"addr\":\"localhost:7003\",\"status\":\"active\"}")})
	installConsumerOffsetsTopology(t, mockFSM, 4)

	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("node1", "localhost:7001", nil, rm, 7000, "", nil)

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

	// Adding an unrelated broker cannot move a group coordinator. Only the
	// durable offsets-partition leader may do that.
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"node4\",\"addr\":\"localhost:7004\",\"status\":\"active\"}")})
	id1_after, _, _ := router.FindCoordinator(group1)
	if id1_after != id1 {
		t.Fatalf("adding broker changed coordinator without offsets leader transition: %s -> %s", id1, id1_after)
	}
}

func TestClusterRouterFindTransactionCoordinatorUsesDurableShardOwner(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil)
	mockFSM.Apply(&raft.Log{Index: 1, Data: []byte("REGISTER:{\"id\":\"node1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})
	mockFSM.Apply(&raft.Log{Index: 2, Data: []byte("REGISTER:{\"id\":\"node2\",\"addr\":\"localhost:7002\",\"status\":\"active\"}")})

	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("node1", "localhost:7001", nil, rm, 7000, "", nil)

	id, addr, epoch, err := router.FindTransactionCoordinator("payments-processor")
	if err != nil {
		t.Fatalf("FindTransactionCoordinator failed: %v", err)
	}
	if id != "node1" && id != "node2" {
		t.Fatalf("unexpected coordinator %q", id)
	}
	if addr == "" || epoch <= 0 {
		t.Fatalf("invalid durable coordinator addr=%q epoch=%d", addr, epoch)
	}

	ownership, ok := mockFSM.GetTransactionCoordinator("payments-processor")
	if !ok || ownership.Owner != id || ownership.Epoch != epoch {
		t.Fatalf("router result does not match replicated ownership: %+v", ownership)
	}
}

func TestClusterRouter_FindCoordinator_CacheRebuild(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil)
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n2\",\"addr\":\"localhost:7002\",\"status\":\"active\"}")})
	installConsumerOffsetsTopology(t, mockFSM, 3)
	installConsumerOffsetsTopology(t, mockFSM, 3)

	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000, "", nil)

	// Same group is stable while durable partition metadata is unchanged.
	id1, _, err := router.FindCoordinator("group-x")
	if err != nil {
		t.Fatalf("FindCoordinator failed: %v", err)
	}

	// Same call should use cache (same result)
	id2, _, _ := router.FindCoordinator("group-x")
	if id1 != id2 {
		t.Fatalf("Cached ring returned different result: %s vs %s", id1, id2)
	}

	// An unrelated membership registration must not move the coordinator.
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n3\",\"addr\":\"localhost:7003\",\"status\":\"active\"}")})
	id3, _, err := router.FindCoordinator("group-x")
	if err != nil {
		t.Fatalf("FindCoordinator after adding node failed: %v", err)
	}
	if id1 != id3 {
		t.Fatalf("membership changed coordinator without offsets leader transition: %s -> %s", id1, id3)
	}
}

func TestClusterRouterFindCoordinatorFollowsDurableOffsetsLeader(t *testing.T) {
	state := fsm.NewBrokerFSM(nil, nil)
	for index, id := range []string{"n1", "n2", "n3"} {
		state.Apply(&raft.Log{Index: uint64(index + 1), Data: []byte(`REGISTER:{"id":"` + id + `","addr":"localhost:700` + string(rune('1'+index)) + `","status":"active"}`)})
	}
	next := installConsumerOffsetsTopology(t, state, 4)
	next = setConsumerOffsetsLeaders(t, state, next, "n2")
	rm := &MockRaftManager{isLeader: true, mockFSM: state}
	router := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000, "", nil)

	id, _, partition, epoch, err := router.FindCoordinatorWithEpoch("failover-group")
	if err != nil {
		t.Fatal(err)
	}
	if id != "n2" {
		t.Fatalf("coordinator=%s, want durable offsets leader n2", id)
	}
	if partition < 0 || partition >= 4 || epoch == 0 {
		t.Fatalf("invalid durable coordinator fence partition=%d epoch=%d", partition, epoch)
	}
	setConsumerOffsetsLeaders(t, state, next, "n1")
	id, _, nextPartition, nextEpoch, err := router.FindCoordinatorWithEpoch("failover-group")
	if err != nil {
		t.Fatal(err)
	}
	if id != "n1" {
		t.Fatalf("coordinator=%s, want post-apply offsets leader n1", id)
	}
	if nextPartition != partition || nextEpoch <= epoch {
		t.Fatalf("durable coordinator fence did not advance: partition %d->%d epoch %d->%d", partition, nextPartition, epoch, nextEpoch)
	}
}

func TestClusterRouter_FindCoordinatorOwnersUsesOneMembershipSnapshot(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil)
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n2\",\"addr\":\"localhost:7002\",\"status\":\"active\"}")})
	installConsumerOffsetsTopology(t, mockFSM, 3)

	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000, "", nil)
	groups := []string{"alpha", "beta", "gamma"}
	owners, err := router.FindCoordinatorOwners(groups)
	if err != nil {
		t.Fatalf("FindCoordinatorOwners failed: %v", err)
	}
	for _, groupName := range groups {
		owner, ok := owners[groupName]
		if !ok || (owner != "n1" && owner != "n2") {
			t.Fatalf("invalid owner for %s: %q", groupName, owner)
		}
		singleOwner, _, singleErr := router.FindCoordinator(groupName)
		if singleErr != nil {
			t.Fatalf("FindCoordinator(%s) failed: %v", groupName, singleErr)
		}
		if owner != singleOwner {
			t.Fatalf("batch owner %q differs from single owner %q for %s", owner, singleOwner, groupName)
		}
	}
}

func TestClusterRouter_FindCoordinator_NoActiveBrokers(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil)
	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000, "", nil)

	_, _, err := router.FindCoordinator("group-x")
	if err == nil {
		t.Fatal("Expected error when no active brokers")
	}
}

func TestClusterRouter_FindCoordinator_NilFSM(t *testing.T) {
	rm := &MockRaftManager{isLeader: true, mockFSM: nil}
	router := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000, "", nil)

	_, _, err := router.FindCoordinator("group-x")
	if err == nil {
		t.Fatal("Expected error when FSM is nil")
	}
}

func TestClusterRouter_ForwardToCoordinator_Local(t *testing.T) {
	mockFSM := fsm.NewBrokerFSM(nil, nil)
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})
	installConsumerOffsetsTopology(t, mockFSM, 2)

	processor := &MockLocalProcessor{}
	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("n1", "localhost:7001", processor, rm, 7000, "", nil)

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
	mockFSM := fsm.NewBrokerFSM(nil, nil)
	mockFSM.Apply(&raft.Log{Data: []byte("REGISTER:{\"id\":\"n1\",\"addr\":\"localhost:7001\",\"status\":\"active\"}")})

	// Create a topic with 1 partition, leader = n1.
	definition := topic.DefaultDefinition("t1", config.DefaultConfig())
	definition.Partitions = 1
	definition.ReplicationFactor = 1
	payload, err := json.Marshal(fsm.TopicCommand{Definition: &definition, LeaderID: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	mockFSM.Apply(&raft.Log{Data: append([]byte("TOPIC:"), payload...)})

	processor := &MockLocalProcessor{}
	rm := &MockRaftManager{isLeader: true, mockFSM: mockFSM}
	router := NewClusterRouter("n1", "localhost:7001", processor, rm, 7000, "", nil)

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
	router := NewClusterRouter("n1", "localhost:7001", processor, rm, 7000, "", nil)

	resp, err := router.ForwardToLeader("LIST")
	if err != nil {
		t.Fatalf("ForwardToLeader failed: %v", err)
	}
	if resp != "OK" {
		t.Fatalf("Expected OK, got %s", resp)
	}
}

func TestClusterRouterBrokerCommandAddrPrefersInternalPort(t *testing.T) {
	rm := &MockRaftManager{isLeader: true}
	router := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000, "", &config.Config{InternalBrokerPort: 19000})
	if got := router.brokerCommandAddr("broker-2"); got != "broker-2:19000" {
		t.Fatalf("expected internal broker port, got %s", got)
	}

	fallback := NewClusterRouter("n1", "localhost:7001", nil, rm, 7000, "", nil)
	if got := fallback.brokerCommandAddr("broker-2"); got != "broker-2:7000" {
		t.Fatalf("expected client port fallback, got %s", got)
	}

	if got := router.brokerCommandAddr("2001:db8::1"); got != "[2001:db8::1]:19000" {
		t.Fatalf("expected bracketed IPv6 internal address, got %s", got)
	}

	if got := fallback.brokerCommandAddr("2001:db8::2"); got != "[2001:db8::2]:7000" {
		t.Fatalf("expected bracketed IPv6 client address, got %s", got)
	}
}
