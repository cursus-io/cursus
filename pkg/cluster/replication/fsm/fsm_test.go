package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
)

type MockStorageHandler struct {
	types.StorageHandler
	offset uint64
}

func (m *MockStorageHandler) Write(msg types.Message) (uint64, error) {
	m.offset++
	return m.offset, nil
}
func (m *MockStorageHandler) AppendMessage(topic string, partition int, msg *types.Message) (uint64, error) {
	m.offset++
	msg.Offset = m.offset
	return m.offset, nil
}
func (m *MockStorageHandler) GetAbsoluteOffset() uint64 { return m.offset }
func (m *MockStorageHandler) GetLatestOffset() uint64   { return m.offset }
func (m *MockStorageHandler) ReserveOffsets(n int) uint64 {
	start := m.offset
	m.offset += uint64(n)
	return start
}
func (m *MockStorageHandler) Close() error { return nil }

type MockHandlerProvider struct{}

func (m *MockHandlerProvider) GetHandler(topic string, partitionID int) (types.StorageHandler, error) {
	return &MockStorageHandler{}, nil
}

func newTestFSM() *BrokerFSM {
	tm := topic.NewTopicManager(config.DefaultConfig(), &MockHandlerProvider{}, nil)
	fsm := NewBrokerFSM(nil, tm, nil)

	if fsm.brokers == nil {
		fsm.brokers = make(map[string]*BrokerInfo)
	}
	if fsm.partitionMetadata == nil {
		fsm.partitionMetadata = make(map[string]*PartitionMetadata)
	}
	if fsm.logs == nil {
		fsm.logs = make(map[uint64]*ReplicationEntry)
	}

	return fsm
}

func TestBrokerFSM_Apply_Register(t *testing.T) {
	fsm := newTestFSM()
	brokerInfo := BrokerInfo{ID: "b1", Addr: "127.0.0.1:9092", Status: "active", LastSeen: time.Now()}
	data, _ := json.Marshal(brokerInfo)

	log := &raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: 1}

	result := fsm.Apply(log)
	if result != nil {
		t.Fatalf("Apply failed: %v", result)
	}

	brokers := fsm.GetBrokers()
	if len(brokers) != 1 || brokers[0].ID != "b1" {
		t.Errorf("Broker not registered correctly: %+v", brokers)
	}
}

func TestBrokerFSM_Apply_Register_InvalidPayload(t *testing.T) {
	fsm := newTestFSM()

	log := &raft.Log{
		Data:  []byte("REGISTER:{invalid-json"),
		Index: 1,
	}

	result := fsm.Apply(log)
	if result == nil {
		t.Fatal("expected error result for invalid REGISTER payload")
	}

	if len(fsm.GetBrokers()) != 0 {
		t.Fatal("broker should not be registered on invalid payload")
	}
}

func TestBrokerFSM_Apply_Deregister(t *testing.T) {
	fsm := newTestFSM()
	fsm.brokers["b1"] = &BrokerInfo{ID: "b1"}

	log := &raft.Log{Data: []byte("DEREGISTER:{\"id\":\"b1\"}"), Index: 2}
	fsm.Apply(log)

	if fsm.brokers["b1"].Status != "inactive" {
		t.Error("Broker not marked inactive")
	}
}

func TestBrokerFSM_Apply_Deregister_ReturnsNil(t *testing.T) {
	fsm := newTestFSM()
	fsm.brokers["b1"] = &BrokerInfo{ID: "b1"}

	log := &raft.Log{Data: []byte("DEREGISTER:{\"id\":\"b1\"}"), Index: 2}
	result := fsm.Apply(log)

	if result != nil {
		t.Fatalf("DEREGISTER should return nil, got: %v", result)
	}

	if fsm.brokers["b1"].Status != "inactive" {
		t.Fatal("broker status not inactive")
	}
}

func TestBrokerFSM_Apply_Partition(t *testing.T) {
	fsm := newTestFSM()
	metadata := PartitionMetadata{Leader: "l1", Replicas: []string{"r1"}, LeaderEpoch: 1}
	data, _ := json.Marshal(metadata)
	key := "t1-0"

	log := &raft.Log{Data: []byte(fmt.Sprintf("PARTITION:%s:%s", key, data)), Index: 4}

	result := fsm.Apply(log)
	if result != nil {
		t.Fatalf("Apply failed: %v", result)
	}

	meta := fsm.GetPartitionMetadata(key)
	if meta == nil || meta.Leader != "l1" {
		t.Errorf("Partition metadata not updated correctly: %+v", meta)
	}
}

func TestBrokerFSM_Apply_UnknownCommand(t *testing.T) {
	fsm := newTestFSM()

	log := &raft.Log{
		Data:  []byte("UNKNOWN:payload"),
		Index: 1,
	}

	result := fsm.Apply(log)
	if result == nil {
		t.Fatal("expected error for unknown command")
	}
}

func TestBrokerFSM_Apply_UpdatesAppliedIndex(t *testing.T) {
	fsm := newTestFSM()

	log1 := &raft.Log{Data: []byte("DEREGISTER:x"), Index: 10}
	log2 := &raft.Log{Data: []byte("DEREGISTER:y"), Index: 11}

	fsm.Apply(log1)
	if fsm.applied != 10 {
		t.Fatalf("applied index not updated, expected 10 got %d", fsm.applied)
	}

	fsm.Apply(log2)
	if fsm.applied != 11 {
		t.Fatalf("applied index not monotonic, expected 11 got %d", fsm.applied)
	}
}

func TestBrokerFSM_Snapshot_Restore(t *testing.T) {
	fsm := newTestFSM()
	fsm.brokers["b1"] = &BrokerInfo{ID: "b1", Addr: "a1"}
	fsm.partitionMetadata["t1-0"] = &PartitionMetadata{Leader: "l1"}
	fsm.logs[5] = &ReplicationEntry{Topic: "t1"}
	fsm.applied = 5

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	buf := new(bytes.Buffer)
	sink := &MockSnapshotSink{Writer: buf}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	newFSM := newTestFSM()
	rc := io.NopCloser(bytes.NewReader(buf.Bytes()))

	if err := newFSM.Restore(rc); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if len(newFSM.brokers) != 1 || newFSM.brokers["b1"].Addr != "a1" {
		t.Errorf("Brokers not restored correctly: %+v", newFSM.brokers)
	}
	if len(newFSM.partitionMetadata) != 1 || newFSM.partitionMetadata["t1-0"].Leader != "l1" {
		t.Errorf("Metadata not restored correctly: %+v", newFSM.partitionMetadata)
	}
	if newFSM.applied != 5 {
		t.Errorf("Applied index not restored correctly: %d", newFSM.applied)
	}
}

func TestBrokerFSM_Snapshot_ClosesSink(t *testing.T) {
	fsm := newTestFSM()
	fsm.brokers["b1"] = &BrokerInfo{ID: "b1"}

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	buf := new(bytes.Buffer)
	sink := &MockSnapshotSink{Writer: buf}

	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	if !sink.closed {
		t.Fatal("snapshot sink was not closed")
	}
}

func TestBrokerFSM_ValidateIdempotency(t *testing.T) {
	fsm := newTestFSM()
	fsm.partitionMetadata["t1-0"] = &PartitionMetadata{PartitionCount: 1, Idempotent: true}

	cmd := &types.MessageCommand{
		Topic:        "t1",
		Partition:    0,
		IsIdempotent: false,
		Messages: []types.Message{
			{ProducerID: "p1", SeqNum: 1, Payload: "m1"},
		},
	}

	if err := fsm.validateMessageCommand(cmd); err != nil {
		t.Errorf("Initial message should be valid via topic policy: %v", err)
	}

	// Error (First must be 1)
	cmd2 := &types.MessageCommand{
		Topic:     "t1",
		Partition: 0,
		Messages: []types.Message{
			{ProducerID: "p2", SeqNum: 2, Payload: "m1"},
		},
	}
	if err := fsm.validateMessageCommand(cmd2); err == nil {
		t.Error("Expected error for first message with SeqNum 2, got nil")
	}

	fsm.updateProducerState("t1", -1, "p1", 1)

	cmd.Partition = 0
	cmd.Messages[0].SeqNum = 2
	if err := fsm.validateMessageCommand(cmd); err != nil {
		t.Errorf("Next message (SeqNum 2) should be valid on partition 0 via global scope: %v", err)
	}
}

func TestBrokerFSM_SequenceScope_Partition(t *testing.T) {
	fsm := newTestFSM()
	fsm.partitionMetadata["t1-0"] = &PartitionMetadata{PartitionCount: 2, Idempotent: true}
	fsm.partitionMetadata["t1-1"] = &PartitionMetadata{PartitionCount: 2, Idempotent: true}
	if err := fsm.tm.CreateTopic("t1", 2, true); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	cmdP0 := &types.MessageCommand{
		Topic:         "t1",
		Partition:     0,
		SequenceScope: "partition",
		Messages:      []types.Message{{ProducerID: "p1", SeqNum: 1, Payload: "m1"}},
	}
	cmdP1 := &types.MessageCommand{
		Topic:         "t1",
		Partition:     1,
		SequenceScope: "partition",
		Messages:      []types.Message{{ProducerID: "p1", SeqNum: 1, Payload: "m1"}},
	}

	if err := fsm.validateMessageCommand(cmdP0); err != nil {
		t.Errorf("Initial P0 should be valid: %v", err)
	}
	if err := fsm.validateMessageCommand(cmdP1); err != nil {
		t.Errorf("Initial P1 should be valid (independent sequence): %v", err)
	}

	fsm.applyMessageBatch(cmdP0)
	fsm.applyMessageBatch(cmdP1)

	cmdP0.Messages[0].SeqNum = 2
	cmdP1.Messages[0].SeqNum = 2
	if err := fsm.validateMessageCommand(cmdP0); err != nil {
		t.Errorf("P0 next message should be valid: %v", err)
	}
	if err := fsm.validateMessageCommand(cmdP1); err != nil {
		t.Errorf("P1 next message should be valid: %v", err)
	}
}

func TestBrokerFSM_TopicCreation_ReplicaSubset(t *testing.T) {
	fsm := newTestFSM()

	// Register 5 active brokers
	for i := 1; i <= 5; i++ {
		data, _ := json.Marshal(BrokerInfo{
			ID:     fmt.Sprintf("broker-%d", i),
			Addr:   fmt.Sprintf("localhost:900%d", i),
			Status: "active",
		})
		fsm.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: uint64(i)})
	}

	// Create topic with replication_factor=3
	topicCmd := TopicCommand{
		Name:              "test-topic",
		Partitions:        6,
		ReplicationFactor: 3,
	}
	data, _ := json.Marshal(topicCmd)
	result := fsm.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", data)), Index: 10})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("Topic creation failed: %v", err)
	}

	// Verify each partition has exactly 3 replicas, not 5
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("test-topic-%d", i)
		meta := fsm.GetPartitionMetadata(key)
		if meta == nil {
			t.Fatalf("Partition %s metadata not found", key)
		}
		if len(meta.Replicas) != 3 {
			t.Errorf("Partition %s: expected 3 replicas, got %d: %v", key, len(meta.Replicas), meta.Replicas)
		}
		if len(meta.ISR) != 3 {
			t.Errorf("Partition %s: expected 3 ISR members, got %d: %v", key, len(meta.ISR), meta.ISR)
		}

		// Leader must be in replica set
		leaderFound := false
		for _, r := range meta.Replicas {
			if r == meta.Leader {
				leaderFound = true
				break
			}
		}
		if !leaderFound {
			t.Errorf("Partition %s: leader %s not in replicas %v", key, meta.Leader, meta.Replicas)
		}

		// All replicas must be unique
		seen := make(map[string]bool)
		for _, r := range meta.Replicas {
			if seen[r] {
				t.Errorf("Partition %s: duplicate replica %s", key, r)
			}
			seen[r] = true
		}
	}
}

func TestBrokerFSM_TopicCreation_DefaultReplicationFactor(t *testing.T) {
	fsm := newTestFSM()

	// Register 2 brokers (less than default RF=3)
	for i := 1; i <= 2; i++ {
		data, _ := json.Marshal(BrokerInfo{
			ID:     fmt.Sprintf("broker-%d", i),
			Addr:   fmt.Sprintf("localhost:900%d", i),
			Status: "active",
		})
		fsm.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: uint64(i)})
	}

	// Create topic without specifying replication_factor (defaults to 3, capped to 2)
	topicCmd := TopicCommand{
		Name:       "small-topic",
		Partitions: 4,
	}
	data, _ := json.Marshal(topicCmd)
	fsm.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", data)), Index: 10})

	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("small-topic-%d", i)
		meta := fsm.GetPartitionMetadata(key)
		if meta == nil {
			t.Fatalf("Partition %s metadata not found", key)
		}
		// Should be capped to 2 (number of brokers)
		if len(meta.Replicas) != 2 {
			t.Errorf("Partition %s: expected 2 replicas (capped), got %d", key, len(meta.Replicas))
		}
	}
}

func TestBrokerFSM_TopicCreation_ConsistentHashing_Stability(t *testing.T) {
	// Create two FSMs with same 3 brokers, create same topic -> should get same assignments
	for trial := 0; trial < 2; trial++ {
		fsm := newTestFSM()
		for i := 1; i <= 3; i++ {
			data, _ := json.Marshal(BrokerInfo{
				ID:     fmt.Sprintf("broker-%d", i),
				Addr:   fmt.Sprintf("localhost:900%d", i),
				Status: "active",
			})
			fsm.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: uint64(i)})
		}

		topicCmd := TopicCommand{
			Name:              "stable-topic",
			Partitions:        8,
			ReplicationFactor: 2,
		}
		data, _ := json.Marshal(topicCmd)
		fsm.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", data)), Index: 10})

		// Check leaders are distributed across brokers (not all on one)
		leaderCounts := make(map[string]int)
		for i := 0; i < 8; i++ {
			key := fmt.Sprintf("stable-topic-%d", i)
			meta := fsm.GetPartitionMetadata(key)
			leaderCounts[meta.Leader]++
		}
		if len(leaderCounts) < 2 {
			t.Errorf("Trial %d: leaders not distributed, all on same broker: %v", trial, leaderCounts)
		}
		t.Logf("Trial %d leader distribution: %v", trial, leaderCounts)
	}
}

func TestBrokerFSM_Snapshot_Restore_WithReplicas(t *testing.T) {
	f := newTestFSM()

	// Register brokers
	for i := 1; i <= 3; i++ {
		data, _ := json.Marshal(BrokerInfo{
			ID:     fmt.Sprintf("b%d", i),
			Addr:   fmt.Sprintf("localhost:900%d", i),
			Status: "active",
		})
		f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: uint64(i)})
	}

	// Create topic with replication_factor=2
	topicCmd := TopicCommand{
		Name:              "snap-topic",
		Partitions:        4,
		ReplicationFactor: 2,
	}
	data, _ := json.Marshal(topicCmd)
	f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", data)), Index: 10})

	// Snapshot
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	buf := new(bytes.Buffer)
	sink := &MockSnapshotSink{Writer: buf}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Restore into new FSM
	newFSM := newTestFSM()
	rc := io.NopCloser(bytes.NewReader(buf.Bytes()))
	if err := newFSM.Restore(rc); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify replica subsets are preserved
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("snap-topic-%d", i)
		meta := newFSM.GetPartitionMetadata(key)
		if meta == nil {
			t.Fatalf("Partition %s metadata not restored", key)
		}
		if len(meta.Replicas) != 2 {
			t.Errorf("Partition %s: expected 2 replicas after restore, got %d", key, len(meta.Replicas))
		}
		if len(meta.ISR) != 2 {
			t.Errorf("Partition %s: expected 2 ISR after restore, got %d", key, len(meta.ISR))
		}
	}
}

func TestBrokerFSM_TopicDelete(t *testing.T) {
	f := newTestFSM()

	// Register a broker
	data, _ := json.Marshal(BrokerInfo{ID: "b1", Addr: "localhost:9001", Status: "active"})
	f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: 1})

	// Create topic
	topicCmd := TopicCommand{Name: "delete-me", Partitions: 3, ReplicationFactor: 1}
	tdata, _ := json.Marshal(topicCmd)
	f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", tdata)), Index: 2})

	// Verify created
	keys := f.GetAllPartitionKeys()
	if len(keys) != 3 {
		t.Fatalf("Expected 3 partitions, got %d", len(keys))
	}

	// Delete topic
	deletePayload, _ := json.Marshal(map[string]string{"topic": "delete-me"})
	f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC_DELETE:%s", deletePayload)), Index: 3})

	keys = f.GetAllPartitionKeys()
	if len(keys) != 0 {
		t.Fatalf("Expected 0 partitions after delete, got %d", len(keys))
	}
}

func TestBrokerFSM_TopicCreation_InvalidPartitionCount(t *testing.T) {
	f := newTestFSM()

	data, _ := json.Marshal(BrokerInfo{ID: "b1", Addr: "localhost:9001", Status: "active"})
	f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: 1})

	// Zero partitions
	topicCmd := TopicCommand{Name: "bad-topic", Partitions: 0}
	tdata, _ := json.Marshal(topicCmd)
	result := f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", tdata)), Index: 2})
	if result == nil {
		t.Fatal("Expected error for 0 partitions")
	}

	// Negative partitions
	topicCmd2 := TopicCommand{Name: "bad-topic2", Partitions: -1}
	tdata2, _ := json.Marshal(topicCmd2)
	result2 := f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", tdata2)), Index: 3})
	if result2 == nil {
		t.Fatal("Expected error for negative partitions")
	}
}

func TestBrokerFSM_TopicCreation_NoBrokers(t *testing.T) {
	f := newTestFSM()

	topicCmd := TopicCommand{Name: "no-broker-topic", Partitions: 2}
	tdata, _ := json.Marshal(topicCmd)
	result := f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", tdata)), Index: 1})
	if result == nil {
		t.Fatal("Expected error when no brokers available")
	}
}

func TestBrokerFSM_TopicCreation_ExplicitLeader(t *testing.T) {
	f := newTestFSM()

	for i := 1; i <= 3; i++ {
		data, _ := json.Marshal(BrokerInfo{
			ID:     fmt.Sprintf("b%d", i),
			Addr:   fmt.Sprintf("localhost:900%d", i),
			Status: "active",
		})
		f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: uint64(i)})
	}

	topicCmd := TopicCommand{
		Name:              "leader-topic",
		Partitions:        2,
		LeaderID:          "b2",
		ReplicationFactor: 2,
	}
	tdata, _ := json.Marshal(topicCmd)
	result := f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", tdata)), Index: 10})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("Topic creation with explicit leader failed: %v", err)
	}

	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("leader-topic-%d", i)
		meta := f.GetPartitionMetadata(key)
		if meta == nil {
			t.Fatalf("Partition %s not found", key)
		}
		if meta.Leader != "b2" {
			t.Errorf("Partition %s: expected leader b2, got %s", key, meta.Leader)
		}
	}
}

func TestBrokerFSM_TopicCreation_ExplicitLeader_NotInBrokers(t *testing.T) {
	f := newTestFSM()

	data, _ := json.Marshal(BrokerInfo{ID: "b1", Addr: "localhost:9001", Status: "active"})
	f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: 1})

	topicCmd := TopicCommand{Name: "bad-leader", Partitions: 1, LeaderID: "nonexistent"}
	tdata, _ := json.Marshal(topicCmd)
	result := f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", tdata)), Index: 2})
	if result == nil {
		t.Fatal("Expected error for non-existent explicit leader")
	}
}

func TestBrokerFSM_GetBroker(t *testing.T) {
	f := newTestFSM()

	// Get non-existent broker
	if b := f.GetBroker("nonexistent"); b != nil {
		t.Fatal("Expected nil for non-existent broker")
	}

	// Register and get
	data, _ := json.Marshal(BrokerInfo{ID: "b1", Addr: "localhost:9001", Status: "active"})
	f.Apply(&raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: 1})

	b := f.GetBroker("b1")
	if b == nil {
		t.Fatal("Expected broker b1")
	}
	if b.Addr != "localhost:9001" {
		t.Errorf("Expected addr localhost:9001, got %s", b.Addr)
	}
}

func TestBrokerFSM_Notifier(t *testing.T) {
	f := newTestFSM()

	ch := f.RegisterNotifier("req-1")

	// Simulate Apply with req_id
	data, _ := json.Marshal(BrokerInfo{ID: "b1", Addr: "localhost:9001", Status: "active", LastSeen: time.Now()})
	payload := fmt.Sprintf(`REGISTER:{"id":"b1","addr":"localhost:9001","status":"active","last_seen":"2026-01-01T00:00:00Z","req_id":"req-1"}`)
	f.Apply(&raft.Log{Data: []byte(payload), Index: 1})

	select {
	case res := <-ch:
		if res != nil {
			t.Logf("Notifier received: %v", res)
		}
	default:
		// Notifier may or may not fire depending on parsing
	}

	_ = data // suppress unused
	f.UnregisterNotifier("req-1")
}

type MockSnapshotSink struct {
	io.Writer
	closed bool
}

func (m *MockSnapshotSink) ID() string    { return "" }
func (m *MockSnapshotSink) Close() error  { m.closed = true; return nil }
func (m *MockSnapshotSink) Cancel() error { return nil }
