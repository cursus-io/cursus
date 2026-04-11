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
	fsm.partitionMetadata["t1"] = &PartitionMetadata{PartitionCount: 1, Idempotent: true}

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
	fsm.partitionMetadata["t1"] = &PartitionMetadata{PartitionCount: 2, Idempotent: true}
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

type MockSnapshotSink struct {
	io.Writer
	closed bool
}

func (m *MockSnapshotSink) ID() string    { return "" }
func (m *MockSnapshotSink) Close() error  { m.closed = true; return nil }
func (m *MockSnapshotSink) Cancel() error { return nil }
