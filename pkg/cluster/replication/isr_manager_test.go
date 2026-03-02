package replication

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/hashicorp/raft"
)

type MockStorageHandler struct{}

func (m *MockStorageHandler) ReadMessages(o uint64, max int) ([]types.Message, error) {
	return nil, nil
}
func (m *MockStorageHandler) GetAbsoluteOffset() uint64      { return 0 }
func (m *MockStorageHandler) GetLatestOffset() uint64        { return 0 }
func (m *MockStorageHandler) GetSegmentPath(b uint64) string { return "" }
func (m *MockStorageHandler) AppendMessage(t string, p int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *MockStorageHandler) AppendMessageSync(t string, p int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *MockStorageHandler) WriteBatch(b []types.DiskMessage) error { return nil }
func (m *MockStorageHandler) Flush()                                 {}
func (m *MockStorageHandler) Close() error                           { return nil }

type FakeHandlerProvider struct{}

func (f *FakeHandlerProvider) GetHandler(t string, p int) (types.StorageHandler, error) {
	return &MockStorageHandler{}, nil
}

func TestISRManager_Quorum(t *testing.T) {
	cfg := &config.Config{LogDir: t.TempDir()}

	hp := &FakeHandlerProvider{}
	tm := topic.NewTopicManager(cfg, hp, nil)
	dm := disk.NewDiskManager(cfg)
	cd := coordinator.NewCoordinator(cfg, tm)
	brokerFSM := fsm.NewBrokerFSM(dm, tm, cd)

	topicName := "test-topic"
	partitionID := 0

	topicPayload := map[string]interface{}{
		"name":       topicName,
		"partitions": 1,
		"leader_id":  "node1",
	}
	topicData, _ := json.Marshal(topicPayload)
	brokerFSM.Apply(&raft.Log{Data: []byte(fmt.Sprintf("TOPIC:%s", string(topicData)))})

	partitionMetadata := fsm.PartitionMetadata{
		Replicas:       []string{"node1", "node2", "node3"},
		ISR:            []string{"node1", "node2", "node3"},
		PartitionCount: 1,
	}
	metaData, _ := json.Marshal(partitionMetadata)
	key := fmt.Sprintf("%s-%d", topicName, partitionID)
	brokerFSM.Apply(&raft.Log{Data: []byte(fmt.Sprintf("PARTITION:%s:%s", key, string(metaData)))})

	isrManager := NewISRManager(brokerFSM, "node1", 100*time.Millisecond)

	isrManager.UpdateHeartbeat("node1")
	isrManager.UpdateHeartbeat("node2")
	isrManager.ComputeISR(topicName, partitionID)

	if !isrManager.HasQuorum(topicName, partitionID, 2) {
		t.Error("Expected quorum to be met (2 in ISR)")
	}

	// heartbeat timeout
	time.Sleep(200 * time.Millisecond)

	isrManager.UpdateHeartbeat("node1")
	isrManager.CleanStaleHeartbeats()
	isrManager.ComputeISR(topicName, partitionID)

	isr := isrManager.GetISR(topicName, partitionID)
	if len(isr) != 1 || isr[0] != "node1" {
		t.Errorf("Expected ISR to be [node1], got %v", isr)
	}

	if isrManager.HasQuorum(topicName, partitionID, 2) {
		t.Error("Expected quorum NOT to be met (only 1 in ISR, minISR=2)")
	}
}
