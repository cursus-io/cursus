package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

type ReplicationEntry struct {
	Topic     string
	Partition int
	Message   types.Message
	Term      uint64
}

type BrokerInfo struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	Status   string    `json:"status"`
	LastSeen time.Time `json:"last_seen"`
}

type BrokerFSMState struct {
	Version           int                                 `json:"version"`
	Applied           uint64                              `json:"applied"`
	Logs              map[uint64]*ReplicationEntry        `json:"logs"`
	Brokers           map[string]*BrokerInfo              `json:"brokers"`
	PartitionMetadata map[string]*PartitionMetadata       `json:"partitionMetadata"`
	ProducerState     map[string]map[int]map[string]int64 `json:"producerState"`
}

type BrokerFSM struct {
	notifiers map[string]chan interface{}
	mu        sync.RWMutex

	logs              map[uint64]*ReplicationEntry
	brokers           map[string]*BrokerInfo
	partitionMetadata map[string]*PartitionMetadata
	producerState     map[string]map[int]map[string]int64 // Topic -> Partition -> ProducerID -> LastSeq
	applied           uint64

	dm *disk.DiskManager
	tm *topic.TopicManager
	cd *coordinator.Coordinator
}

func NewBrokerFSM(dm *disk.DiskManager, tm *topic.TopicManager, cd *coordinator.Coordinator) *BrokerFSM {
	return &BrokerFSM{
		notifiers:         make(map[string]chan interface{}),
		logs:              make(map[uint64]*ReplicationEntry),
		brokers:           make(map[string]*BrokerInfo),
		partitionMetadata: make(map[string]*PartitionMetadata),
		producerState:     make(map[string]map[int]map[string]int64),
		dm:                dm,
		tm:                tm,
		cd:                cd,
	}
}

func (f *BrokerFSM) GetBrokers() []BrokerInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var brokers []BrokerInfo
	for _, broker := range f.brokers {
		brokers = append(brokers, *broker)
	}
	return brokers
}

func (f *BrokerFSM) GetAllPartitionKeys() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	keys := make([]string, 0, len(f.partitionMetadata))
	for k := range f.partitionMetadata {
		keys = append(keys, k)
	}
	return keys
}

func (f *BrokerFSM) SetCoordinator(cd *coordinator.Coordinator) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cd = cd
}

func (f *BrokerFSM) Apply(log *raft.Log) interface{} {
	data := string(log.Data)
	var reqID string

	if startIdx := strings.Index(data, "{"); startIdx != -1 {
		dec := json.NewDecoder(strings.NewReader(data[startIdx:]))
		var meta struct {
			ReqID string `json:"req_id"`
		}

		if err := dec.Decode(&meta); err != nil {
			util.Error("FSM Apply: failed to decode req_id: %v", err)
		} else {
			reqID = meta.ReqID
		}
	}

	var res interface{}
	switch {
	case strings.HasPrefix(data, "REGISTER:"):
		res = f.applyRegisterCommand(strings.TrimPrefix(data, "REGISTER:"))
	case strings.HasPrefix(data, "DEREGISTER:"):
		res = f.applyDeregisterCommand(strings.TrimPrefix(data, "DEREGISTER:"))
	case strings.HasPrefix(data, "JOIN_GROUP:"):
		res = f.applyJoinGroupCommand(strings.TrimPrefix(data, "JOIN_GROUP:"))
	case strings.HasPrefix(data, "MESSAGE:"):
		res = f.applyMessageCommand(strings.TrimPrefix(data, "MESSAGE:"))
	case strings.HasPrefix(data, "BATCH:"):
		res = f.applyMessageCommand(strings.TrimPrefix(data, "BATCH:"))
	case strings.HasPrefix(data, "TOPIC:"):
		res = f.applyTopicCommand(strings.TrimPrefix(data, "TOPIC:"))
	case strings.HasPrefix(data, "PARTITION:"):
		res = f.applyPartitionCommand(data)
	case strings.HasPrefix(data, "GROUP_SYNC:"):
		res = f.applyGroupSyncCommand(strings.TrimPrefix(data, "GROUP_SYNC:"))
	case strings.HasPrefix(data, "OFFSET_SYNC:"):
		res = f.applyOffsetSyncCommand(strings.TrimPrefix(data, "OFFSET_SYNC:"))
	case strings.HasPrefix(data, "BATCH_OFFSET:"):
		res = f.applyBatchOffsetSyncCommand(strings.TrimPrefix(data, "BATCH_OFFSET:"))
	default:
		res = f.handleUnknownCommand(data)
	}

	if reqID != "" {
		f.notify(reqID, res)
	}

	f.mu.Lock()
	f.applied = log.Index
	f.mu.Unlock()

	return res
}

func (f *BrokerFSM) Restore(rc io.ReadCloser) error {
	defer func() {
		if err := rc.Close(); err != nil {
			util.Error("failed to close rc: %v", err)
		}
	}()

	util.Info("Starting FSM restore from snapshot")

	var state BrokerFSMState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		util.Error("Failed to decode snapshot: %v", err)
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	switch state.Version {
	case 0:
		util.Warn("FSM Restore: Legacy snapshot detected (Version 0).")
	case 1:
		util.Info("FSM Restore: Validating snapshot Version 1")
	default:
		return fmt.Errorf("unknown snapshot version: %d", state.Version)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.logs = state.Logs
	f.brokers = state.Brokers
	f.partitionMetadata = state.PartitionMetadata
	f.applied = state.Applied

	f.producerState = state.ProducerState
	if f.producerState == nil {
		f.producerState = make(map[string]map[int]map[string]int64)
	}

	util.Info("FSM restore completed: %d logs, %d brokers, %d partitions", len(state.Logs), len(state.Brokers), len(state.PartitionMetadata))
	return nil
}

func (f *BrokerFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	logsCopy := make(map[uint64]*ReplicationEntry, len(f.logs))
	for k, v := range f.logs {
		entryCopy := *v
		logsCopy[k] = &entryCopy
	}
	brokersCopy := make(map[string]*BrokerInfo, len(f.brokers))
	for k, v := range f.brokers {
		brokerCopy := *v
		brokersCopy[k] = &brokerCopy
	}
	metadataCopy := make(map[string]*PartitionMetadata, len(f.partitionMetadata))
	for k, v := range f.partitionMetadata {
		metaCopy := *v
		metadataCopy[k] = &metaCopy
	}
	producerStateCopy := make(map[string]map[int]map[string]int64, len(f.producerState))
	for topic, partitions := range f.producerState {
		partitionMap := make(map[int]map[string]int64, len(partitions))
		for pID, producers := range partitions {
			producerMap := make(map[string]int64, len(producers))
			for prodID, seq := range producers {
				producerMap[prodID] = seq
			}
			partitionMap[pID] = producerMap
		}
		producerStateCopy[topic] = partitionMap
	}

	util.Debug("Creating FSM snapshot")
	return &BrokerFSMSnapshot{
		applied:           f.applied,
		logs:              logsCopy,
		brokers:           brokersCopy,
		partitionMetadata: metadataCopy,
		producerState:     producerStateCopy,
	}, nil
}

func (f *BrokerFSM) GetPartitionMetadata(key string) *PartitionMetadata {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if meta := f.partitionMetadata[key]; meta != nil {
		copy := *meta
		return &copy
	}
	return nil
}

// todo. (issues #27)
func (f *BrokerFSM) getCurrentRaftLeaderID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if len(f.brokers) == 0 {
		return ""
	}

	var ids []string
	for id := range f.brokers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids[0]
}
