package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/transaction"
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
	ID         string    `json:"id"`
	Addr       string    `json:"addr"`
	ClientAddr string    `json:"client_addr,omitempty"`
	Status     string    `json:"status"`
	LastSeen   time.Time `json:"last_seen"`
}

type ProducerSequence struct {
	Epoch int64  `json:"epoch"`
	Seq   uint64 `json:"seq"`
}

func (s *ProducerSequence) UnmarshalJSON(data []byte) error {
	var legacySeq int64
	if err := json.Unmarshal(data, &legacySeq); err == nil {
		if legacySeq > 0 {
			s.Seq = uint64(legacySeq)
		}
		return nil
	}
	type alias ProducerSequence
	var decoded alias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*s = ProducerSequence(decoded)
	return nil
}

type BrokerFSMState struct {
	Version           int                                            `json:"version"`
	Applied           uint64                                         `json:"applied"`
	Logs              map[uint64]*ReplicationEntry                   `json:"logs"`
	Brokers           map[string]*BrokerInfo                         `json:"brokers"`
	PartitionMetadata map[string]*PartitionMetadata                  `json:"partitionMetadata"`
	ProducerState     map[string]map[int]map[string]ProducerSequence `json:"producerState"`
	GroupState        map[string]*coordinator.GroupStateSnapshot     `json:"groupState,omitempty"`
	TransactionState  map[string]*transaction.Snapshot               `json:"transactionState,omitempty"`
}

type BrokerFSM struct {
	notifiers map[string]chan interface{}
	mu        sync.RWMutex

	logs              map[uint64]*ReplicationEntry
	brokers           map[string]*BrokerInfo
	partitionMetadata map[string]*PartitionMetadata
	producerState     map[string]map[int]map[string]ProducerSequence // Topic -> Partition -> ProducerID -> Last Epoch/Seq
	applied           uint64

	tm                       *topic.TopicManager
	cd                       *coordinator.Coordinator
	txn                      *transaction.Manager
	restoredTransactionState map[string]*transaction.Snapshot
}

func NewBrokerFSM(tm *topic.TopicManager, cd *coordinator.Coordinator) *BrokerFSM {
	return &BrokerFSM{
		notifiers:         make(map[string]chan interface{}),
		logs:              make(map[uint64]*ReplicationEntry),
		brokers:           make(map[string]*BrokerInfo),
		partitionMetadata: make(map[string]*PartitionMetadata),
		producerState:     make(map[string]map[int]map[string]ProducerSequence),
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

func (f *BrokerFSM) SetTransactionManager(txn *transaction.Manager) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.txn = txn
	if f.txn != nil && f.restoredTransactionState != nil {
		f.txn.ImportState(f.restoredTransactionState)
		util.Info("FSM: Imported %d deferred restored transactions", len(f.restoredTransactionState))
		f.restoredTransactionState = nil
	}
}

func (f *BrokerFSM) Apply(log *raft.Log) interface{} {
	data := string(log.Data)
	var reqID string

	// skip prefixs like "MESSAGE:" or "REGISTER:"
	if startIdx := strings.Index(data, "{"); startIdx != -1 {
		jsonData := data[startIdx:]
		dec := json.NewDecoder(strings.NewReader(jsonData))
		var meta struct {
			ReqID string `json:"req_id"`
		}

		if err := dec.Decode(&meta); err != nil {
			if !strings.Contains(data, "REGISTER:") && !strings.Contains(data, "DEREGISTER:") {
				util.Debug("FSM Apply: potential JSON decode issue for req_id (Prefix: %s): %v", data[:startIdx], err)
			}
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
	case strings.HasPrefix(data, "TOPIC_DELETE:"):
		res = f.applyTopicDeleteCommand(strings.TrimPrefix(data, "TOPIC_DELETE:"))
	case strings.HasPrefix(data, "PARTITION:"):
		res = f.applyPartitionCommand(strings.TrimPrefix(data, "PARTITION:"))
	case strings.HasPrefix(data, "GROUP_SYNC:"):
		res = f.applyGroupSyncCommand(strings.TrimPrefix(data, "GROUP_SYNC:"))
	case strings.HasPrefix(data, "OFFSET_SYNC:"):
		res = f.applyOffsetSyncCommand(strings.TrimPrefix(data, "OFFSET_SYNC:"))
	case strings.HasPrefix(data, "BATCH_OFFSET:"):
		res = f.applyBatchOffsetSyncCommand(strings.TrimPrefix(data, "BATCH_OFFSET:"))
	case strings.HasPrefix(data, "TXN_SYNC:"):
		res = f.applyTransactionSyncCommand(strings.TrimPrefix(data, "TXN_SYNC:"))
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
	case 2:
		util.Info("FSM Restore: Validating snapshot Version 2 (with group state)")
	case 3:
		util.Info("FSM Restore: Validating snapshot Version 3 (with producer epochs)")
	case 4:
		util.Info("FSM Restore: Validating snapshot Version 4 (with transaction state)")
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
		f.producerState = make(map[string]map[int]map[string]ProducerSequence)
	}

	if state.GroupState != nil && f.cd != nil {
		f.cd.ImportState(state.GroupState)
		util.Info("FSM Restore: Restored %d consumer groups from snapshot", len(state.GroupState))
	}

	if state.TransactionState != nil {
		if f.txn != nil {
			f.txn.ImportState(state.TransactionState)
			util.Info("FSM Restore: Restored %d transactions from snapshot", len(state.TransactionState))
		} else {
			f.restoredTransactionState = state.TransactionState
			util.Info("FSM Restore: Deferred %d transactions until transaction manager is attached", len(state.TransactionState))
		}
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
		if v.Replicas != nil {
			metaCopy.Replicas = make([]string, len(v.Replicas))
			copy(metaCopy.Replicas, v.Replicas)
		}
		if v.ISR != nil {
			metaCopy.ISR = make([]string, len(v.ISR))
			copy(metaCopy.ISR, v.ISR)
		}
		metadataCopy[k] = &metaCopy
	}
	producerStateCopy := make(map[string]map[int]map[string]ProducerSequence, len(f.producerState))
	for topic, partitions := range f.producerState {
		partitionMap := make(map[int]map[string]ProducerSequence, len(partitions))
		for pID, producers := range partitions {
			producerMap := make(map[string]ProducerSequence, len(producers))
			for prodID, seq := range producers {
				producerMap[prodID] = seq
			}
			partitionMap[pID] = producerMap
		}
		producerStateCopy[topic] = partitionMap
	}

	var groupState map[string]*coordinator.GroupStateSnapshot
	if f.cd != nil {
		groupState = f.cd.ExportState()
	}
	var transactionState map[string]*transaction.Snapshot
	if f.txn != nil {
		transactionState = f.txn.ExportState()
	}

	util.Debug("Creating FSM snapshot")
	return &BrokerFSMSnapshot{
		applied:           f.applied,
		logs:              logsCopy,
		brokers:           brokersCopy,
		partitionMetadata: metadataCopy,
		producerState:     producerStateCopy,
		groupState:        groupState,
		transactionState:  transactionState,
	}, nil
}

func (f *BrokerFSM) GetPartitionMetadata(key string) *PartitionMetadata {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if meta := f.partitionMetadata[key]; meta != nil {
		copy := *meta
		// Deep copy slices to avoid aliasing internal FSM state
		if meta.Replicas != nil {
			copy.Replicas = make([]string, len(meta.Replicas))
			for i, r := range meta.Replicas {
				copy.Replicas[i] = r
			}
		}
		if meta.ISR != nil {
			copy.ISR = make([]string, len(meta.ISR))
			for i, r := range meta.ISR {
				copy.ISR[i] = r
			}
		}
		return &copy
	}
	return nil
}

func (f *BrokerFSM) GetBroker(id string) *BrokerInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if broker, ok := f.brokers[id]; ok {
		copy := *broker
		return &copy
	}
	return nil
}

func (f *BrokerFSM) RegisterNotifier(reqID string) chan interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	ch := make(chan interface{}, 1)
	f.notifiers[reqID] = ch
	return ch
}

func (f *BrokerFSM) UnregisterNotifier(reqID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.notifiers, reqID)
}

func (f *BrokerFSM) notify(reqID string, res interface{}) {
	f.mu.RLock()
	ch, ok := f.notifiers[reqID]
	f.mu.RUnlock()

	if ok {
		select {
		case ch <- res:
		default:
		}
	}
}
