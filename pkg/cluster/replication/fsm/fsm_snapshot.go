package fsm

import (
	"encoding/json"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/cursus-io/cursus/util"
	"github.com/hashicorp/raft"
)

type BrokerFSMSnapshot struct {
	applied           uint64
	logs              map[uint64]*ReplicationEntry
	brokers           map[string]*BrokerInfo
	partitionMetadata map[string]*PartitionMetadata
	producerState     map[string]map[int]map[string]ProducerSequence
	groupState        map[string]*coordinator.GroupStateSnapshot
	transactionState  map[string]*transaction.Snapshot
}

func (s *BrokerFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	state := BrokerFSMState{
		Version:           5,
		Applied:           s.applied,
		Logs:              s.logs,
		Brokers:           s.brokers,
		PartitionMetadata: s.partitionMetadata,
		ProducerState:     s.producerState,
		GroupState:        s.groupState,
		TransactionState:  s.transactionState,
	}

	util.Debug("Persisting snapshot data")
	err := json.NewEncoder(sink).Encode(state)
	if err != nil {
		cancelErr := sink.Cancel()
		if cancelErr != nil {
			util.Error("Failed to cancel snapshot after encoding error: %v", cancelErr)
		}
		return err
	}
	return sink.Close()
}

func (s *BrokerFSMSnapshot) Release() {}
