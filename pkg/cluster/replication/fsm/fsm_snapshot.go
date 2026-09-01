package fsm

import (
	"encoding/json"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/topic"
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
	topicState        map[string]*topic.Definition
}

func (s *BrokerFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	state := BrokerFSMState{
		Version:           s.writeVersion(),
		Applied:           s.applied,
		Logs:              s.logs,
		Brokers:           s.brokers,
		PartitionMetadata: s.partitionMetadata,
		ProducerState:     s.producerState,
		GroupState:        s.groupState,
		TransactionState:  s.transactionState,
		TopicState:        s.topicState,
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

func (s *BrokerFSMSnapshot) writeVersion() int {
	for _, definition := range s.topicState {
		if definition != nil && definition.LifecycleEpoch > topic.InitialLifecycleEpoch {
			return 8
		}
	}
	// A first-generation snapshot carries additive epoch fields but retains the
	// previous version so old followers can keep participating while upgrading.
	return 7
}

func (s *BrokerFSMSnapshot) Release() {}
