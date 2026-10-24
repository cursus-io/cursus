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
	cooperativeConsumerActivated     bool
	topicCleanupBoundaries           map[string]*coordinator.TopicCleanupBoundary
	consumerHistoryActivated         bool
	retentionTimeActivated           bool
	consumerRecovery                 *coordinator.ConsumerRecoveryEnvelope
	applied                          uint64
	logs                             map[uint64]*ReplicationEntry
	brokers                          map[string]*BrokerInfo
	retiredBrokerIncarnations        map[string][]string
	partitionMetadata                map[string]*PartitionMetadata
	producerState                    map[string]map[int]map[string]ProducerSequence
	groupState                       map[string]*coordinator.GroupStateSnapshot
	transactionState                 map[string]*transaction.Snapshot
	nextProducerEpoch                uint64
	transactionCoordinatorShards     map[int]TransactionCoordinatorShard
	transactionCoordinatorShardCount int
	offsetReservationsActivated      bool
	consumerMetadataRoutingActivated bool
	topicState                       map[string]*topic.Definition
}

func (s *BrokerFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	state := BrokerFSMState{
		CooperativeConsumerActivated:     s.cooperativeConsumerActivated,
		TopicCleanupBoundaries:           s.topicCleanupBoundaries,
		ConsumerHistoryActivated:         s.consumerHistoryActivated,
		RetentionTimeActivated:           s.retentionTimeActivated,
		Version:                          s.writeVersion(),
		Applied:                          s.applied,
		Logs:                             s.logs,
		Brokers:                          s.brokers,
		RetiredBrokerIncarnations:        s.retiredBrokerIncarnations,
		PartitionMetadata:                s.partitionMetadata,
		ProducerState:                    s.producerState,
		GroupState:                       s.groupState,
		ConsumerRecovery:                 s.consumerRecovery,
		TransactionState:                 s.transactionState,
		NextProducerEpoch:                &s.nextProducerEpoch,
		TransactionCoordinatorShards:     s.transactionCoordinatorShards,
		TransactionCoordinatorShardCount: s.transactionCoordinatorShardCount,
		OffsetReservationsActivated:      s.offsetReservationsActivated,
		ConsumerMetadataRoutingActivated: s.consumerMetadataRoutingActivated,
		TopicState:                       s.topicState,
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
	if s.cooperativeConsumerActivated {
		return SnapshotVersionCooperativeRuntime
	}
	for _, group := range s.groupState {
		if group != nil && group.Cooperative != nil {
			return SnapshotVersionCooperative
		}
	}
	return SnapshotVersionCurrent
}

func (s *BrokerFSMSnapshot) Release() {}
