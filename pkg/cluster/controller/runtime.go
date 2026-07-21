package controller

import (
	"sort"
	"strconv"
	"strings"
)

// PartitionRuntimeSnapshot describes replicated partition placement.
type PartitionRuntimeSnapshot struct {
	Topic       string
	Partition   int
	Leader      string
	LeaderEpoch int
	Replicas    int
	InSync      int
}

// MaterializationAttemptsSnapshot counts local convergence attempts by result.
type MaterializationAttemptsSnapshot struct {
	Success uint64
	Failure uint64
}

// RuntimeSnapshot is a point-in-time view of cluster control state.
type RuntimeSnapshot struct {
	Enabled                           bool
	BrokerID                          string
	BrokerCount                       int
	HasLeader                         bool
	IsLeader                          bool
	Offline                           int
	UnderReplicated                   int
	TopicMaterializationsPending      map[string]int
	TopicMaterializationAttempts      map[string]MaterializationAttemptsSnapshot
	TopicMaterializationOldestPending float64
	PartitionDetails                  []PartitionRuntimeSnapshot
}

// RuntimeSnapshot returns cluster metadata without exposing mutable FSM state.
func (cc *ClusterController) RuntimeSnapshot() RuntimeSnapshot {
	if cc == nil || cc.RaftManager == nil {
		return RuntimeSnapshot{}
	}

	snapshot := RuntimeSnapshot{
		Enabled:   true,
		BrokerID:  cc.brokerID,
		HasLeader: cc.RaftManager.GetLeaderAddress() != "",
		IsLeader:  cc.RaftManager.IsLeader(),
	}
	fsmState := cc.RaftManager.GetFSM()
	if fsmState == nil {
		return snapshot
	}

	snapshot.BrokerCount = len(fsmState.GetBrokers())
	materialization := fsmState.TopicMaterializationRuntimeSnapshot()
	snapshot.TopicMaterializationsPending = make(map[string]int, len(materialization.PendingByOperation))
	for operation, count := range materialization.PendingByOperation {
		snapshot.TopicMaterializationsPending[operation] = count
	}
	snapshot.TopicMaterializationAttempts = make(map[string]MaterializationAttemptsSnapshot, len(materialization.AttemptsByOperation))
	for operation, attempts := range materialization.AttemptsByOperation {
		snapshot.TopicMaterializationAttempts[operation] = MaterializationAttemptsSnapshot{
			Success: attempts.Success,
			Failure: attempts.Failure,
		}
	}
	snapshot.TopicMaterializationOldestPending = materialization.OldestPending.Seconds()
	keys := fsmState.GetAllPartitionKeys()
	sort.Strings(keys)
	for _, key := range keys {
		separator := strings.LastIndexByte(key, '-')
		if separator <= 0 || separator == len(key)-1 {
			continue
		}
		partition, err := strconv.Atoi(key[separator+1:])
		if err != nil {
			continue
		}
		metadata := fsmState.GetPartitionMetadata(key)
		if metadata == nil {
			continue
		}

		detail := PartitionRuntimeSnapshot{
			Topic:       key[:separator],
			Partition:   partition,
			Leader:      metadata.Leader,
			LeaderEpoch: metadata.LeaderEpoch,
			Replicas:    len(metadata.Replicas),
			InSync:      len(metadata.ISR),
		}
		if detail.Leader == "" {
			snapshot.Offline++
		}
		if detail.InSync < detail.Replicas {
			snapshot.UnderReplicated++
		}
		snapshot.PartitionDetails = append(snapshot.PartitionDetails, detail)
	}

	return snapshot
}
