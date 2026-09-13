package fsm

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ReplicaMaterializationIssue describes a configured local replica whose
// durable data boundary has not converged with the committed Raft boundary.
type ReplicaMaterializationIssue struct {
	Topic          string
	Partition      int
	CommittedHWM   uint64
	LocalLEO       uint64
	LocalHWM       uint64
	LeaderEpoch    int
	LifecycleEpoch uint64
	Error          string
	PendingSince   time.Time
}

// ReplicaMaterializationIssues returns a stable detached view of pending local
// replica-data work.
func (f *BrokerFSM) ReplicaMaterializationIssues() []ReplicaMaterializationIssue {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	issues := make([]ReplicaMaterializationIssue, 0, len(f.replicaMaterialization))
	for _, issue := range f.replicaMaterialization {
		issues = append(issues, issue)
	}
	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Topic != issues[j].Topic {
			return issues[i].Topic < issues[j].Topic
		}
		return issues[i].Partition < issues[j].Partition
	})
	return issues
}

// ReconcileReplicaMaterializations refreshes local data convergence state.
// Network transfer is performed by the controller catch-up loop; this scan is
// also called by the topic reconcile loop so an existing handler is never
// mistaken for completed data materialization.
func (f *BrokerFSM) ReconcileReplicaMaterializations(brokerID string) error {
	if f == nil || brokerID == "" {
		return nil
	}
	f.mu.RLock()
	metadata := make(map[string]PartitionMetadata, len(f.partitionMetadata))
	for key, current := range f.partitionMetadata {
		if current == nil || !current.CommittedHWMKnown || !containsString(current.Replicas, brokerID) {
			continue
		}
		copy := *current
		copy.Replicas = append([]string(nil), current.Replicas...)
		metadata[key] = copy
	}
	definitions := copyTopicState(f.topicState)
	topicManager := f.tm
	previous := make(map[string]ReplicaMaterializationIssue, len(f.replicaMaterialization))
	for key, issue := range f.replicaMaterialization {
		previous[key] = issue
	}
	f.mu.RUnlock()

	now := time.Now()
	pending := make(map[string]ReplicaMaterializationIssue)
	var reconcileErr error
	for key, current := range metadata {
		topicName, partitionID, ok := splitPartitionMetadataKey(key)
		if !ok {
			continue
		}
		issue := ReplicaMaterializationIssue{
			Topic: topicName, Partition: partitionID, CommittedHWM: current.CommittedHWM,
			LeaderEpoch: current.LeaderEpoch, LifecycleEpoch: current.LifecycleEpoch,
			PendingSince: now,
		}
		if old, exists := previous[key]; exists && !old.PendingSince.IsZero() {
			issue.PendingSince = old.PendingSince
		}
		definition := definitions[topicName]
		var issueErr error
		switch {
		case definition == nil:
			issueErr = fmt.Errorf("authoritative topic definition is unavailable")
		case definition.LifecycleEpoch != current.LifecycleEpoch:
			issueErr = fmt.Errorf("topic lifecycle mismatch: definition=%d partition=%d", definition.LifecycleEpoch, current.LifecycleEpoch)
		case topicManager == nil:
			issueErr = fmt.Errorf("topic manager is unavailable")
		default:
			localTopic := topicManager.GetTopic(topicName)
			if localTopic == nil {
				issueErr = fmt.Errorf("local topic is not materialized")
			} else if localTopic.Definition().LifecycleEpoch != current.LifecycleEpoch {
				issueErr = fmt.Errorf("local topic lifecycle is stale")
			} else {
				partition, err := localTopic.GetPartition(partitionID)
				if err != nil {
					issueErr = err
				} else {
					issue.LocalLEO = partition.NextOffset()
					issue.LocalHWM = partition.GetHWM()
					switch {
					case issue.LocalHWM > current.CommittedHWM:
						issueErr = fmt.Errorf("local HWM %d is ahead of committed HWM %d", issue.LocalHWM, current.CommittedHWM)
					case issue.LocalLEO < current.CommittedHWM:
						issueErr = fmt.Errorf("local replica has not reached committed HWM")
					case issue.LocalHWM < current.CommittedHWM:
						issueErr = fmt.Errorf("local committed HWM has not been applied")
					}
				}
			}
		}
		if issueErr == nil {
			continue
		}
		issue.Error = issueErr.Error()
		pending[key] = issue
		reconcileErr = errors.Join(reconcileErr, fmt.Errorf("%s: %w", key, issueErr))
	}

	f.mu.Lock()
	f.replicaMaterialization = pending
	f.mu.Unlock()
	return reconcileErr
}

// ReplicaMaterializationReadinessError recalculates and reports node-local
// replica data readiness for the supplied broker identity.
func (f *BrokerFSM) ReplicaMaterializationReadinessError(brokerID string) error {
	_ = f.ReconcileReplicaMaterializations(brokerID)
	issues := f.ReplicaMaterializationIssues()
	if len(issues) == 0 {
		return nil
	}
	first := issues[0]
	return fmt.Errorf(
		"%d replica materialization operation(s) pending: %s-%d leo=%d hwm=%d committed_hwm=%d: %s",
		len(issues), first.Topic, first.Partition, first.LocalLEO, first.LocalHWM, first.CommittedHWM, first.Error,
	)
}

func splitPartitionMetadataKey(key string) (string, int, bool) {
	separator := strings.LastIndexByte(key, '-')
	if separator <= 0 || separator == len(key)-1 {
		return "", 0, false
	}
	partition, err := strconv.Atoi(key[separator+1:])
	if err != nil || partition < 0 {
		return "", 0, false
	}
	return key[:separator], partition, true
}
