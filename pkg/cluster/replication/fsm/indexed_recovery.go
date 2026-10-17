package fsm

import (
	"context"
	"errors"
	"fmt"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
)

// ErrReplicaBootstrapRequired means retained histories have no comparable anchor.
// It never authorizes truncation or an empty-log reset.
var ErrReplicaBootstrapRequired = errors.New("replica bootstrap required: no common retained prefix")

func addReplicaRecoveryProof(partition *topic.Partition, request *ReplicaCatchupRequest, receiver, source BrokerInfo, inISR bool, definition *topic.Definition) error {
	release := partition.BeginReplicationMutation()
	defer release()
	witness, err := partition.CaptureReplicaRequestWitness(context.Background(), request.NextOffset)
	if err != nil {
		return err
	}
	if err := addReplicaRecoveryChallenge(partition, request, receiver, source, inISR, definition); err != nil {
		return err
	}
	if err := partition.ValidateReplicaRequestWitness(context.Background(), witness, request.NextOffset); err != nil {
		return err
	}
	request.LocalWitness = witness
	return nil
}

func addReplicaRecoveryChallenge(partition *topic.Partition, request *ReplicaCatchupRequest, receiver, source BrokerInfo, inISR bool, definition *topic.Definition) error {
	if err := addPreviousReplicaRecord(partition, request); err != nil {
		return err
	}
	if inISR || receiver.RecoveryProtocol < IndexedRecoveryProtocolVersion || source.RecoveryProtocol < IndexedRecoveryProtocolVersion || config.HasCleanupPolicy(definition.Policy.CleanupPolicy, config.CleanupPolicyCompact) {
		return nil
	}
	proofs, err := partition.ReplicaLineageCandidates(request.NextOffset)
	if err != nil {
		return err
	}
	request.RecoveryProtocol = IndexedRecoveryProtocolVersion
	request.PrefixCandidates = proofs
	return nil
}

func validateReplicaRecoveryRequest(request ReplicaCatchupRequest, source, receiver BrokerInfo, meta PartitionMetadata) error {
	if request.RecoveryProtocol != BootstrapRecoveryProtocolVersion && request.RetainedPrefix != nil {
		return fmt.Errorf("unexpected retained prefix in recovery request")
	}
	switch request.RecoveryProtocol {
	case 0:
		if len(request.PrefixCandidates) != 0 {
			return fmt.Errorf("unexpected indexed proofs in legacy recovery request")
		}
		return nil
	case IndexedRecoveryProtocolVersion:
		if source.RecoveryProtocol < IndexedRecoveryProtocolVersion || receiver.RecoveryProtocol < IndexedRecoveryProtocolVersion {
			return fmt.Errorf("indexed recovery capability is not mutual")
		}
		if containsString(meta.ISR, request.BrokerID) {
			return fmt.Errorf("indexed recovery receiver must be outside ISR")
		}
		if len(request.PrefixCandidates) == 0 || len(request.PrefixCandidates) > 64 {
			return fmt.Errorf("invalid indexed recovery candidate count")
		}
		exact := false
		start := request.PrefixCandidates[0].StartOffset
		for _, prefix := range request.PrefixCandidates {
			if prefix.StartOffset != start || prefix.EndOffset < prefix.StartOffset || prefix.EndOffset > request.NextOffset || len(prefix.Digest) != 64 {
				return fmt.Errorf("invalid indexed recovery prefix")
			}
			if prefix.EndOffset == request.NextOffset {
				exact = true
			}
		}
		if !exact {
			return fmt.Errorf("indexed recovery exact-tail proof is missing")
		}
		return nil
	case BootstrapRecoveryProtocolVersion:
		if source.RecoveryProtocol < BootstrapRecoveryProtocolVersion || receiver.RecoveryProtocol < BootstrapRecoveryProtocolVersion {
			return fmt.Errorf("retained recovery capability is not mutual")
		}
		if containsString(meta.ISR, request.BrokerID) {
			return fmt.Errorf("retained recovery receiver must be outside ISR")
		}
		if len(request.PrefixCandidates) != 0 || request.PreviousRecordDigest != "" || request.PreviousLeaderEpoch != 0 || !validRetainedPrefix(request.RetainedPrefix) || request.RetainedPrefix.End != request.NextOffset {
			return fmt.Errorf("invalid retained recovery challenge")
		}
		return nil
	default:
		return fmt.Errorf("%w: recovery protocol %d", ErrUnsupportedRecoveryProtocol, request.RecoveryProtocol)
	}
}

func (f *BrokerFSM) replicaRecoveryPartition(name string, id int) (*topic.Partition, error) {
	f.mu.RLock()
	tm := f.tm
	f.mu.RUnlock()
	if tm == nil {
		return nil, fmt.Errorf("topic manager is unavailable")
	}
	local := tm.GetTopic(name)
	if local == nil {
		return nil, fmt.Errorf("topic %s is unavailable", name)
	}
	return local.GetPartition(id)
}

// ValidateReplicaRecoveryMode checks additive fields before any local mutation.
func ValidateReplicaRecoveryMode(batch ReplicaCatchupBatch) error {
	if batch.RecoveryProtocol != BootstrapRecoveryProtocolVersion && (batch.RetainedPrefix != nil || batch.MatchedRetainedPrefix != nil) {
		return fmt.Errorf("unexpected retained fields in recovery response")
	}
	switch batch.RecoveryProtocol {
	case 0:
		if batch.MatchedPrefix != nil || batch.BootstrapRequired {
			return fmt.Errorf("unexpected indexed fields in legacy recovery response")
		}
	case IndexedRecoveryProtocolVersion:
		if batch.Compacted {
			return fmt.Errorf("indexed recovery does not support compacted batches")
		}
		if batch.BootstrapRequired {
			if batch.MatchedPrefix != nil || batch.TruncateTo != nil || len(batch.Messages) != 0 || batch.Verified || batch.EndOffset != batch.StartOffset {
				return fmt.Errorf("invalid bootstrap-required response")
			}
			return ErrReplicaBootstrapRequired
		}
		prefix := batch.MatchedPrefix
		if prefix == nil || prefix.StartOffset > prefix.EndOffset || prefix.EndOffset > batch.CommittedHWM || len(prefix.Digest) != 64 {
			return fmt.Errorf("invalid matched recovery prefix")
		}
		if batch.TruncateTo != nil {
			if prefix.EndOffset != *batch.TruncateTo {
				return fmt.Errorf("rollback is not bound to matched prefix")
			}
		} else if prefix.EndOffset != batch.StartOffset {
			return fmt.Errorf("catch-up is not bound to exact prefix")
		}
		endOffset := batch.EndOffset
		if endOffset == 0 && len(batch.Messages) > 0 {
			endOffset = batch.Messages[len(batch.Messages)-1].Offset + 1
		}
		if batch.TruncateTo == nil && endOffset == batch.CommittedHWM && !batch.Verified {
			return fmt.Errorf("replica catch-up completed without prefix verification")
		}
	case BootstrapRecoveryProtocolVersion:
		if batch.MatchedPrefix != nil || batch.TruncateTo != nil || !validRetainedPrefix(batch.RetainedPrefix) || batch.RetainedPrefix.End != batch.StartOffset {
			return fmt.Errorf("invalid retained recovery response")
		}
		if batch.BootstrapRequired {
			if batch.MatchedRetainedPrefix != nil || len(batch.Messages) != 0 || batch.Verified || batch.Compacted || batch.EndOffset != batch.StartOffset {
				return fmt.Errorf("invalid retained bootstrap-required response")
			}
			return ErrReplicaBootstrapRequired
		}
		if !validRetainedPrefix(batch.MatchedRetainedPrefix) || *batch.MatchedRetainedPrefix != *batch.RetainedPrefix || batch.StartOffset > batch.CommittedHWM {
			return fmt.Errorf("retained recovery match differs from challenge")
		}
		if batch.Verified != (batch.EndOffset == batch.CommittedHWM) {
			return fmt.Errorf("retained recovery verification does not match committed boundary")
		}
	default:
		return fmt.Errorf("%w: recovery protocol %d", ErrUnsupportedRecoveryProtocol, batch.RecoveryProtocol)
	}
	return nil
}

// WithReplicaRecoveryFence prevents Raft changes between validation and recovery
// mutation. Release f.mu before callbacks: rebuilding derived state can read FSM.
// Acquire controller lifecycle/write gates first, then this fence, then partition
// reconciliation locks. Existing controller writes can await Raft while gated.
func (f *BrokerFSM) WithReplicaRecoveryFence(batch ReplicaCatchupBatch, apply func() error) error {
	if err := ValidateReplicaRecoveryMode(batch); err != nil {
		return err
	}
	f.transitionMu.Lock()
	defer f.transitionMu.Unlock()
	f.materializationMu.Lock()
	defer f.materializationMu.Unlock()
	f.mu.RLock()
	meta := f.partitionMetadata[fmt.Sprintf("%s-%d", batch.Topic, batch.Partition)]
	source := f.brokers[batch.SourceBroker]
	receiver := f.brokers[batch.BrokerID]
	valid := meta != nil && meta.CommittedHWMKnown && meta.Leader == batch.Leader && meta.LeaderEpoch == batch.LeaderEpoch && meta.LifecycleEpoch == batch.LifecycleEpoch && meta.CommittedHWM >= batch.CommittedHWM && containsString(meta.Replicas, batch.BrokerID) && containsString(meta.Replicas, batch.SourceBroker) && containsString(meta.ISR, batch.SourceBroker) && source != nil && source.Status == "active" && receiver != nil && receiver.Status == "active"
	if valid {
		valid = !containsString(meta.ISR, batch.BrokerID)
	}
	if valid && batch.RecoveryProtocol == IndexedRecoveryProtocolVersion {
		valid = !containsString(meta.ISR, batch.BrokerID) && source.RecoveryProtocol >= IndexedRecoveryProtocolVersion && receiver.RecoveryProtocol >= IndexedRecoveryProtocolVersion
	}
	if valid && batch.RecoveryProtocol == BootstrapRecoveryProtocolVersion {
		valid = source.RecoveryProtocol >= BootstrapRecoveryProtocolVersion && receiver.RecoveryProtocol >= BootstrapRecoveryProtocolVersion
	}
	f.mu.RUnlock()
	if !valid {
		return fmt.Errorf("replica recovery fence changed")
	}
	return apply()
}
