package controller

import (
	"context"
	"errors"
	"fmt"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	replicationFSM "github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
)

// ApplyReplicaCatchup appends one leader-fenced committed logical range to a
// local replica. Compacted ranges may contain physical offset holes. It advances
// HWM only when the complete committed range is present; ISR admission remains
// the heartbeat proof's responsibility.
func (ch *CommandHandler) ApplyReplicaCatchup(ctx context.Context, batch replicationFSM.ReplicaCatchupBatch) error {
	return ch.applyReplicaCatchupRequest(ctx, nil, batch, nil)
}

// ApplyReplicaCatchupVerified binds actual local application to the immutable
// challenge that preceded the network fetch. Tuple-only callers get no authority.
func (ch *CommandHandler) ApplyReplicaCatchupVerified(ctx context.Context, request replicationFSM.ReplicaCatchupRequest, batch replicationFSM.ReplicaCatchupBatch) (*topic.ReplicaCatchupWitness, error) {
	if err := clusterController.ValidateReplicaCatchupBatch(request, batch); err != nil {
		return nil, err
	}
	if _, needed := replicationFSM.ReplicaPrefixRefinement(request, batch); needed {
		return nil, fmt.Errorf("indexed rollback requires exact common-prefix refinement")
	}
	var witness *topic.ReplicaCatchupWitness
	err := ch.applyReplicaCatchupRequest(ctx, &request, batch, &witness)
	return witness, err
}

func (ch *CommandHandler) applyReplicaCatchupRequest(ctx context.Context, request *replicationFSM.ReplicaCatchupRequest, batch replicationFSM.ReplicaCatchupBatch, witness **topic.ReplicaCatchupWitness) error {
	if err := replicationFSM.ValidateReplicaCatchupBatchDigest(batch); err != nil {
		return err
	}
	if err := replicationFSM.ValidateReplicaRecoveryMode(batch); err != nil {
		return err
	}
	if batch.RecoveryProtocol == replicationFSM.BootstrapRecoveryProtocolVersion && (request == nil || request.LocalWitness == nil) {
		return fmt.Errorf("retained recovery requires the original receiver witness")
	}
	if ch == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.TopicManager == nil {
		return fmt.Errorf("replica catch-up dependencies are unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := injectedReplicaResponseDelay(ctx, batch.Topic); err != nil {
		return err
	}
	if batch.TruncateTo != nil && batch.Topic == config.ConsumerOffsetsTopicName {
		if ch.Coordinator == nil {
			return fmt.Errorf("consumer log repair coordinator unavailable")
		}
		release, err := ch.Coordinator.BeginConsumerMetadataRecovery(ctx)
		if err != nil {
			return err
		}
		defer release()
		if !ch.groupRecoveryReloadMu.TryLock() {
			return ErrConsumerRecoveryPending
		}
		defer ch.groupRecoveryReloadMu.Unlock()
	}
	releaseLifecycle, err := ch.topicLifecycleGates.acquire(ctx, batch.Topic, false)
	if err != nil {
		return err
	}
	defer releaseLifecycle()
	writeLock := ch.partitionWriteLock(batch.Topic, batch.Partition)
	if err := writeLock.LockContext(ctx); err != nil {
		return err
	}
	defer writeLock.Unlock()
	if batch.RecoveryProtocol == replicationFSM.BootstrapRecoveryProtocolVersion {
		// The original witness is revalidated again under the FSM fence below.
		// Keep the potentially cold retained-prefix scan outside global gates.
		local := ch.TopicManager.GetTopic(batch.Topic)
		if local == nil {
			return fmt.Errorf("retained recovery topic unavailable")
		}
		partition, err := local.GetPartition(batch.Partition)
		if err != nil {
			return err
		}
		if err := partition.ValidateReplicaRequestWitness(ctx, request.LocalWitness, batch.StartOffset); err != nil {
			return err
		}
		prefix := batch.MatchedRetainedPrefix
		actual, err := partition.ReplicaRetainedPrefixContext(ctx, prefix.Start, prefix.End)
		if err != nil {
			return err
		}
		if actual.Proof != *prefix {
			return fmt.Errorf("local retained prefix changed before recovery")
		}
	}
	if request != nil || batch.RecoveryProtocol != 0 || batch.TruncateTo != nil {
		if ch.Cluster.RaftManager.GetFSM() == nil {
			return fmt.Errorf("replica catch-up metadata is unavailable")
		}
		return ch.Cluster.RaftManager.GetFSM().WithReplicaRecoveryFence(batch, func() error { return ch.applyReplicaCatchup(ctx, request, batch, witness) })
	}
	return ch.applyReplicaCatchup(ctx, request, batch, witness)
}

func (ch *CommandHandler) applyReplicaCatchup(ctx context.Context, request *replicationFSM.ReplicaCatchupRequest, batch replicationFSM.ReplicaCatchupBatch, witness **topic.ReplicaCatchupWitness) error {
	if ch.Cluster.Router == nil || batch.BrokerID != ch.Cluster.Router.BrokerID() {
		return fmt.Errorf("replica catch-up broker identity mismatch")
	}
	if err := injectedReplicaCatchupError(batch.Topic); err != nil {
		return err
	}
	fsmRef := ch.Cluster.RaftManager.GetFSM()
	if fsmRef == nil {
		return fmt.Errorf("FSM is unavailable")
	}
	metadata := fsmRef.GetPartitionMetadata(fmt.Sprintf("%s-%d", batch.Topic, batch.Partition))
	if metadata == nil || !metadata.CommittedHWMKnown {
		return fmt.Errorf("authoritative partition metadata is unavailable")
	}
	if metadata.Leader != batch.Leader || metadata.LeaderEpoch != batch.LeaderEpoch {
		return fmt.Errorf("%w: stale replica catch-up leader fence", clusterController.ErrPartitionLeaderFenced)
	}
	if metadata.LifecycleEpoch != batch.LifecycleEpoch {
		return fmt.Errorf("%w: stale replica catch-up lifecycle fence", clusterController.ErrPartitionLeaderFenced)
	}
	if metadata.CommittedHWM < batch.CommittedHWM {
		return fmt.Errorf("replica catch-up HWM regressed: current=%d response=%d", metadata.CommittedHWM, batch.CommittedHWM)
	}
	if !containsReplica(metadata.Replicas, batch.BrokerID) {
		return fmt.Errorf("broker %s is not a configured replica", batch.BrokerID)
	}
	if !containsReplica(metadata.Replicas, batch.SourceBroker) || !containsReplica(metadata.ISR, batch.SourceBroker) {
		return fmt.Errorf("source broker %s is not an in-sync replica", batch.SourceBroker)
	}
	localTopic := ch.TopicManager.GetTopic(batch.Topic)
	if localTopic == nil || localTopic.LifecycleEpoch != batch.LifecycleEpoch {
		return fmt.Errorf("local topic lifecycle does not match catch-up response")
	}
	partition, err := localTopic.GetPartition(batch.Partition)
	if err != nil {
		return err
	}
	if batch.TruncateTo == nil {
		releaseMutation := partition.BeginReplicationMutation()
		defer releaseMutation()
	}
	if request != nil {
		if err := partition.ValidateReplicaRequestWitness(ctx, request.LocalWitness, batch.StartOffset); err != nil {
			return err
		}
		if err := replicationFSM.ValidateReplicaRequestChallenge(partition, *request); err != nil {
			return err
		}
	}
	repairGeneration := uint64(0)
	if request != nil && batch.Topic == config.ConsumerOffsetsTopicName {
		if ch.Coordinator == nil {
			return fmt.Errorf("consumer repair coordinator unavailable")
		}
		repairGeneration = ch.Coordinator.PendingConsumerLogRepair(batch.Partition)
	}
	if partition.NextOffset() != batch.StartOffset {
		return fmt.Errorf("replica catch-up local LEO changed: current=%d response_start=%d", partition.NextOffset(), batch.StartOffset)
	}
	if batch.RecoveryProtocol == replicationFSM.IndexedRecoveryProtocolVersion && batch.TruncateTo == nil {
		matched, err := partition.VerifyReplicaLineage(*batch.MatchedPrefix, batch.CommittedHWM)
		if err != nil {
			return err
		}
		if !matched {
			return fmt.Errorf("local retained prefix changed before indexed recovery")
		}
	}
	if batch.TruncateTo != nil {
		if len(batch.Messages) != 0 || batch.Verified || *batch.TruncateTo >= batch.StartOffset || batch.EndOffset != batch.StartOffset {
			return fmt.Errorf("invalid replica catch-up truncation boundary")
		}
		var beforeMutation func() error
		if request != nil {
			beforeMutation = func() error { return partition.CheckReplicaRequestGeneration(ctx, request.LocalWitness) }
		}
		var generation uint64
		if batch.Topic == config.ConsumerOffsetsTopicName {
			if err := ch.Coordinator.ValidateConsumerMetadataBootstrapEligibility(batch.Partition); err != nil {
				return err
			}
			beforeMutation = func() error {
				if request != nil {
					if err := partition.CheckReplicaRequestGeneration(ctx, request.LocalWitness); err != nil {
						return err
					}
				}
				var err error
				generation, err = ch.Coordinator.BeginConsumerLogRepair(batch.Partition)
				return err
			}
		}
		err := partition.ApplyReplicaRollbackChecked(ctx, *batch.TruncateTo, batch.StartOffset, batch.MatchedPrefix, batch.CommittedHWM, beforeMutation)
		if generation != 0 {
			// A checked pre-mutation rejection restores partition availability.
			// Destructive/rebuild failures retain their own recovery error.
			var failure error
			if errors.Is(err, topic.ErrReplicaRollbackFailed) {
				failure = err
			}
			ch.Coordinator.FinishConsumerLogRepair(generation, failure)
		}
		return err
	}
	finish := func() error {
		if localTopic.IsEventSourcing {
			if ch.ESHandler == nil {
				return fmt.Errorf("event sourcing index unavailable")
			}
			if request == nil {
				if err := ch.ESHandler.PrepareCommittedIndex(batch.Topic, batch.Partition); err != nil {
					return err
				}
			}
		}
		var err error
		if request != nil {
			err = partition.ApplyReplicaHWMChecked(ctx, request.LocalWitness, batch.CommittedHWM)
		} else {
			err = partition.ApplyReplicaHWM(batch.CommittedHWM)
		}
		if err != nil {
			return err
		}
		partition.FlushDisk()
		if localTopic.IsEventSourcing {
			if request != nil {
				err := partition.WithReplicaCatchupIndexView(ctx, request.LocalWitness, batch.CommittedHWM, func(view *topic.PartitionReadView) error {
					return ch.ESHandler.IndexReplicaCatchupWithView(batch.Topic, batch.Partition, batch.CommittedHWM, view)
				})
				if err != nil {
					return err
				}
			} else if err := ch.ESHandler.IndexCommittedToHWM(batch.Topic, batch.Partition, batch.CommittedHWM); err != nil {
				return err
			}
		}
		if request != nil {
			*witness, err = partition.CompleteReplicaCatchupVerification(ctx, request.LocalWitness, batch.Leader, batch.LeaderEpoch, batch.LifecycleEpoch, batch.CommittedHWM, repairGeneration)
		}
		return err
	}
	if batch.Verified && batch.StartOffset == batch.CommittedHWM {
		if len(batch.Messages) != 0 || batch.EndOffset != batch.StartOffset {
			return fmt.Errorf("invalid replica catch-up verification batch")
		}
		return finish()
	}
	if len(batch.Messages) > replicationFSM.MaxReplicaCatchupRecords || (!batch.Compacted && len(batch.Messages) == 0) {
		return fmt.Errorf("invalid replica catch-up batch size %d", len(batch.Messages))
	}
	endOffset := batch.EndOffset
	if endOffset == 0 && len(batch.Messages) > 0 {
		endOffset = batch.Messages[len(batch.Messages)-1].Offset + 1
	}
	if endOffset <= batch.StartOffset || endOffset > batch.CommittedHWM {
		return fmt.Errorf("invalid replica catch-up range [%d,%d)", batch.StartOffset, endOffset)
	}
	next := batch.StartOffset
	for i := range batch.Messages {
		message := &batch.Messages[i]
		if message.Offset >= endOffset || (!batch.Compacted && message.Offset != next) || (batch.Compacted && message.Offset < next) {
			return fmt.Errorf("invalid replica catch-up offset: expected=%d got=%d hwm=%d", next, message.Offset, batch.CommittedHWM)
		}
		// This authenticated ISR transfer copies an already committed log
		// range, including unresolved/aborted records and old control markers.
		// Rechecking live producer or transaction state would reject history
		// after commit, reinitialization, expiry, or a coordinator change.
		// Source, lifecycle, leader, digest, and committed HWM fences above
		// authorize the copy; live replication retains admission validation.
		next = message.Offset + 1
	}
	if batch.Compacted {
		if batch.RecoveryProtocol != replicationFSM.BootstrapRecoveryProtocolVersion && !config.HasCleanupPolicy(localTopic.PolicySnapshot().CleanupPolicy, config.CleanupPolicyCompact) {
			return fmt.Errorf("compacted replica catch-up is not allowed for topic policy")
		}
		if err := partition.ReplicaAppendCompactedRange(batch.Messages, endOffset); err != nil {
			return err
		}
	} else if err := partition.ReplicaAppendWithMode(batch.Messages, true); err != nil {
		return err
	}
	injectedReplicaCatchupCrash(batch.Topic, batch.Partition)
	if endOffset != batch.CommittedHWM {
		return nil
	}
	if !batch.Verified {
		return fmt.Errorf("replica catch-up completed without prefix verification")
	}
	return finish()
}

func containsReplica(replicas []string, brokerID string) bool {
	for _, replica := range replicas {
		if replica == brokerID {
			return true
		}
	}
	return false
}
