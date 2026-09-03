package controller

import (
	"fmt"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	replicationFSM "github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
)

// ApplyReplicaCatchup appends one leader-fenced committed logical range to a
// local replica. Compacted ranges may contain physical offset holes. It advances
// HWM only when the complete committed range is present; ISR admission remains
// the heartbeat proof's responsibility.
func (ch *CommandHandler) ApplyReplicaCatchup(batch replicationFSM.ReplicaCatchupBatch) error {
	if ch == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.TopicManager == nil {
		return fmt.Errorf("replica catch-up dependencies are unavailable")
	}
	if ch.Cluster.Router == nil || batch.BrokerID != ch.Cluster.Router.BrokerID() {
		return fmt.Errorf("replica catch-up broker identity mismatch")
	}
	if len(batch.Messages) > replicationFSM.MaxReplicaCatchupRecords || (!batch.Compacted && len(batch.Messages) == 0) {
		return fmt.Errorf("invalid replica catch-up batch size %d", len(batch.Messages))
	}

	ch.topicLifecycleMu.RLock()
	defer ch.topicLifecycleMu.RUnlock()
	writeLock := ch.partitionWriteLock(batch.Topic, batch.Partition)
	writeLock.Lock()
	defer writeLock.Unlock()

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
	if metadata.CommittedHWM != batch.CommittedHWM {
		return fmt.Errorf("replica catch-up HWM changed: current=%d response=%d", metadata.CommittedHWM, batch.CommittedHWM)
	}
	if !containsReplica(metadata.Replicas, batch.BrokerID) {
		return fmt.Errorf("broker %s is not a configured replica", batch.BrokerID)
	}
	localTopic := ch.TopicManager.GetTopic(batch.Topic)
	if localTopic == nil || localTopic.LifecycleEpoch != batch.LifecycleEpoch {
		return fmt.Errorf("local topic lifecycle does not match catch-up response")
	}
	partition, err := localTopic.GetPartition(batch.Partition)
	if err != nil {
		return err
	}
	if partition.NextOffset() != batch.StartOffset {
		return fmt.Errorf("replica catch-up local LEO changed: current=%d response_start=%d", partition.NextOffset(), batch.StartOffset)
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
		if errResp := ch.validateReplicatedTransactionMessage(batch.Topic, batch.Partition, message); errResp != "" {
			return fmt.Errorf("replica catch-up transaction validation failed: %s", errResp)
		}
		next = message.Offset + 1
	}
	if batch.Compacted {
		if !config.HasCleanupPolicy(localTopic.PolicySnapshot().CleanupPolicy, config.CleanupPolicyCompact) {
			return fmt.Errorf("compacted replica catch-up is not allowed for topic policy")
		}
		if err := partition.ReplicaAppendCompactedRange(batch.Messages, endOffset); err != nil {
			return err
		}
	} else if err := partition.ReplicaAppendWithMode(batch.Messages, true); err != nil {
		return err
	}
	if endOffset != batch.CommittedHWM {
		return nil
	}
	if localTopic.IsEventSourcing && ch.ESHandler != nil {
		if err := ch.ESHandler.PrepareCommittedIndex(batch.Topic, batch.Partition); err != nil {
			return err
		}
	}
	if err := partition.ApplyReplicaHWM(batch.CommittedHWM); err != nil {
		return err
	}
	partition.FlushDisk()
	if localTopic.IsEventSourcing && ch.ESHandler != nil {
		if err := ch.ESHandler.IndexCommittedToHWM(batch.Topic, batch.Partition, batch.CommittedHWM); err != nil {
			return err
		}
	}
	return nil
}

func containsReplica(replicas []string, brokerID string) bool {
	for _, replica := range replicas {
		if replica == brokerID {
			return true
		}
	}
	return false
}
