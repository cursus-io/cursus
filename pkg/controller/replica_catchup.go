package controller

import (
	"fmt"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	replicationFSM "github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
)

// ApplyReplicaCatchup appends one leader-fenced, contiguous raw log range to a
// local replica. It advances the local HWM only when the complete committed
// range is present; ISR admission remains the heartbeat proof's responsibility.
func (ch *CommandHandler) ApplyReplicaCatchup(batch replicationFSM.ReplicaCatchupBatch) error {
	if ch == nil || ch.Cluster == nil || ch.Cluster.RaftManager == nil || ch.TopicManager == nil {
		return fmt.Errorf("replica catch-up dependencies are unavailable")
	}
	if ch.Cluster.Router == nil || batch.BrokerID != ch.Cluster.Router.BrokerID() {
		return fmt.Errorf("replica catch-up broker identity mismatch")
	}
	if len(batch.Messages) == 0 || len(batch.Messages) > replicationFSM.MaxReplicaCatchupRecords {
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
	next := batch.StartOffset
	for i := range batch.Messages {
		message := &batch.Messages[i]
		if message.Offset != next || message.Offset >= batch.CommittedHWM {
			return fmt.Errorf("invalid replica catch-up offset: expected=%d got=%d hwm=%d", next, message.Offset, batch.CommittedHWM)
		}
		if errResp := ch.validateReplicatedTransactionMessage(batch.Topic, batch.Partition, message); errResp != "" {
			return fmt.Errorf("replica catch-up transaction validation failed: %s", errResp)
		}
		next++
	}
	if err := partition.ReplicaAppendWithMode(batch.Messages, true); err != nil {
		return err
	}
	if next != batch.CommittedHWM {
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
