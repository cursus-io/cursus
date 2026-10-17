package controller

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
)

type consumerRecoveryOwnership struct {
	Epoch          int
	PartitionCount int
	Lifecycle      uint64
	End            uint64
}

func (ch *CommandHandler) consumerOwnershipRecovered(partition uint64, epoch int) bool {
	if ch.Cluster.RaftManager.GetFSM().ConsumerMetadataRoutingEnabled() {
		metadata := ch.Cluster.RaftManager.GetFSM().GetPartitionMetadata(fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, partition))
		if metadata == nil || metadata.Leader != ch.Cluster.Router.BrokerID() || metadata.LeaderEpoch != epoch {
			return false
		}
		ch.groupRecoveryMu.Lock()
		proof, ok := ch.groupRecoveryOwnership[partition]
		ch.groupRecoveryMu.Unlock()
		return ok && proof.Epoch == epoch && proof.PartitionCount == metadata.PartitionCount && proof.Lifecycle == metadata.LifecycleEpoch
	}
	ch.groupRecoveryMu.Lock()
	defer ch.groupRecoveryMu.Unlock()
	return ch.groupRecoveryEpoch[partition] == epoch
}
