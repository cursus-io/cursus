package fsm

import (
	"fmt"
	"reflect"

	"github.com/cursus-io/cursus/pkg/config"
)

// CheckConsumerCleanupAuthority is an in-memory, nonblocking final check. It
// may run from materialization/Apply and must never acquire transition locks or
// perform I/O. A detached prefix remains evidence only in the same topology,
// topic incarnation and source/requester processes; ordinary commits may advance.
func (f *BrokerFSM) CheckConsumerCleanupAuthority(instance string, expected []PartitionMetadata, processes []BootstrapProcessIdentity) error {
	if !f.mu.TryRLock() {
		return ErrConsumerMetadataFencePending
	}
	defer f.mu.RUnlock()
	d := f.topicState[config.ConsumerOffsetsTopicName]
	if f.partitionRecoveryPending || f.recoveryReplayPending || d == nil || d.InstanceID != instance || d.Partitions != len(expected) {
		return ErrConsumerMetadataFencePending
	}
	if _, pending := f.topicMaterialization[config.ConsumerOffsetsTopicName]; pending {
		return ErrConsumerMetadataFencePending
	}
	for i, before := range expected {
		p := f.partitionMetadata[fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, i)]
		if p == nil || !p.CommittedHWMKnown || p.CommittedHWM < before.CommittedHWM || p.LifecycleEpoch != d.LifecycleEpoch {
			return ErrConsumerMetadataFencePending
		}
		current := *p
		current.CommittedHWM = before.CommittedHWM
		if !reflect.DeepEqual(before, current) {
			return ErrConsumerMetadataFencePending
		}
	}
	for _, process := range processes {
		b := f.brokers[process.BrokerID]
		if !process.valid() || b == nil || b.Status != "active" || b.IncarnationID != process.IncarnationID || b.IncarnationEpoch != process.IncarnationEpoch || b.RecoveryProtocol < BootstrapRecoveryProtocolVersion {
			return ErrConsumerMetadataFencePending
		}
	}
	return nil
}
