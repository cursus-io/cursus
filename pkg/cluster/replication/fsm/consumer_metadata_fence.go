package fsm

import (
	"errors"
	"fmt"

	"github.com/cursus-io/cursus/pkg/config"
)

var ErrConsumerMetadataFencePending = errors.New("consumer metadata FSM fence pending")

// ConsumerMetadataRecoverySnapshot captures every offsets partition, including
// non-owned partitions. A local HWM alone cannot authorize consumer recovery.
func (f *BrokerFSM) ConsumerMetadataRecoverySnapshot() ([]PartitionMetadata, error) {
	return f.consumerMetadataRecoverySnapshot(false)
}

// ConsumerMetadataBootstrapSnapshot is preflight only; the lifecycle and full
// vector must be revalidated under WithConsumerMetadataBootstrapFence.
func (f *BrokerFSM) ConsumerMetadataBootstrapSnapshot() ([]PartitionMetadata, error) {
	return f.consumerMetadataRecoverySnapshot(true)
}

func (f *BrokerFSM) consumerMetadataRecoverySnapshot(bootstrap bool) ([]PartitionMetadata, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !bootstrap && (f.partitionRecoveryPending || f.recoveryReplayPending) {
		return nil, fmt.Errorf("consumer metadata FSM replay pending")
	}
	definition := f.topicState[config.ConsumerOffsetsTopicName]
	if definition == nil || definition.Partitions <= 0 {
		return nil, fmt.Errorf("consumer metadata topology pending")
	}
	if _, pending := f.topicMaterialization[config.ConsumerOffsetsTopicName]; pending && !bootstrap {
		return nil, fmt.Errorf("consumer metadata materialization pending")
	}
	metadata := make([]PartitionMetadata, definition.Partitions)
	for i := range metadata {
		key := fmt.Sprintf("%s-%d", config.ConsumerOffsetsTopicName, i)
		p := f.partitionMetadata[key]
		if p == nil || !p.CommittedHWMKnown || p.LifecycleEpoch != definition.LifecycleEpoch {
			return nil, fmt.Errorf("consumer metadata authority pending: partition=%d", i)
		}
		metadata[i] = *p
		metadata[i].Replicas = append([]string(nil), p.Replicas...)
		metadata[i].ISR = append([]string(nil), p.ISR...)
		metadata[i].RecoveryReplicas = append([]string(nil), p.RecoveryReplicas...)
	}
	return metadata, nil
}

// WithConsumerMetadataBootstrapFence permits the dedicated pending-resume path
// while ordinary recovery is waiting for disk installation/finalization. Network
// protocol callers must additionally validate their source/receiver fence here.
func (f *BrokerFSM) WithConsumerMetadataBootstrapFence(partition int, lifecycle uint64, fn func([]PartitionMetadata) error) error {
	if !f.transitionMu.TryLock() {
		return ErrConsumerMetadataFencePending
	}
	defer f.transitionMu.Unlock()
	if !f.materializationMu.TryLock() {
		return ErrConsumerMetadataFencePending
	}
	defer f.materializationMu.Unlock()
	metadata, err := f.consumerMetadataRecoverySnapshot(true)
	if err != nil {
		return err
	}
	if partition < 0 || partition >= len(metadata) || metadata[partition].LifecycleEpoch != lifecycle {
		return fmt.Errorf("consumer bootstrap lifecycle changed")
	}
	return fn(metadata)
}

// WithConsumerMetadataRecoveryFence excludes both Restore/Apply and independent
// materialization. Caller drains consumer admission BEFORE controller reload
// exclusion. Callback is local-only; f.mu is never held during callback work.
func (f *BrokerFSM) WithConsumerMetadataRecoveryFence(fn func([]PartitionMetadata) error) error {
	if !f.transitionMu.TryLock() {
		return ErrConsumerMetadataFencePending
	}
	defer f.transitionMu.Unlock()
	if !f.materializationMu.TryLock() {
		return ErrConsumerMetadataFencePending
	}
	defer f.materializationMu.Unlock()
	metadata, err := f.ConsumerMetadataRecoverySnapshot()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConsumerMetadataFencePending, err)
	}
	return fn(metadata)
}
