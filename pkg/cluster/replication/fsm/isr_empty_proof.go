package fsm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/topic"
)

type emptyISRProofCandidate struct {
	key        string
	topic      string
	partition  int
	metadata   PartitionMetadata
	definition topic.Definition
	broker     BrokerInfo
	local      *topic.Partition
	generation disk.ContentGeneration
	anchor     uint64
	compacted  bool
}

// BuildEmptyISRRecoveryProofsContext builds complete local prefix proofs
// without holding the FSM transition gates across disk scans.
func (f *BrokerFSM) BuildEmptyISRRecoveryProofsContext(ctx context.Context, brokerID string) ([]ISRCatchupProof, error) {
	if ctx == nil || brokerID == "" {
		return nil, fmt.Errorf("invalid empty ISR proof builder arguments")
	}
	candidates, err := f.emptyISRProofCandidates(ctx, brokerID)
	proofs := make([]ISRCatchupProof, 0, len(candidates))
	firstErr := err
	for _, candidate := range candidates {
		proof, err := f.buildEmptyISRRecoveryProof(ctx, candidate)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("build empty ISR proof for %s: %w", candidate.key, err)
			}
			continue
		}
		proofs = append(proofs, proof)
	}
	sort.Slice(proofs, func(i, j int) bool {
		if proofs[i].Topic != proofs[j].Topic {
			return proofs[i].Topic < proofs[j].Topic
		}
		return proofs[i].Partition < proofs[j].Partition
	})
	return proofs, firstErr
}

func (f *BrokerFSM) emptyISRProofCandidates(ctx context.Context, brokerID string) ([]emptyISRProofCandidate, error) {
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return nil, err
	}
	defer unlock()

	f.mu.RLock()
	broker := f.brokers[brokerID]
	topicManager := f.tm
	if broker == nil || broker.Status != "active" || broker.RecoveryProtocol < EmptyISRRecoveryProtocolVersion ||
		broker.IncarnationID == "" || broker.IncarnationEpoch == 0 || topicManager == nil {
		f.mu.RUnlock()
		return nil, nil
	}
	brokerCopy := *broker
	type input struct {
		key        string
		metadata   PartitionMetadata
		definition *topic.Definition
	}
	inputs := make([]input, 0)
	for key, metadata := range f.partitionMetadata {
		if metadata == nil || len(metadata.ISR) != 0 || !metadata.CommittedHWMKnown || !containsString(metadata.Replicas, brokerID) {
			continue
		}
		copy := *metadata
		copy.Replicas = append([]string(nil), metadata.Replicas...)
		copy.ISR = append([]string(nil), metadata.ISR...)
		copy.RecoveryReplicas = append([]string(nil), metadata.RecoveryReplicas...)
		separator := strings.LastIndex(key, "-")
		if separator <= 0 {
			continue
		}
		inputs = append(inputs, input{key: key, metadata: copy, definition: copyTopicDefinition(f.topicState[key[:separator]])})
	}
	f.mu.RUnlock()

	candidates := make([]emptyISRProofCandidate, 0, len(inputs))
	var firstErr error
	for _, input := range inputs {
		separator := strings.LastIndex(input.key, "-")
		partitionID, err := strconv.Atoi(input.key[separator+1:])
		if err != nil || input.definition == nil || input.definition.LifecycleEpoch != input.metadata.LifecycleEpoch {
			continue
		}
		topicName := input.key[:separator]
		localTopic := topicManager.GetTopic(topicName)
		if localTopic == nil || !reflect.DeepEqual(localTopic.Definition(), *input.definition) {
			continue
		}
		partition, err := localTopic.GetPartition(partitionID)
		if err != nil || partition.BootstrapPending() || partition.SnapshotRecoveryPending() || partition.RecoveryError() != nil {
			continue
		}
		if partition.NextOffset() != input.metadata.CommittedHWM || partition.GetHWM() != input.metadata.CommittedHWM {
			continue
		}
		generation, err := partition.CaptureReplicaContentGeneration(ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("capture content generation for %s: %w", input.key, err)
			}
			continue
		}
		anchor, err := partition.ReplicaFirstOffsetContext(ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("read first offset for %s: %w", input.key, err)
			}
			continue
		}
		if anchor > input.metadata.CommittedHWM {
			continue
		}
		candidates = append(candidates, emptyISRProofCandidate{
			key: input.key, topic: topicName, partition: partitionID, metadata: input.metadata,
			definition: *input.definition, broker: brokerCopy, local: partition, generation: generation, anchor: anchor,
			compacted: config.HasCleanupPolicy(input.definition.Policy.CleanupPolicy, config.CleanupPolicyCompact),
		})
	}
	return candidates, firstErr
}

func (f *BrokerFSM) buildEmptyISRRecoveryProof(ctx context.Context, candidate emptyISRProofCandidate) (ISRCatchupProof, error) {
	proof := ISRCatchupProof{
		Topic: candidate.topic, Partition: candidate.partition, BrokerID: candidate.broker.ID,
		CommittedHWM: candidate.metadata.CommittedHWM, LocalLEO: candidate.metadata.CommittedHWM, LocalHWM: candidate.metadata.CommittedHWM,
		LeaderEpoch: candidate.metadata.LeaderEpoch, LifecycleEpoch: candidate.metadata.LifecycleEpoch,
		IncarnationID: candidate.broker.IncarnationID, IncarnationEpoch: candidate.broker.IncarnationEpoch,
	}
	if !candidate.compacted {
		lineage, err := candidate.local.ReplicaLineagePrefixContext(ctx, candidate.metadata.CommittedHWM)
		if err == nil {
			proof.RecoveryProtocol = IndexedRecoveryProtocolVersion
			proof.LineagePrefix = &lineage
			return proof, f.revalidateEmptyISRProofCandidate(ctx, candidate)
		}
		if !errors.Is(err, disk.ErrLineageUnsupported) {
			return ISRCatchupProof{}, err
		}
	}
	retained, err := candidate.local.ReplicaRetainedPrefixContext(ctx, candidate.anchor, candidate.metadata.CommittedHWM)
	if err != nil {
		return ISRCatchupProof{}, err
	}
	if retained.ContentGeneration != candidate.generation {
		return ISRCatchupProof{}, disk.ErrStaleContentGeneration
	}
	if !candidate.compacted && !retained.UsedMarkedAuthority {
		return ISRCatchupProof{}, fmt.Errorf("unsupported lineage lacks marked retained authority")
	}
	proof.RecoveryProtocol = BootstrapRecoveryProtocolVersion
	proof.RetainedPrefix = &retained.Proof
	return proof, f.revalidateEmptyISRProofCandidate(ctx, candidate)
}

func (f *BrokerFSM) revalidateEmptyISRProofCandidate(ctx context.Context, candidate emptyISRProofCandidate) error {
	unlock, err := lockSourceContext(ctx, &f.transitionMu, &f.materializationMu)
	if err != nil {
		return err
	}
	defer unlock()
	f.mu.RLock()
	metadata := f.partitionMetadata[candidate.key]
	broker := f.brokers[candidate.broker.ID]
	definition := f.topicState[candidate.topic]
	topicManager := f.tm
	valid := metadata != nil && broker != nil && definition != nil && topicManager != nil &&
		sameEmptyISRProofMetadata(*metadata, candidate.metadata) && *broker == candidate.broker && reflect.DeepEqual(*definition, candidate.definition)
	f.mu.RUnlock()
	if !valid {
		return fmt.Errorf("empty ISR proof authority changed")
	}
	localTopic := topicManager.GetTopic(candidate.topic)
	if localTopic == nil || !reflect.DeepEqual(localTopic.Definition(), candidate.definition) {
		return fmt.Errorf("empty ISR local topic changed")
	}
	partition, err := localTopic.GetPartition(candidate.partition)
	if err != nil || partition != candidate.local || partition.NextOffset() != candidate.metadata.CommittedHWM || partition.GetHWM() != candidate.metadata.CommittedHWM ||
		partition.BootstrapPending() || partition.SnapshotRecoveryPending() || partition.RecoveryError() != nil {
		return fmt.Errorf("empty ISR local partition changed")
	}
	return partition.CheckReplicaContentGeneration(ctx, candidate.generation)
}

func sameEmptyISRProofMetadata(current, captured PartitionMetadata) bool {
	return current.Leader == captured.Leader && current.LeaderEpoch == captured.LeaderEpoch &&
		current.CommittedHWM == captured.CommittedHWM && current.CommittedHWMKnown == captured.CommittedHWMKnown &&
		current.PartitionCount == captured.PartitionCount && current.Idempotent == captured.Idempotent &&
		current.LifecycleEpoch == captured.LifecycleEpoch && slices.Equal(current.Replicas, captured.Replicas) &&
		slices.Equal(current.ISR, captured.ISR) && slices.Equal(current.RecoveryReplicas, captured.RecoveryReplicas)
}
