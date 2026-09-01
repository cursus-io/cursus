package fsm

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ISRCatchupProof fences ISR re-admission with the authoritative partition
// boundary and the current leader and topic lifecycle generations.
type ISRCatchupProof struct {
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	BrokerID       string `json:"broker_id"`
	CommittedHWM   uint64 `json:"committed_hwm"`
	LocalLEO       uint64 `json:"local_leo"`
	LocalHWM       uint64 `json:"local_hwm"`
	LeaderEpoch    int    `json:"leader_epoch"`
	LifecycleEpoch uint64 `json:"lifecycle_epoch"`
}

// ValidateISRCatchupProof validates a proof against the current FSM view. The
// boolean result reports whether a Raft transition is still required.
func (f *BrokerFSM) ValidateISRCatchupProof(proof ISRCatchupProof) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.validateISRCatchupProofLocked(proof)
}

func (f *BrokerFSM) validateISRCatchupProofLocked(proof ISRCatchupProof) (bool, error) {
	if proof.Topic == "" || proof.Partition < 0 || proof.BrokerID == "" {
		return false, fmt.Errorf("invalid ISR catch-up proof identity")
	}
	key := proof.Topic + "-" + strconv.Itoa(proof.Partition)
	metadata := f.partitionMetadata[key]
	if metadata == nil {
		return false, fmt.Errorf("partition metadata %s not found", key)
	}
	if !containsString(metadata.Replicas, proof.BrokerID) {
		return false, fmt.Errorf("broker %s is not a configured replica for %s", proof.BrokerID, key)
	}
	if !metadata.CommittedHWMKnown {
		return false, fmt.Errorf("%w: partition %s has no authoritative committed HWM", ErrUnsupportedRecoveryProtocol, key)
	}
	if proof.CommittedHWM != metadata.CommittedHWM {
		return false, fmt.Errorf("committed HWM mismatch for %s: current=%d proof=%d", key, metadata.CommittedHWM, proof.CommittedHWM)
	}
	if proof.LocalLEO != metadata.CommittedHWM || proof.LocalHWM != metadata.CommittedHWM {
		return false, fmt.Errorf(
			"replica %s is not synchronized for %s: leo=%d hwm=%d committed_hwm=%d",
			proof.BrokerID, key, proof.LocalLEO, proof.LocalHWM, metadata.CommittedHWM,
		)
	}
	if proof.LeaderEpoch != metadata.LeaderEpoch {
		return false, fmt.Errorf("stale leader epoch for %s: current=%d proof=%d", key, metadata.LeaderEpoch, proof.LeaderEpoch)
	}
	if proof.LifecycleEpoch != metadata.LifecycleEpoch {
		return false, fmt.Errorf("stale topic lifecycle epoch for %s: current=%d proof=%d", key, metadata.LifecycleEpoch, proof.LifecycleEpoch)
	}
	definition := f.topicState[proof.Topic]
	if definition == nil {
		return false, fmt.Errorf("topic definition %s not found", proof.Topic)
	}
	if definition.LifecycleEpoch != metadata.LifecycleEpoch {
		return false, fmt.Errorf(
			"partition lifecycle epoch conflicts with topic %s: topic=%d partition=%d",
			proof.Topic, definition.LifecycleEpoch, metadata.LifecycleEpoch,
		)
	}
	if !containsString(metadata.Replicas, metadata.Leader) {
		return false, fmt.Errorf("partition leader %s is not a configured replica for %s", metadata.Leader, key)
	}
	if containsString(metadata.ISR, proof.BrokerID) {
		return false, nil
	}
	return true, nil
}

func (f *BrokerFSM) applyISRCatchupCommand(jsonData string) interface{} {
	var proof ISRCatchupProof
	if err := decodeStrictJSON([]byte(jsonData), &proof); err != nil {
		return fmt.Errorf("decode ISR catch-up proof: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	required, err := f.validateISRCatchupProofLocked(proof)
	if err != nil || !required {
		return err
	}

	key := proof.Topic + "-" + strconv.Itoa(proof.Partition)
	metadata := f.partitionMetadata[key]
	members := make(map[string]struct{}, len(metadata.ISR)+1)
	for _, brokerID := range metadata.ISR {
		if !containsString(metadata.Replicas, brokerID) {
			return fmt.Errorf("ISR broker %s is not a configured replica for %s", brokerID, key)
		}
		members[brokerID] = struct{}{}
	}
	members[proof.BrokerID] = struct{}{}

	ordered := make([]string, 0, len(members))
	for _, brokerID := range metadata.Replicas {
		if _, ok := members[brokerID]; ok {
			ordered = append(ordered, brokerID)
		}
	}
	metadata.ISR = ordered
	return nil
}

// BuildISRCatchupProofs returns proofs only for local, synchronized replicas
// that are currently outside ISR under the same topic lifecycle generation.
func (f *BrokerFSM) BuildISRCatchupProofs(brokerID string) []ISRCatchupProof {
	if brokerID == "" {
		return nil
	}

	f.mu.RLock()
	metadata := make(map[string]PartitionMetadata, len(f.partitionMetadata))
	for key, value := range f.partitionMetadata {
		if value != nil {
			copy := *value
			copy.Replicas = append([]string(nil), value.Replicas...)
			copy.ISR = append([]string(nil), value.ISR...)
			metadata[key] = copy
		}
	}
	definitions := copyTopicState(f.topicState)
	topicManager := f.tm
	f.mu.RUnlock()
	if topicManager == nil {
		return nil
	}

	proofs := make([]ISRCatchupProof, 0)
	for key, partitionMetadata := range metadata {
		if !partitionMetadata.CommittedHWMKnown ||
			!containsString(partitionMetadata.Replicas, brokerID) ||
			containsString(partitionMetadata.ISR, brokerID) {
			continue
		}
		separator := strings.LastIndex(key, "-")
		if separator <= 0 {
			continue
		}
		partitionID, err := strconv.Atoi(key[separator+1:])
		if err != nil {
			continue
		}
		topicName := key[:separator]
		definition := definitions[topicName]
		localTopic := topicManager.GetTopic(topicName)
		if definition == nil || localTopic == nil ||
			definition.LifecycleEpoch != partitionMetadata.LifecycleEpoch ||
			localTopic.Definition().LifecycleEpoch != partitionMetadata.LifecycleEpoch {
			continue
		}
		partition, err := localTopic.GetPartition(partitionID)
		if err != nil {
			continue
		}
		leo, hwm := partition.NextOffset(), partition.GetHWM()
		if leo != partitionMetadata.CommittedHWM || hwm != partitionMetadata.CommittedHWM {
			continue
		}
		proofs = append(proofs, ISRCatchupProof{
			Topic: topicName, Partition: partitionID, BrokerID: brokerID,
			CommittedHWM: partitionMetadata.CommittedHWM, LocalLEO: leo, LocalHWM: hwm,
			LeaderEpoch: partitionMetadata.LeaderEpoch, LifecycleEpoch: partitionMetadata.LifecycleEpoch,
		})
	}
	sort.Slice(proofs, func(i, j int) bool {
		if proofs[i].Topic != proofs[j].Topic {
			return proofs[i].Topic < proofs[j].Topic
		}
		return proofs[i].Partition < proofs[j].Partition
	})
	return proofs
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
