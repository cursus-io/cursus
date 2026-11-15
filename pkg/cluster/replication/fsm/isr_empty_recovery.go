package fsm

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
)

// ISREmptyRecoveryCommand atomically restores the first proven ISR after all
// previous members were removed. Proofs are ordered by replica assignment.
type ISREmptyRecoveryCommand struct {
	Topic               string            `json:"topic"`
	Partition           int               `json:"partition"`
	ExpectedLeader      string            `json:"expected_leader"`
	ExpectedLeaderEpoch int               `json:"expected_leader_epoch"`
	CommittedHWM        uint64            `json:"committed_hwm"`
	LifecycleEpoch      uint64            `json:"lifecycle_epoch"`
	RequiredMinISR      int               `json:"required_min_isr"`
	Proofs              []ISRCatchupProof `json:"proofs"`
}

func (f *BrokerFSM) SetDefaultMinISR(value int) {
	if value < 1 {
		value = 1
	}
	f.mu.Lock()
	f.defaultMinISR = value
	f.mu.Unlock()
}

func (f *BrokerFSM) effectiveMinISRLocked(topicName string) (int, error) {
	definition := f.topicState[topicName]
	if definition == nil {
		return 0, fmt.Errorf("topic definition %s not found", topicName)
	}
	return definition.Policy.EffectiveMinInSyncReplicas(f.defaultMinISR), nil
}

func (f *BrokerFSM) ValidateEmptyISRRecoveryProof(proof ISRCatchupProof) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.validateEmptyISRRecoveryProofLocked(proof)
}

func (f *BrokerFSM) validateEmptyISRRecoveryProofLocked(proof ISRCatchupProof) error {
	if proof.Topic == "" || proof.Partition < 0 || proof.BrokerID == "" {
		return fmt.Errorf("invalid empty ISR recovery proof identity")
	}
	key := proof.Topic + "-" + strconv.Itoa(proof.Partition)
	metadata := f.partitionMetadata[key]
	if metadata == nil {
		return fmt.Errorf("partition metadata %s not found", key)
	}
	if len(metadata.ISR) != 0 {
		return fmt.Errorf("partition %s ISR is no longer empty", key)
	}
	if !metadata.CommittedHWMKnown {
		return fmt.Errorf("%w: partition %s has no authoritative committed HWM", ErrUnsupportedRecoveryProtocol, key)
	}
	if !containsString(metadata.Replicas, proof.BrokerID) {
		return fmt.Errorf("broker %s is not a configured replica for %s", proof.BrokerID, key)
	}
	broker := f.brokers[proof.BrokerID]
	if broker == nil || broker.Status != "active" {
		return fmt.Errorf("broker %s is not active for %s", proof.BrokerID, key)
	}
	if broker.RecoveryProtocol < EmptyISRRecoveryProtocolVersion || proof.IncarnationID == "" || proof.IncarnationEpoch == 0 ||
		broker.IncarnationID != proof.IncarnationID || broker.IncarnationEpoch != proof.IncarnationEpoch {
		return fmt.Errorf("stale broker incarnation for %s on %s", proof.BrokerID, key)
	}
	if proof.CommittedHWM != metadata.CommittedHWM {
		return fmt.Errorf("committed HWM mismatch for %s: current=%d proof=%d", key, metadata.CommittedHWM, proof.CommittedHWM)
	}
	if proof.LocalLEO != metadata.CommittedHWM || proof.LocalHWM != metadata.CommittedHWM {
		return fmt.Errorf("replica %s is not synchronized for %s: leo=%d hwm=%d committed_hwm=%d", proof.BrokerID, key, proof.LocalLEO, proof.LocalHWM, metadata.CommittedHWM)
	}
	if proof.LeaderEpoch != metadata.LeaderEpoch {
		return fmt.Errorf("stale leader epoch for %s: current=%d proof=%d", key, metadata.LeaderEpoch, proof.LeaderEpoch)
	}
	if proof.LifecycleEpoch != metadata.LifecycleEpoch {
		return fmt.Errorf("stale topic lifecycle epoch for %s: current=%d proof=%d", key, metadata.LifecycleEpoch, proof.LifecycleEpoch)
	}
	definition := f.topicState[proof.Topic]
	if definition == nil || definition.LifecycleEpoch != metadata.LifecycleEpoch {
		return fmt.Errorf("partition lifecycle epoch conflicts with topic %s", proof.Topic)
	}
	return validateEmptyISRPrefix(proof, metadata.CommittedHWM)
}

func validateEmptyISRPrefix(proof ISRCatchupProof, hwm uint64) error {
	if (proof.LineagePrefix == nil) == (proof.RetainedPrefix == nil) {
		return fmt.Errorf("empty ISR proof must contain exactly one prefix")
	}
	if proof.LineagePrefix != nil {
		prefix := proof.LineagePrefix
		if proof.RecoveryProtocol != IndexedRecoveryProtocolVersion || prefix.StartOffset > prefix.EndOffset || prefix.EndOffset != hwm {
			return fmt.Errorf("invalid lineage prefix boundary")
		}
		return validateRecoveryDigest(prefix.Digest)
	}
	prefix := proof.RetainedPrefix
	if proof.RecoveryProtocol != BootstrapRecoveryProtocolVersion || prefix.Version == 0 || prefix.Start > prefix.End || prefix.End != hwm {
		return fmt.Errorf("invalid retained prefix boundary")
	}
	return validateRecoveryDigest(prefix.Digest)
}

func validateRecoveryDigest(value string) error {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return fmt.Errorf("invalid recovery prefix digest")
	}
	return nil
}

func emptyISRProofIdentity(proof ISRCatchupProof) string {
	if proof.LineagePrefix != nil {
		p := proof.LineagePrefix
		return fmt.Sprintf("lineage:%d:%d:%s", p.StartOffset, p.EndOffset, p.Digest)
	}
	p := proof.RetainedPrefix
	return fmt.Sprintf("retained:%d:%d:%d:%s", p.Version, p.Start, p.End, p.Digest)
}

// BuildISREmptyRecoveryCommand selects one unambiguous matching quorum and
// orders it by the authoritative replica assignment.
func (f *BrokerFSM) BuildISREmptyRecoveryCommand(proofs []ISRCatchupProof) (ISREmptyRecoveryCommand, bool, error) {
	if len(proofs) == 0 {
		return ISREmptyRecoveryCommand{}, false, nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	first := proofs[0]
	key := first.Topic + "-" + strconv.Itoa(first.Partition)
	metadata := f.partitionMetadata[key]
	if metadata == nil {
		return ISREmptyRecoveryCommand{}, false, fmt.Errorf("partition metadata %s not found", key)
	}
	minimum, err := f.effectiveMinISRLocked(first.Topic)
	if err != nil {
		return ISREmptyRecoveryCommand{}, false, err
	}
	groups := make(map[string]map[string]ISRCatchupProof)
	for _, proof := range proofs {
		if proof.Topic != first.Topic || proof.Partition != first.Partition {
			continue
		}
		if err := f.validateEmptyISRRecoveryProofLocked(proof); err != nil {
			continue
		}
		identity := emptyISRProofIdentity(proof)
		if groups[identity] == nil {
			groups[identity] = make(map[string]ISRCatchupProof)
		}
		groups[identity][proof.BrokerID] = proof
	}
	var quorum map[string]ISRCatchupProof
	for _, group := range groups {
		if len(group) < minimum {
			continue
		}
		if quorum != nil {
			return ISREmptyRecoveryCommand{}, false, fmt.Errorf("conflicting empty ISR proof quorums for %s", key)
		}
		quorum = group
	}
	if quorum == nil {
		return ISREmptyRecoveryCommand{}, false, nil
	}
	selected := make([]ISRCatchupProof, 0, minimum)
	for _, brokerID := range metadata.Replicas {
		if proof, ok := quorum[brokerID]; ok {
			selected = append(selected, proof)
			if len(selected) == minimum {
				break
			}
		}
	}
	if len(selected) != minimum {
		return ISREmptyRecoveryCommand{}, false, fmt.Errorf("matching proofs are outside replica assignment for %s", key)
	}
	return ISREmptyRecoveryCommand{
		Topic: first.Topic, Partition: first.Partition, ExpectedLeader: metadata.Leader,
		ExpectedLeaderEpoch: metadata.LeaderEpoch, CommittedHWM: metadata.CommittedHWM,
		LifecycleEpoch: metadata.LifecycleEpoch, RequiredMinISR: minimum, Proofs: selected,
	}, true, nil
}

func (f *BrokerFSM) applyISREmptyRecoveryCommand(jsonData string) interface{} {
	var command ISREmptyRecoveryCommand
	if err := decodeStrictJSON([]byte(jsonData), &command); err != nil {
		return fmt.Errorf("decode empty ISR recovery: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applyISREmptyRecoveryLocked(command)
}

func (f *BrokerFSM) applyISREmptyRecoveryLocked(command ISREmptyRecoveryCommand) error {
	if command.Topic == "" || command.Partition < 0 || command.ExpectedLeader == "" || command.RequiredMinISR < 1 {
		return fmt.Errorf("invalid empty ISR recovery command")
	}
	key := command.Topic + "-" + strconv.Itoa(command.Partition)
	metadata := f.partitionMetadata[key]
	if metadata == nil {
		return fmt.Errorf("partition metadata %s not found", key)
	}
	minimum, err := f.effectiveMinISRLocked(command.Topic)
	if err != nil {
		return err
	}
	if command.RequiredMinISR != minimum || len(command.Proofs) != minimum {
		return fmt.Errorf("effective minimum ISR mismatch for %s", key)
	}
	seen := make(map[string]struct{}, len(command.Proofs))
	identity := ""
	selected := make([]string, 0, len(command.Proofs))
	proofByBroker := make(map[string]ISRCatchupProof, len(command.Proofs))
	for _, proof := range command.Proofs {
		if proof.Topic != command.Topic || proof.Partition != command.Partition || proof.CommittedHWM != command.CommittedHWM ||
			proof.LeaderEpoch != command.ExpectedLeaderEpoch || proof.LifecycleEpoch != command.LifecycleEpoch {
			return fmt.Errorf("empty ISR proof fence mismatch for %s", key)
		}
		if err := validateEmptyISRPrefix(proof, command.CommittedHWM); err != nil {
			return err
		}
		if _, duplicate := seen[proof.BrokerID]; duplicate {
			return fmt.Errorf("duplicate empty ISR proof broker %s", proof.BrokerID)
		}
		seen[proof.BrokerID] = struct{}{}
		proofIdentity := emptyISRProofIdentity(proof)
		if identity == "" {
			identity = proofIdentity
		} else if identity != proofIdentity {
			return fmt.Errorf("empty ISR lineage digest mismatch for %s", key)
		}
		proofByBroker[proof.BrokerID] = proof
	}
	for _, brokerID := range metadata.Replicas {
		if _, ok := proofByBroker[brokerID]; ok {
			selected = append(selected, brokerID)
		}
	}
	if len(selected) != minimum {
		return fmt.Errorf("empty ISR proofs do not match replica assignment for %s", key)
	}
	for index, proof := range command.Proofs {
		if proof.BrokerID != selected[index] {
			return fmt.Errorf("empty ISR proofs are not in replica assignment order for %s", key)
		}
	}
	targetLeader := selected[0]
	if command.ExpectedLeaderEpoch == math.MaxInt {
		return fmt.Errorf("leader epoch overflow for %s", key)
	}
	// An apply retry after an unknown outcome is an exact no-op.
	if metadata.Leader == targetLeader && metadata.LeaderEpoch == command.ExpectedLeaderEpoch+1 &&
		metadata.CommittedHWMKnown && metadata.CommittedHWM == command.CommittedHWM && metadata.LifecycleEpoch == command.LifecycleEpoch &&
		containsAllStrings(metadata.ISR, selected) {
		return nil
	}
	for brokerID, broker := range f.brokers {
		if broker != nil && broker.Status == "active" && broker.RecoveryProtocol < EmptyISRRecoveryProtocolVersion {
			return fmt.Errorf("broker %s does not support empty ISR recovery", brokerID)
		}
	}
	if len(metadata.ISR) != 0 || metadata.Leader != command.ExpectedLeader || metadata.LeaderEpoch != command.ExpectedLeaderEpoch ||
		!metadata.CommittedHWMKnown || metadata.CommittedHWM != command.CommittedHWM || metadata.LifecycleEpoch != command.LifecycleEpoch {
		return fmt.Errorf("stale empty ISR recovery command for %s", key)
	}
	for _, proof := range command.Proofs {
		if err := f.validateEmptyISRRecoveryProofLocked(proof); err != nil {
			return err
		}
	}
	metadata.ISR = append([]string(nil), selected...)
	metadata.Leader = targetLeader
	metadata.LeaderEpoch++
	for _, brokerID := range selected {
		metadata.RecoveryReplicas = removeString(metadata.RecoveryReplicas, brokerID)
	}
	return nil
}

func containsAllStrings(values, required []string) bool {
	for _, value := range required {
		if !containsString(values, value) {
			return false
		}
	}
	return true
}
